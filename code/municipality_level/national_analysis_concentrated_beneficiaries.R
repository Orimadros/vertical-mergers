# -------------------------------
# 0.  LIBRARIES
# -------------------------------
library(tidyverse)
library(arrow)
library(here)
library(janitor)
library(haven)
library(geobr)
library(sf)

# -------------------------------
# 1.  LOAD PROCESSED READJUSTMENTS
# -------------------------------

message("Step 5: Load processed readjustments & compute averages…")
# your 11M‐row reajustes panel
reaj <- open_dataset(
  here("data","processed_data","national","reajustes_panel_final.parquet")
)

# Relevant plans are large employer-sponsored plans with hospital coverage. The
# readjustment dataset is already filtered for these plans.
relevant_plans <- reaj %>% distinct(id_plano) %>% pull()

# -------------------------------
# 2.  LOAD PROCESSED BENEFICIARIES
# -------------------------------
message("Step 2: Load filtered beneficiaries…")

beneficiarios <- open_dataset(
  here("data","processed_data","national","beneficiarios_filtered_2015_2024.parquet")
) %>%
  filter(id_plano %in% relevant_plans)

# -------------------------------
# 3. IDENTIFY TREATMENT GROUPS
# -------------------------------
message("Step 3: Identify treatment groups…")
baseline_total_benefs <- beneficiarios %>%
  filter(year == 2020) %>%
  group_by(cd_municipio) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm=TRUE), .groups="drop") %>%
  collect()

baseline_insurer_presence <- beneficiarios %>%
  filter(year == 2020) %>%
  group_by(cd_municipio, cd_operadora) %>%
  summarise(insurer_benef = sum(qt_beneficiario_ativo, na.rm=TRUE), .groups="drop") %>%
  collect() %>%
  left_join(baseline_total_benefs, by="cd_municipio") %>%
  mutate(
    market_share  = ifelse(total_benef>0, insurer_benef/total_benef, 0),
    insurer_type  = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ "Other"
    ),
    has_presence  = insurer_benef >= 50
  )

municipality_treatment <- baseline_insurer_presence %>%
  filter(insurer_type %in% c("Hapvida","GNDI")) %>%
  select(cd_municipio, insurer_type, has_presence) %>%
  pivot_wider(
    id_cols     = cd_municipio,
    names_from  = insurer_type,
    values_from = has_presence,
    values_fill = FALSE
  ) %>%
  mutate(
    Hapvida        = coalesce(Hapvida, FALSE),
    GNDI           = coalesce(GNDI,    FALSE),
    treatment_status = case_when(
      Hapvida & GNDI ~ "Treated",
      Hapvida | GNDI ~ "Control",
      TRUE           ~ "Out of Sample"
    ),
    treated = as.numeric(treatment_status == "Treated")
  )

# -------------------------------
# 4. PLAN MARKET SHARES
# -------------------------------
message("Step 4: Compute plan market shares…")
total_benef_by_mun_year <- beneficiarios %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm=TRUE), .groups="drop") %>%
  collect()

plan_market_shares <- beneficiarios %>%
  filter(!is.na(id_plano)) %>%
  group_by(cd_municipio, year, id_plano, cd_operadora) %>%
  summarise(plan_benef = sum(qt_beneficiario_ativo, na.rm=TRUE), .groups="drop") %>%
  collect() %>%
  left_join(total_benef_by_mun_year, by=c("cd_municipio","year")) %>%
  mutate(
    market_share = ifelse(total_benef>0, plan_benef/total_benef, 0),
    insurer      = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ "Other"
    ),
    merged_insurer = ifelse(
      year >= 2021 & insurer %in% c("Hapvida","GNDI"),
      "HAP-GNDI-merged",
      insurer
    )
  ) %>%
  left_join(
    municipality_treatment %>% select(cd_municipio, treatment_status, treated),
    by = "cd_municipio"
  )

# -------------------------------
# 5. MUNICIPALITY PANEL
# -------------------------------
message("Step 5: Build municipality panel…")
plan_reajustes_avg <- reaj %>%
  group_by(year, id_plano) %>%
  summarise(
    total_benef  = sum(benef_comunicado, na.rm = TRUE),
    weighted_sum = sum(percentual * benef_comunicado, na.rm = TRUE),
    percentual_avg = weighted_sum / total_benef,
    .groups="drop"
  ) %>%
  select(year, id_plano, percentual_avg) %>%
  collect()

mp <- plan_market_shares %>%
  left_join(plan_reajustes_avg, by = c("id_plano","year")) %>%
  group_by(cd_municipio, year) %>%
  summarise(
    readjustment       = weighted.mean(percentual_avg, w = market_share, na.rm = TRUE),
    n_plans            = n(),
    n_plans_with_readj = sum(!is.na(percentual_avg)),
    coverage           = sum(market_share[!is.na(percentual_avg)], na.rm = TRUE),
    treatment_status   = first(treatment_status),
    treated            = first(treated),
    .groups            = "drop"
  ) %>%
  mutate(
    merger_year = 2022,
    timeToTreat = ifelse(treated==1, year - merger_year, NA_real_)
  )

# Attach 2020 GDP and state
pib2020 <- read_csv(here("data","raw_data","municipios","br_ibge_pib_municipio.csv")) %>%
  clean_names() %>%
  filter(ano == 2020) %>%
  mutate(cd_municipio = as.numeric(substr(id_municipio,1,6))) %>%
  select(cd_municipio, gdp_2020 = pib)

mp <- mp %>%
  left_join(pib2020, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup()

muni_states <- beneficiarios %>%
  select(cd_municipio, sg_uf) %>%
  distinct() %>%
  collect()

mp <- mp %>%
  left_join(muni_states, by = "cd_municipio")

# -------------------------------
# 6. BUILD THE THREE PANELS
# -------------------------------
message("Step 6: Build all three panels…")

# 6a) In‐sample municipalities
mp_all <- mp %>%
  filter(treatment_status %in% c("Treated","Control"))

# 6b) Hap-GNDI share ≥ 20% in 2020
hapgndi2020 <- plan_market_shares %>%
  filter(year == 2020, insurer %in% c("Hapvida","GNDI")) %>%
  group_by(cd_municipio) %>%
  summarise(hg_share2020 = sum(market_share, na.rm=TRUE), .groups="drop")

mp_hapgndi20 <- mp_all %>%
  left_join(hapgndi2020, by="cd_municipio") %>%
  filter(hg_share2020 >= 0.20) %>%
  select(-hg_share2020)

# 6c) Municipalities above the median 2020 HHI
hhi2020 <- plan_market_shares %>%
  filter(year == 2020) %>%
  group_by(cd_municipio) %>%
  summarise(hhi = sum(market_share^2, na.rm=TRUE), .groups="drop")

median_hhi <- median(hhi2020$hhi, na.rm=TRUE)

mp_highhhi <- mp_all %>%
  left_join(hhi2020, by="cd_municipio") %>%
  filter(hhi > median_hhi) %>%
  select(-hhi)

# 6d) Municipalities with Hapvida + GNDI share < 20% in 2020
mp_lowhapgndi20 <- mp_all %>%
  left_join(hapgndi2020, by = "cd_municipio") %>%
  filter(hg_share2020 < 0.20) %>%
  select(-hg_share2020)



# -------------------------------
# 7. EXPORT TO STATA
# -------------------------------
message("Step 7: Export panels to Stata…")
write_dta(mp_all,          here("data","processed_data","national","municipality_panel_event_study_all.dta"))
write_dta(mp_hapgndi20,    here("data","processed_data","national","municipality_panel_event_study_hapgndi20.dta"))
write_dta(mp_highhhi,      here("data","processed_data","national","municipality_panel_event_study_hhi.dta"))
write_dta(mp_lowhapgndi20, here("data","processed_data","national", "municipality_panel_event_study_lowhapgndi20.dta"))

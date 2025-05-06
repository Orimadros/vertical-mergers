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
# 3.  IDENTIFY TREATMENT GROUPS
# -------------------------------
message("Step 3: Identify treatment groups…")
baseline_total_benefs <- beneficiarios %>%
  filter(year == 2020) %>%
  group_by(cd_municipio) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm=TRUE)) %>%
  collect()

baseline_insurer_presence <- beneficiarios %>%
  filter(year == 2020) %>%
  group_by(cd_municipio, cd_operadora) %>%
  summarise(insurer_benef = sum(qt_beneficiario_ativo, na.rm=TRUE)) %>%
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
# 4.  PLAN MARKET SHARES
# -------------------------------

# 4a) Competitors only
message("Step 4a: Compute plan market shares – competitors only…")
total_benef_by_mun_year_comp <- beneficiarios %>%
  filter(!cd_operadora %in% c("368253","359017")) %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect()

plan_market_shares_comp <- beneficiarios %>%
  filter(!is.na(id_plano),
         !cd_operadora %in% c("368253","359017")) %>%
  group_by(cd_municipio, year, id_plano, cd_operadora) %>%
  summarise(plan_benef = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect() %>%
  left_join(total_benef_by_mun_year_comp, by = c("cd_municipio","year")) %>%
  mutate(
    market_share = ifelse(total_benef>0, plan_benef/total_benef, 0),
    insurer      = "Other"
  ) %>%
  left_join(municipality_treatment %>% select(cd_municipio, treatment_status, treated),
            by = "cd_municipio")

# 4b) Hapvida & GNDI only
message("Step 4b: Compute plan market shares – Hapvida & GNDI only…")
total_benef_by_mun_year_hapgndi <- beneficiarios %>%
  filter(cd_operadora %in% c("368253","359017")) %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect()

plan_market_shares_hapgndi <- beneficiarios %>%
  filter(!is.na(id_plano),
         cd_operadora %in% c("368253","359017")) %>%
  group_by(cd_municipio, year, id_plano, cd_operadora) %>%
  summarise(plan_benef = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect() %>%
  left_join(total_benef_by_mun_year_hapgndi, by = c("cd_municipio","year")) %>%
  mutate(
    market_share = ifelse(total_benef>0, plan_benef/total_benef, 0),
    insurer      = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI"
    )
  ) %>%
  left_join(municipality_treatment %>% select(cd_municipio, treatment_status, treated),
            by = "cd_municipio")

# determine how many years the full event‐study spans
years_panel <- plan_reajustes_avg %>% distinct(year) %>% nrow()

# 5a) Municipality panel – competitors (balanced to full panel, drop any muni with missing readjustment)
mp_comp <- plan_market_shares_comp %>%
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
  ) %>%
  left_join(pib, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup() %>%
  left_join(muni_states, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  filter(
    n() == years_panel,                 # balanced full span
    all(!is.na(readjustment))           # no missing readjustment in any year
  ) %>%
  ungroup() %>%
  filter(treatment_status %in% c("Treated","Control"))

# 5b) Municipality panel – Hapvida & GNDI (balanced to full panel, drop any muni with missing readjustment)
mp_hapgndi <- plan_market_shares_hapgndi %>%
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
  ) %>%
  left_join(pib, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup() %>%
  left_join(muni_states, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  filter(
    n() == years_panel,
    all(!is.na(readjustment))
  ) %>%
  ungroup() %>%
  filter(treatment_status %in% c("Treated","Control"))

# -------------------------------
# 6.  EXPORT TO STATA
# -------------------------------
message("Step 6: Export both panels to Stata…")
write_dta(mp_comp,     here("data","processed_data","national","dta","municipality_panel_competitors.dta"))
write_dta(mp_hapgndi,  here("data","processed_data","national","dta","municipality_panel_hapgndi.dta"))

# -------------------------------
# 7.  SANITY CHECKS FOR PANELS
# -------------------------------
message("Step 9: Sanity checks for both panels…")

# 9a) Municipalities per year
message("– Competitors panel: municipalities per year")
mp_comp %>%
  count(year) %>%
  rename(n_municipalities = n) %>%
  print()

message("– Hapvida & GNDI panel: municipalities per year")
mp_hapgndi %>%
  count(year) %>%
  rename(n_municipalities = n) %>%
  print()

# 9b) Distinct municipalities overall
message("– Competitors panel: distinct municipalities")
mp_comp %>%
  summarise(distinct_munis = n_distinct(cd_municipio)) %>%
  print()

message("– Hapvida & GNDI panel: distinct municipalities")
mp_hapgndi %>%
  summarise(distinct_munis = n_distinct(cd_municipio)) %>%
  print()

# 9c) Quintiles of n_plans
probs <- seq(0, 1, 0.2)

message("– Competitors panel: quintiles of n_plans")
quantile(mp_comp$n_plans, probs = probs, na.rm = TRUE) %>%
  enframe(name = "quantile", value = "n_plans") %>%
  print()

message("– Hapvida & GNDI panel: quintiles of n_plans")
quantile(mp_hapgndi$n_plans, probs = probs, na.rm = TRUE) %>%
  enframe(name = "quantile", value = "n_plans") %>%
  print()

# -------------------------------
# 9x. SANITY CHECKS – READJUSTMENT DISTRIBUTIONS
# -------------------------------
message("Readjustment summary – Competitors panel")
mp_comp %>%
  summarise(
    obs               = n(),
    missing           = sum(is.na(readjustment)),
    pct_missing       = 100 * missing / obs,
    mean_readj        = mean(readjustment,   na.rm = TRUE),
    sd_readj          = sd(readjustment,     na.rm = TRUE),
    median_readj      = median(readjustment, na.rm = TRUE),
    min_readj         = min(readjustment,    na.rm = TRUE),
    p20_readj         = quantile(readjustment, 0.20, na.rm = TRUE),
    p40_readj         = quantile(readjustment, 0.40, na.rm = TRUE),
    p60_readj         = quantile(readjustment, 0.60, na.rm = TRUE),
    p80_readj         = quantile(readjustment, 0.80, na.rm = TRUE),
    max_readj         = max(readjustment,    na.rm = TRUE)
  ) %>%
  print()

message("Readjustment by year – Competitors panel")
mp_comp %>%
  group_by(year) %>%
  summarise(
    n                = n(),
    mean_readj       = mean(readjustment, na.rm = TRUE),
    sd_readj         = sd(readjustment,   na.rm = TRUE),
    median_readj     = median(readjustment, na.rm = TRUE),
    pct_missing_readj = 100 * sum(is.na(readjustment)) / n
  ) %>%
  print()

message("Readjustment summary – Hapvida & GNDI panel")
mp_hapgndi %>%
  summarise(
    obs               = n(),
    missing           = sum(is.na(readjustment)),
    pct_missing       = 100 * missing / obs,
    mean_readj        = mean(readjustment,   na.rm = TRUE),
    sd_readj          = sd(readjustment,     na.rm = TRUE),
    median_readj      = median(readjustment, na.rm = TRUE),
    min_readj         = min(readjustment,    na.rm = TRUE),
    p20_readj         = quantile(readjustment, 0.20, na.rm = TRUE),
    p40_readj         = quantile(readjustment, 0.40, na.rm = TRUE),
    p60_readj         = quantile(readjustment, 0.60, na.rm = TRUE),
    p80_readj         = quantile(readjustment, 0.80, na.rm = TRUE),
    max_readj         = max(readjustment,    na.rm = TRUE)
  ) %>%
  print()

message("Readjustment by year – Hapvida & GNDI panel")
mp_hapgndi %>%
  group_by(year) %>%
  summarise(
    n                = n(),
    mean_readj       = mean(readjustment, na.rm = TRUE),
    sd_readj         = sd(readjustment,   na.rm = TRUE),
    median_readj     = median(readjustment, na.rm = TRUE),
    pct_missing_readj = 100 * sum(is.na(readjustment)) / n
  ) %>%
  print()
mp_hapgndi %>% 
  filter(is.na(readjustment))


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
message("Step 1: Load processed readjustments…")
reaj <- open_dataset(
  here("data","processed_data","national","reajustes_panel_final.parquet")
)

relevant_plans <- reaj %>% 
  distinct(id_plano) %>% 
  pull()

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
    market_share = ifelse(total_benef > 0,
                          insurer_benef / total_benef,
                          0),
    insurer_type = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ "Other"
    ),
    has_presence = insurer_benef >= 50
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
message("Step 4: Compute plan market shares…")
total_benef_by_mun_year <- beneficiarios %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo, na.rm=TRUE)) %>%
  collect()

plan_market_shares <- beneficiarios %>%
  filter(!is.na(id_plano)) %>%
  group_by(cd_municipio, year, id_plano, cd_operadora) %>%
  summarise(plan_benef = sum(qt_beneficiario_ativo, na.rm=TRUE)) %>%
  collect() %>%
  left_join(total_benef_by_mun_year, by=c("cd_municipio","year")) %>%
  mutate(
    market_share = ifelse(total_benef>0,
                          plan_benef/total_benef,
                          0),
    insurer = case_when(
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
# 5.  MUNICIPALITY PANEL
# -------------------------------
message("Step 5: Build municipality panels…")
plan_reajustes_avg <- reaj %>%
  group_by(year, id_plano) %>%
  summarise(
    total_benef  = sum(benef_comunicado, na.rm = TRUE),
    weighted_sum = sum(percentual * benef_comunicado, na.rm = TRUE),
    percentual_avg = weighted_sum / total_benef
  ) %>%
  select(year, id_plano, percentual_avg) %>%
  ungroup() %>%
  collect()

ps <- plan_market_shares %>%
  left_join(plan_reajustes_avg, by = c("id_plano","year"))

# 5a-i) Panel 1: only Hapvida & GNDI
mp_hapgndi <- ps %>%
  filter(insurer %in% c("Hapvida","GNDI")) %>%
  group_by(cd_municipio, year) %>%
  summarise(
    readjustment     = weighted.mean(percentual_avg, w = market_share, na.rm = TRUE),
    n_plans          = n(),
    coverage         = sum(market_share, na.rm = TRUE),
    treatment_status = first(treatment_status),
    treated          = first(treated),
    .groups          = "drop"
  ) %>%
  mutate(
    merger_year = 2022,
    timeToTreat = ifelse(treated == 1, year - merger_year, NA_real_)
  ) %>%
  left_join(pib, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup() %>%
  left_join(
    beneficiarios %>% 
      select(cd_municipio, sg_uf) %>% 
      distinct() %>% 
      collect(),
    by = "cd_municipio"
  )

# 5a-ii) Panel 2: all other plans
mp_other <- ps %>%
  filter(insurer == "Other") %>%
  group_by(cd_municipio, year) %>%
  summarise(
    readjustment     = weighted.mean(percentual_avg, w = market_share, na.rm = TRUE),
    n_plans          = n(),
    coverage         = sum(market_share, na.rm = TRUE),
    treatment_status = first(treatment_status),
    treated          = first(treated),
    .groups          = "drop"
  ) %>%
  mutate(
    merger_year = 2022,
    timeToTreat = ifelse(treated == 1, year - merger_year, NA_real_)
  ) %>%
  left_join(pib, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup() %>%
  left_join(
    beneficiarios %>% 
      select(cd_municipio, sg_uf) %>% 
      distinct() %>% 
      collect(),
    by = "cd_municipio"
  )

# -------------------------------
# 6.  EXPORT TO STATA
# -------------------------------
message("Step 6: Export to Stata…")
write_dta(mp_hapgndi, here("data","processed_data","national","mp_hapgndi.dta"))
write_dta(mp_other,  here("data","processed_data","national","mp_other.dta"))

# -------------------------------
# 7.  NATIONAL TREATMENT MAP
# -------------------------------
message("Step 7: Plot national treatment map…")
br_munis <- read_municipality(code_muni = "all", year = 2020) %>%
  mutate(cd_municipio = as.numeric(substr(code_muni,1,nchar(code_muni)-1)))
map_data <- municipality_treatment %>%
  mutate(cd_municipio = as.numeric(cd_municipio))
br_map <- br_munis %>%
  left_join(map_data, by = "cd_municipio") %>%
  mutate(
    treatment_status = replace_na(treatment_status, "Out of Sample"),
    treatment_status = factor(treatment_status,
                              levels = c("Treated","Control","Out of Sample"))
  )
treatment_map_br <- ggplot(br_map) +
  geom_sf(aes(fill = treatment_status), color = NA, size = 0.01) +
  geom_sf(data = read_state(year = 2020),
          fill = NA, color = "white", size = 0.3) +
  scale_fill_manual(values = c(
    Treated         = "orange",
    Control         = "darkcyan",
    `Out of Sample` = "lightgrey"
  ), name = "Treatment") +
  labs(
    title    = "Treatment Status – Brazil",
    subtitle = "(Hapvida & GNDI ≥50 beneficiaries in 2020)"
  ) +
  theme_classic() +
  theme(
    legend.position = "bottom",
    axis.line        = element_blank(),
    axis.text        = element_blank(),
    axis.ticks       = element_blank(),
    axis.title       = element_blank()
  )
ggsave(
  here("data","images","national","treatment_status_map_BR.png"),
  treatment_map_br,
  width = 8, height = 8, dpi = 1000
)

# -------------------------------
# 8.  SANITY CHECKS
# -------------------------------
message("Step 8: Sanity checks…")

# 8a) counts by year & treatment_status
mp_hapgndi %>% count(year, treatment_status) %>% arrange(year, treatment_status) %>% print()
mp_other    %>% count(year, treatment_status) %>% arrange(year, treatment_status) %>% print()

# 8b) missing‐readjustment diagnostics
mp_hapgndi %>%
  summarise(
    total_obs     = n(),
    missing_readj = sum(is.na(readjustment)),
    pct_missing   = 100 * missing_readj/total_obs
  ) %>% print()
mp_other %>%
  summarise(
    total_obs     = n(),
    missing_readj = sum(is.na(readjustment)),
    pct_missing   = 100 * missing_readj/total_obs
  ) %>% print()

# 8c) distribution of readjustments
mp_hapgndi %>%
  summarise(min = min(readjustment, na.rm=TRUE),
            q1  = quantile(readjustment, .25, na.rm=TRUE),
            med = median(readjustment, na.rm=TRUE),
            q3  = quantile(readjustment, .75, na.rm=TRUE),
            max = max(readjustment, na.rm=TRUE)) %>% print()
mp_other %>%
  summarise(min = min(readjustment, na.rm=TRUE),
            q1  = quantile(readjustment, .25, na.rm=TRUE),
            med = median(readjustment, na.rm=TRUE),
            q3  = quantile(readjustment, .75, na.rm=TRUE),
            max = max(readjustment, na.rm=TRUE)) %>% print()

message("Pipeline complete.")

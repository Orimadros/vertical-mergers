# -------------------------------
# 0.  LIBRARIES
# -------------------------------
library(tidyverse)
library(arrow)
library(here)
library(janitor)
library(haven)

# -------------------------------
# 1.  LOAD PROCESSED READJUSTMENTS
# -------------------------------

message("Step 5: Load processed readjustments & compute averages…")
# your 11M‐row reajustes panel
reaj <- open_dataset(
  here("data","processed_data","national","reajustes_panel_final.parquet")
)
reaj %>% glimpse()
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
# 5.  MUNICIPALITY PANEL
# -------------------------------

# beneficiarios was filtered at the beginning of the script to the relevant
# plans

beneficiarios %>%
  group_by(cd_municipio, year) %>%
  

# -------------------------------
# 6.  EXPORT TO STATA (ONLY IN‐SAMPLE MUNICIPALITIES)
# -------------------------------
message("Step 7: Export panels to Stata…")

# keep only Treated & Control (drop "Out of Sample")
mp_sample <- mp %>%
  filter(treatment_status %in% c("Treated", "Control"))

write_dta(
  mp_sample,
  here("data","processed_data","national","municipality_panel_event_study.dta")
)

# -------------------------------
# 8.  SANITY CHECKS
# -------------------------------
message("Step 9: Sanity checks…")

# 8a) Count of municipalities by treatment_status and year (including Out of Sample)
mp %>%
  group_by(year, treatment_status) %>%
  summarise(
    n_municipalities = n(),
    .groups = "drop"
  ) %>%
  arrange(year, treatment_status) %>%
  print()

# 8b) Missing‐readjustment diagnostics by treatment_status
mp %>%
  group_by(treatment_status) %>%
  summarise(
    total_obs           = n(),
    missing_readjustment= sum(is.na(readjustment)),
    pct_missing_readj   = 100 * missing_readjustment / total_obs,
    .groups = "drop"
  ) %>%
  print()

# 8c) Overall readjustment distribution
mp %>%
  summarise(
    obs_total           = n(),
    missing_readj       = sum(is.na(readjustment)),
    pct_missing_readj   = 100 * missing_readj / obs_total,
    mean_readj          = mean(readjustment,   na.rm=TRUE),
    sd_readj            = sd(readjustment,     na.rm=TRUE),
    median_readj        = median(readjustment, na.rm=TRUE),
    min_readj           = min(readjustment,    na.rm=TRUE),
    max_readj           = max(readjustment,    na.rm=TRUE)
  ) %>%
  print()

# 8d) Coverage = 0 cases by year
mp %>%
  filter(coverage == 0) %>%
  count(year, name = "n_no_coverage") %>%
  arrange(year) %>%
  print()

# 8e) Distribution of number of plans per municipality
mp %>%
  summarise(
    mean_plans         = mean(n_plans, na.rm=TRUE),
    median_plans       = median(n_plans, na.rm=TRUE),
    min_plans          = min(n_plans, na.rm=TRUE),
    max_plans          = max(n_plans, na.rm=TRUE),
    sd_plans           = sd(n_plans, na.rm=TRUE)
  ) %>%
  print()

message("Sanity checks complete — national pipeline still running!")



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
# 1.1  LOAD PLAN CHARACTERISTICS
# -------------------------------
message("Step 2.1: Load plan characteristics...")

caracteristicas_planos <- read_csv2(
  here("data", "raw_data", "ANS", "operadoras", "planos", 
       "caracteristicas_produtos_saude_suplementar.csv")
) %>%
  clean_names() %>%
  filter(year(dt_registro_plano) <= 2020,
         situacao_plano == 'Ativo') %>%
  mutate(coparticipacao = as.numeric(str_detect(fator_moderador, "Coparticipação")),
         franquia = as.numeric(str_detect(fator_moderador, "Franquia")),
         in_network_only = as.numeric(livre_escolha == "Ausente"))

perennial_plans <- caracteristicas_planos %>% distinct(id_plano) %>% pull()

# -------------------------------
# 1.2  LOAD PROCESSED READJUSTMENTS
# -------------------------------

message("Step 5: Load processed readjustments & compute averages…")
# your 11M‐row reajustes panel
reaj <- open_dataset(
  here("data","processed_data","national","reajustes_panel_final.parquet")
)

# Relevant plans are large employer-sponsored plans with hospital coverage. The
# readjustment dataset is already filtered for these plans.
large_employer_hospital_plans <- reaj %>% distinct(id_plano) %>% pull()

relevant_plans <- intersect(large_employer_hospital_plans, perennial_plans)
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
    market_share = ifelse(total_benef>0, plan_benef/total_benef, 0),
    insurer      = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ "Other"
    ),
    merged_insurer = ifelse(
      year >= 2021 & insurer %in% c("Hapvida","GNDI"),
      "HAP‑GNDI‑merged",
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
message("Step 6: Build municipality panel…")

plan_reajustes_avg <- reaj %>%
  group_by(year, id_plano) %>%
  summarise(
    total_benef = sum(benef_comunicado, na.rm = TRUE),
    weighted_sum = sum(percentual * benef_comunicado, na.rm = TRUE),
    percentual_avg = weighted_sum / total_benef
  ) %>%
  select(year, id_plano, percentual_avg) %>%
  ungroup() %>%
  collect()

# 5a) build the basic panel
mp <- plan_market_shares %>%
  left_join(
    plan_reajustes_avg,
    by = c("id_plano","year")
  ) %>%
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
    timeToTreat = case_when(
      treated == 1 ~ year - merger_year,
      treated == 0 ~ NA
    )
  )

# 5b) attach 2020 GDP and propagate to all years
pib <- read_csv(here("data","raw_data","municipios","br_ibge_pib_municipio.csv")) %>%
  clean_names() %>%
  filter(ano == 2020) %>%
  mutate(cd_municipio = as.numeric(substr(id_municipio,1,6))) %>%
  select(cd_municipio, gdp_2020 = pib)

mp <- mp %>%
  left_join(pib, by = "cd_municipio") %>%
  group_by(cd_municipio) %>%
  fill(gdp_2020, .direction = "downup") %>%
  ungroup()

# 5c) pull in each muni’s state code (sg_uf) from the Arrow dataset
muni_states <- beneficiarios %>%
  select(cd_municipio, sg_uf) %>%
  distinct() %>%
  collect()

mp <- mp %>%
  left_join(muni_states, by = "cd_municipio")

benef_per_munic_year <- beneficiarios %>%
  group_by(cd_municipio, year) %>%
  summarize(total_benef = sum(qt_beneficiario_ativo)) %>%
  collect()

mp <- mp %>% left_join(benef_per_munic_year, by = c("cd_municipio", "year"))


# -------------------------------
# 6.  EXPORT TO STATA (ONLY IN‐SAMPLE MUNICIPALITIES)
# -------------------------------
message("Step 7: Export panels to Stata…")

# keep only Treated & Control (drop "Out of Sample")
mp_sample <- mp %>%
  filter(treatment_status %in% c("Treated", "Control"))

write_dta(
  mp_sample,
  here("data","processed_data","national","dta","municipality_panel_event_study_perennial_plans.dta")
)

# -------------------------------
# 7.  NATIONAL TREATMENT MAP
# -------------------------------
message("Step 8: Plot national treatment map…")

# load municipality geometries
br_munis <- read_municipality(code_muni = "all", year = 2020) %>%
  mutate(cd_municipio = as.numeric(substr(code_muni, 1, nchar(code_muni) - 1)))

# bring in your treatment data
map_data <- municipality_treatment %>%
  mutate(cd_municipio = as.numeric(cd_municipio))

# join & recode NAs to "Out of Sample"
br_map <- br_munis %>%
  left_join(map_data, by = "cd_municipio") %>%
  mutate(
    treatment_status = replace_na(treatment_status, "Out of Sample"),
    treatment_status = factor(
      treatment_status,
      levels = c("Treated", "Control", "Out of Sample")
    )
  )

# plot with white background
treatment_map_br <- ggplot(br_map) +
  geom_sf(aes(fill = treatment_status), color = NA, size = 0.01) +
  geom_sf(
    data = read_state(year = 2020),
    fill = NA, color = "white", size = 0.3
  ) +
  scale_fill_manual(
    values = c(
      Treated         = "orange",
      Control         = "darkcyan",
      `Out of Sample` = "lightgrey"
    ),
    name = "Treatment"
  ) +
  labs(
    title    = "Treatment Status – Brazil",
    subtitle = "(Hapvida & GNDI ≥50 beneficiaries in 2020)"
  ) +
  theme_classic() +
  theme(
    legend.position = "bottom",
    # ensure axes and grid are clean for a map
    axis.line        = element_blank(),
    axis.text        = element_blank(),
    axis.ticks       = element_blank(),
    axis.title       = element_blank()
  )

# save
ggsave(
  here("data","images","national","treatment_status_map_BR.png"),
  treatment_map_br,
  width = 8, height = 8, dpi = 1000
)

treatment_map_br %>% glimpse()

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

mp_sample %>% glimpse()

reaj %>% 
  group_by(year) %>%
  summarise(na_count = sum(is.na(lg_fator_moderador))) %>%
  collect()

beneficiarios %>% names()

caracteristicas_planos <- read_csv2('/Users/leonardogomes/Library/CloudStorage/GoogleDrive-leodavinci550@gmail.com/My Drive/Insper/TCC/TCC_project/data/raw_data/ANS/operadoras/planos/caracteristicas_produtos_saude_suplementar.csv')

beneficiarios <- open_dataset('/Users/leonardogomes/Library/CloudStorage/GoogleDrive-leodavinci550@gmail.com/My Drive/Insper/TCC/TCC_project/data/raw_data/ANS/beneficiarios/2023/parquet/pda-024-icb-SP-2023_12.parquet')
beneficiarios %>% colnames()

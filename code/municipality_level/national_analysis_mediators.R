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
  filter(id_plano %in% relevant_plans,
         qt_beneficiario_ativo > 0)

# -------------------------------
# 2.1  LOAD PLAN CHARACTERISTICS
# -------------------------------
message("Step 2.1: Load plan characteristics...")

caracteristicas_planos <- read_csv2(
  here("data", "raw_data", "ANS", "operadoras", "planos", 
       "caracteristicas_produtos_saude_suplementar.csv")
) %>%
  clean_names() %>%
  # Keep only relevant plans
  filter(id_plano %in% relevant_plans) %>%
  mutate(coparticipacao = as.numeric(str_detect(fator_moderador, "Coparticipação")),
         franquia = as.numeric(str_detect(fator_moderador, "Franquia")),
         in_network_only = as.numeric(livre_escolha == "Ausente"))

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
      cd_operadora %in% c("359017", "348520") ~ "GNDI",
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

beneficiarios <- beneficiarios %>% left_join(caracteristicas_planos %>% select(id_plano, coparticipacao, franquia, in_network_only), by = "id_plano")

mp <- beneficiarios %>%
  group_by(cd_municipio, year) %>%
  summarize(total_benef = sum(qt_beneficiario_ativo),
            n_idosos = sum(
              if_else(de_faixa_etaria_reaj == '59 ou mais',
                      qt_beneficiario_ativo,
                      0)),
            pct_idosos = n_idosos / total_benef,
            n_coparticipacao = sum(
              if_else(coparticipacao == 1,
                      qt_beneficiario_ativo,
                      0)),
            pct_coparticipacao = n_coparticipacao / total_benef,
            n_franquia = sum(
              if_else(franquia == 1,
                      qt_beneficiario_ativo,
                      0)),
            pct_franquia = n_franquia / total_benef,
            n_in_network = sum(
              if_else(in_network_only == 1,
                      qt_beneficiario_ativo,
                      0)),
            pct_in_network = n_in_network / total_benef) %>%
  left_join(municipality_treatment %>% select(cd_municipio, treatment_status)) %>%
  mutate(timeToTreat = if_else(
    treatment_status == 'Treated',
    year - 2022,
    NA
  )) %>%
  select(-n_idosos, -n_coparticipacao, -n_franquia, -n_in_network) %>%
  collect()

muni_states <- beneficiarios %>%
  select(cd_municipio, sg_uf) %>%
  distinct() %>%
  collect()

mp <- mp %>%
  left_join(muni_states, by = "cd_municipio")


# -------------------------------
# 6.  EXPORT TO STATA (ONLY IN‐SAMPLE MUNICIPALITIES)
# -------------------------------
message("Step 7: Export panels to Stata…")

# keep only Treated & Control (drop "Out of Sample")
mp_sample <- mp %>%
  filter(treatment_status %in% c("Treated", "Control"))

write_dta(
  mp_sample,
  here("data","processed_data","national","dta","municipality_panel_event_study_mediators.dta")
)

# -------------------------------
# 8.  SANITY CHECKS
# -------------------------------
message("Step 9: Sanity checks…")

# 8a) Count of municipalities by treatment_status and year (including Out of Sample)
mp_sample %>%
  group_by(year, treatment_status) %>%
  summarise(
    n_municipalities = n(),
    .groups = "drop"
  ) %>%
  arrange(year, treatment_status) %>%
  print()

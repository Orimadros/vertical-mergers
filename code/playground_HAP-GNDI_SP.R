library(tidyverse)
library(here)
library(readxl)
library(janitor)
library(VennDiagram)
library(did)
library(geobr)
library(sf)


# -------------------------------
# LEITURA DAS BASES
# -------------------------------

# Usamos a base de operadoras ativas para conseguir o registro ANS a partir do
# CNPJ para cada operadora

operadoras <- read_csv2(here('data', 
                             'raw_data',
                             'ANS',
                             'operadoras',
                             'operadoras ativas',
                             'operadoras_ativas.csv')) %>%
  clean_names() %>%
  select(registro_ans,
         cnpj,
         razao_social,
         modalidade,
         uf)



# Usamos a base de informações consolidadas dos beneficiários para identificar
# o grupo de cidades com pelo menos um beneficiário para cada operadora

beneficiarios_cons_202305 <- read_csv2(here('data', 
                                            'raw_data',
                                            'ANS',
                                            'beneficiarios',
                                            'ben202305_SP.csv')) %>%
  clean_names()


beneficiarios_cons_202305 <- beneficiarios_cons_202305 %>%
  filter(de_contratacao_plano == 'COLETIVO EMPRESARIAL',
         de_segmentacao_plano != 'ODONTOL\xd3GICO')

# Usamos a base de relações de credenciamento para identificar as redes

hospitais_planos <- read_csv2(here('data', 
                                   'raw_data',
                                   'ANS',
                                   'prestadores',
                                   'produtos e prestadores hospitalares',
                                   'produtos_prestadores_hospitalares_SP.csv')) %>%
  clean_names()

hospitais_planos <- hospitais_planos %>%
  filter(
    de_tipo_contratacao == 'COLETIVO EMPRESARIAL',
    de_clas_estb_saude == 'Assistencia Hospitalar'
  )

# Usamos a base de reajustes_202305 de planos coletivos para obter nossa
# variável dependente

# Define the range of months
months <- seq(as.Date("2019-06-01"), as.Date("2023-05-01"), by = "month")
file_names <- paste0("PDA_RPC_", format(months, "%Y%m"), ".csv")
file_paths <- file_names %>%
  map(~ here("data", "raw_data", "ANS", "operadoras", "reajustes", .x))

# Explicit column types
col_types <- cols(
  CD_PLANO = col_character(),  # Ensure CD_PLANO is always character
  .default = col_guess()       # Infer other column types
)

# Import and stack the CSV files
reajustes <- file_paths %>%
  map_dfr(~ read_csv2(.x, col_types = col_types))
reajustes <- reajustes %>% clean_names()



# O cd_agrupamento == 0 tira planos empresariais com menos de 30 vidas, cujos 
# reajustes são negociados de forma agrupada por operadora.

hospitais_planos <- hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar')

# -------------------------------
# DATABASE COMPARISON
# -------------------------------

# Extract unique values from the 'cd_plano' column in each data frame
set_reajustes <- unique(reajustes$cd_plano)
set_hospitais <- unique(hospitais_planos$cd_plano)
set_beneficiarios <- unique(beneficiarios_cons_202305$cd_plano)

# Calculate areas and intersections
area1 <- length(set_reajustes)
area2 <- length(set_hospitais)
area3 <- length(set_beneficiarios)
n12 <- length(intersect(set_reajustes, set_hospitais))
n13 <- length(intersect(set_reajustes, set_beneficiarios))
n23 <- length(intersect(set_hospitais, set_beneficiarios))
n123 <- length(Reduce(intersect, list(set_reajustes, set_hospitais, set_beneficiarios)))

# Create the Venn diagram
venn_diagram <- draw.triple.venn(
  area1 = area1,
  area2 = area2,
  area3 = area3,
  n12 = n12,
  n13 = n13,
  n23 = n23,
  n123 = n123,
  category = c("Reajustes", "Hospitais Planos", "Beneficiarios"),
  fill = c("#FF9999", "#99CCFF", "#99FF99"),
  alpha = 0.5,
  lty = "dashed",
  cex = 1.5,
  cat.cex = 1.5,
)

# Plot the diagram
grid.draw(venn_diagram)

# -------------------------------
# EXPLORAÇÃO
# -------------------------------

# Dúvida: planos HapVida dão acesso a hospitais não-hapvida?

single_insurer_hospitals <- hospitais_planos %>%
  group_by(cd_cnes) %>%
  filter(n_distinct(cd_operadora) == 1)

GNDI_hospitals <- hospitais_planos %>%
  filter(str_detect(cd_cnpj_estb_saude, '44649812')) %>%
  distinct(cd_cnes, .keep_all = TRUE) %>%
  drop_na(cd_cnes)

hospitais_planos %>%
  filter(cd_cnes == '2372207')

# Descoberta: todos os hospitais da Hapvida em SP vieram de aquisições, como da
# GNDI, São Francisco, e Ultra Som.

# CNPJ de hospitais Hapvida (e.g. CNES 2081601) estão desatualizados, e não
# podem ser utilizados para identificar Hap_hospitals.

# Próximo passo: identificar planos Hapvida atuando em cidades paulistas com
# hospitais GNDI.

hap_plans_w_GNDI <- hospitais_planos %>%
  filter(cd_operadora == 368253,
         str_detect(cd_cnpj_estb_saude, '44649812')) %>%
  distinct(cd_plano) %>%
  pull()

# IMPORTANTE: pra identificar mesmo MB Plans, tem que pegar as bases de
# beneficiarios de todos os estados. Tem planos que sobram com o filtro
# abaixo mas que são predominantemente de tocantins, por exemplo.

municipally_bound_plans <- beneficiarios_cons_202305 %>%
  group_by(cd_plano) %>%
  summarize(cidades = n_distinct(cd_municipio)) %>%
  arrange(desc(cidades)) %>%
  filter(cidades == 1) %>%
  pull(cd_plano)


# Defining treated plans as MB plans operating in municipalities where 1) there
# is at least one Hapvida plan and 2) there is at least one GNDI hospital

# Defining control plans as MB plans operating in municipalities where there
# is at least one Hapvida plan but no GNDI hospital

# To identify these plans we first flag municipalities that satisfy the
# conditions above, and then find plans bound in these municipalities.

GNDI_hospital_munics <- GNDI_hospitals %>% distinct(cd_municipio) %>% pull()

hap_plan_munics <- hospitais_planos %>%
  filter(cd_operadora == 368253) %>%
  distinct(cd_municipio) %>%
  pull()
  
treated_munics <- intersect(hap_plan_munics, GNDI_hospital_munics)

control_munics <- setdiff(hap_plan_munics, GNDI_hospital_munics)

treated_plans <- hospitais_planos %>% 
  filter(cd_municipio %in% treated_munics) %>%
  distinct(cd_plano) %>%
  pull()

control_plans <- hospitais_planos %>% 
  filter(cd_municipio %in% control_munics) %>%
  distinct(cd_plano) %>%
  pull()

reajustes_sample <- reajustes %>%
  filter(cd_plano %in% treated_plans |
           cd_plano %in% control_plans,
         sg_uf_contrato_reaj == 'SP',
         cd_agrupamento == 0,
         benef_comunicado > 1) %>%
  distinct()

reajustes_sample_MB <- reajustes %>%
  filter(cd_plano %in% treated_plans |
           cd_plano %in% control_plans,
         cd_plano %in% municipally_bound_plans,
         sg_uf_contrato_reaj == 'SP',
         cd_agrupamento == 0,
         benef_comunicado > 1) %>%
  distinct()

# We have a fucking database! Now we need to decide a date for the treatment.

merger_date <- as.Date('2022-02-11')

# Now we have to:
# 1) get readjustment data for the full period for that sample. CHECK
# 2) assign treatment status based on the merger date. CHECK
# 3) add controls. 
# 4) estimate

reajustes_sample <- reajustes_sample %>%
  mutate(
    treatment_status = if_else(
      cd_plano %in% treated_plans & dt_inic_aplicacao > merger_date,
      1,
      0
    )
  )

reajustes_sample_MB <- reajustes_sample_MB %>%
  mutate(
    treatment_status = if_else(
      cd_plano %in% treated_plans & dt_inic_aplicacao > merger_date,
      1,
      0
    )
  )

unique_hospitais_planos <- hospitais_planos %>%
  select(cd_plano, no_razao) %>%
  distinct()

reajustes_summary <- reajustes_sample %>%
  group_by(cd_plano) %>%
  summarize(
    has_0 = any(treatment_status == 0),
    has_1 = any(treatment_status == 1)
  ) %>%
  mutate(
    status_type = case_when(
      has_0 & has_1 ~ "Both",
      has_0 ~ "Only 0",
      has_1 ~ "Only 1"
    )
  ) %>%
  left_join(unique_hospitais_planos, by = "cd_plano")

reajustes_summary_MB <- reajustes_sample_MB %>%
  group_by(cd_plano) %>%
  summarize(
    has_0 = any(treatment_status == 0),
    has_1 = any(treatment_status == 1)
  ) %>%
  mutate(
    status_type = case_when(
      has_0 & has_1 ~ "Both",
      has_0 ~ "Only 0",
      has_1 ~ "Only 1"
    )
  ) %>%
  left_join(unique_hospitais_planos, by = "cd_plano")

reajustes_summary <- reajustes_summary %>%
  group_by(no_razao) %>%
  summarize(
    total = n(),
    never_treated = sum(status_type == "Only 0"),
    always_treated = sum(status_type == "Only 1"),
    both = sum(status_type == "Both")
  ) %>%
  arrange(desc(total))

reajustes_summary_MB <- reajustes_summary_MB %>%
  group_by(no_razao) %>%
  summarize(
    total = n(),
    never_treated = sum(status_type == "Only 0"),
    always_treated = sum(status_type == "Only 1"),
    both = sum(status_type == "Both")
  ) %>%
  arrange(desc(total))

aux <- hospitais_planos %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics) %>%
  distinct(cd_plano) %>%
  pull()

hap_tratados_MB <- hospitais_planos %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics,
         cd_plano %in% municipally_bound_plans) %>%
  distinct(cd_plano) %>%
  pull()

hap_tratados <- hospitais_planos %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics) %>%
  distinct(cd_plano) %>%
  pull()

planos_com_dados <- reajustes %>% pull(cd_plano)

intersect(planos_com_dados, hap_tratados)


# Existem 52 planos da Hapvida atuando em cidades com hospitais GNDI, mas
# apenas 25 deles tem dados RPC e apenas 3 deles são MB.


df_did <- reajustes_sample_MB %>%
  select(id_contrato, cd_operadora, cd_plano, dt_inic_aplicacao, percentual,
         lg_introducao_franquia_copt, treatment_status) %>%
  mutate(year = year(dt_inic_aplicacao),
         id_contrato_numeric = as.numeric(as.factor(id_contrato)))

df_did <- df_did %>%
  group_by(id_contrato_numeric) %>%
  mutate(G = ifelse(any(treatment_status == 1),
                    min(year[treatment_status == 1]),
                    0)) %>%
  ungroup()

# Example call:
es_results <- att_gt(
  yname = "percentual",
  tname = "year",
  idname = "id_contrato_numeric",
  gname = "G",
  data = df_did,
  panel = TRUE,
  # Choose appropriate control group, often "never treated"
  control_group = "nevertreated",
  allow_unbalanced_panel = TRUE,
)

es_agg <- aggte(es_results, type = "dynamic")
ggdid(es_agg)


glimpse(df_did)


# Classification (assuming df_did is already loaded and structured)
df_classified <- df_did %>%
  group_by(id_contrato_numeric) %>%
  mutate(first_year = min(year)) %>%
  ungroup() %>%
  mutate(
    treatment_category = case_when(
      G == 0 ~ "never treated",
      G == first_year ~ "always treated",
      G > first_year ~ "eventually treated"
    )
  )

# Get one row per unit
df_units <- df_classified %>%
  distinct(id_contrato_numeric, treatment_category)

# Count units in each category
counts <- df_units %>%
  group_by(treatment_category) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  mutate(label = paste0(treatment_category, "\n", n, " contracts"))

# Bar plot of treatment categories
ggplot(counts, aes(x = treatment_category, y = n, fill = treatment_category)) +
  geom_bar(stat = "identity", color = "black") +
  geom_text(aes(label = n), vjust = -0.5, size = 4) +
  labs(
    title = "Distribution of Treatment Categories",
    x = element_blank(),
    y = "Number of Contracts in the Sample"
  ) +
  theme_classic() +
  theme(legend.position = "none")

treated_lives <- reajustes_sample_MB %>%
  filter(year(dt_inic_aplicacao) == 2022) %>%
  group_by(treatment_status) %>%
  summarize(lives = sum(benef_comunicado))

# Os 258 planos tratados atendem 19.310 vidas

# ---------------

trim_last_char <- function(column) {
  as.numeric(substr(as.character(column), 1, nchar(column) - 1))
}

# Load São Paulo municipalities (SP state has code 35)
sp_municipalities <- read_municipality(code_muni = 35, year = 2020) %>%
  mutate(code_muni = trim_last_char(code_muni))

munics <- c(treated_munics, control_munics)
munic_treatment <- c(rep('treated', length(treated_munics)),
                rep('control', length(control_munics)))
munics_map <- tibble(code_muni = munics,
                     treatment = munic_treatment)
sp_municipalities <- sp_municipalities %>%
  left_join(munics_map, by = "code_muni") %>%
  mutate(group = ifelse(is.na(treatment), "Out of Sample", treatment))

ggplot(data = sp_municipalities) +
  geom_sf(aes(fill = group), color = "white", size = 0.2) +  # Fill based on group
  scale_fill_manual(
    values = c("treated" = "orange", "control" = "darkcyan", "Out of Sample" = "gray"),
    name = "Group"
  ) +
  labs(title = "Treatment Status of Municipalities",
       caption = "Data source: IBGE") +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    panel.grid = element_blank()
  )

sp_municipalities %>%
  left_join(munics_map, by = "code_muni") %>%
  mutate(group = ifelse(is.na(treatment), "Out of Sample", treatment),
         code_muni = trim_last_char(code_muni))



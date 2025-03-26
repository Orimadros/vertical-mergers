library(tidyverse)
library(here)
library(readxl)
library(janitor)
library(VennDiagram)
library(did)
library(geobr)
library(sf)
library(estimatr)
library(broom)
library(stringr)
library(panelView)
library(knitr)
library(kableExtra)
library(haven)
library(arrow)
library(flextable)
library(webshot2)


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

caracteristicas_planos <- read_csv2(here('data', 
                             'raw_data',
                             'ANS',
                             'operadoras',
                             'planos',
                             'caracteristicas_produtos_saude_suplementar.csv')) %>%
  clean_names()  %>%
  filter(contratacao == 'Coletivo empresarial',
         sgmt_assistencial != 'Odontológico') %>%
  rename(cd_operadora = 'registro_operadora')

# Identify duplicate `cd_plano` - `cd_operadora` pairs
dopplegangers <- caracteristicas_planos %>%
  count(cd_plano, cd_operadora, sort = TRUE) %>%  # Count occurrences
  filter(n > 1)  # Keep only duplicates

# Há 274 duplicatas na base com 54627 planos relevantes. Vamos remover as mais
# antigas

caracteristicas_planos <- caracteristicas_planos %>%
  group_by(cd_plano, cd_operadora) %>%  # Group by unique key
  slice_max(dt_situacao, with_ties = FALSE) %>%  # Keep only the latest date
  ungroup()  # Remove grouping

# IMPORTANTE: apenas o id_plano é unico. Pode haver planos diferentes
# com o mesmo cd_plano, mas estes terão id_plano diferentes.

# Usamos a base de informações consolidadas dos beneficiários para identificar
# o grupo de cidades com pelo menos um beneficiário para cada operadora

beneficiarios_cons_202012 <- read_csv2(here('data', 
                                            'raw_data',
                                            'ANS',
                                            'beneficiarios',
                                            'pda-024-icb-SP-2020_12.csv')
                                       #locale = locale(encoding = "Latin1")
) %>%
  clean_names()

beneficiarios_cons_202012 <- beneficiarios_cons_202012 %>%
  filter(de_contratacao_plano == 'Coletivo Empresarial',
         de_segmentacao_plano != 'Odontológico')

# Convert cd_operadora to character in both datasets
beneficiarios_cons_202012 <- beneficiarios_cons_202012 %>%
  mutate(cd_operadora = as.character(cd_operadora))

caracteristicas_planos <- caracteristicas_planos %>%
  mutate(cd_operadora = as.character(cd_operadora))

planos_hospitalares <- caracteristicas_planos %>%
  filter(gr_sgmt_assistencial != 'Ambulatorial') %>%
  distinct(id_plano) %>%
  pull()

# Now perform the join
beneficiarios_cons_202012 <- beneficiarios_cons_202012 %>%
  left_join(
    caracteristicas_planos %>% select(cd_plano, cd_operadora, id_plano),
    by = c("cd_plano", "cd_operadora")
  )

beneficiarios_cons_202305 <- read_csv2(here('data', 
                                            'raw_data',
                                            'ANS',
                                            'beneficiarios',
                                            'pda-024-icb-SP-2023_05.csv')
                                       #locale = locale(encoding = "Latin1")
) %>%
  clean_names()

beneficiarios_cons_202305 <- beneficiarios_cons_202305 %>%
  filter(de_contratacao_plano == 'Coletivo Empresarial',
         de_segmentacao_plano != 'Odontológico')

# Convert cd_operadora to character in both datasets
beneficiarios_cons_202305 <- beneficiarios_cons_202305 %>%
  mutate(cd_operadora = as.character(cd_operadora))

caracteristicas_planos <- caracteristicas_planos %>%
  mutate(cd_operadora = as.character(cd_operadora))

# Now perform the join
beneficiarios_cons_202305 <- beneficiarios_cons_202305 %>%
  left_join(
    caracteristicas_planos %>% select(cd_plano, cd_operadora, id_plano),
    by = c("cd_plano", "cd_operadora")
  )

aux <- union(
  setdiff(beneficiarios_cons_202012 %>% distinct(id_plano) %>% pull(),
          beneficiarios_cons_202305 %>% distinct(id_plano) %>% pull()),
  setdiff(beneficiarios_cons_202305 %>% distinct(id_plano) %>% pull(),
          beneficiarios_cons_202012 %>% distinct(id_plano) %>% pull())
)

# Usamos a base de relações de credenciamento para identificar as redes

# Set directory where Parquet files are stored
parquet_dir <- here('data', 
                    'raw_data',
                    'ANS',
                    'prestadores',
                    'produtos e prestadores hospitalares',
                    'produtos_prestadores_parquet')

# List all Parquet files
parquet_files <- list.files(parquet_dir, pattern = "*.parquet", full.names = TRUE)

# Load all Parquet files into a tibble
hospitais_planos <- do.call(rbind, lapply(parquet_files, function(x) as_tibble(read_parquet(x))))


hospitais_planos <- hospitais_planos %>%
  clean_names() %>%
  filter(
    de_tipo_contratacao == 'COLETIVO EMPRESARIAL',
    de_clas_estb_saude == 'Assistencia Hospitalar'
  )

# Usamos a base de reajustes de planos coletivos para obter nossa
# variável dependente

# Define the range of months
months <- seq(as.Date("2015-01-01"), as.Date("2024-11-01"), by = "month")
file_names <- paste0("PDA_RPC_", format(months, "%Y%m"), ".parquet")  # Use .parquet extension
file_paths <- map(file_names, ~ here("data", "raw_data", "ANS", "operadoras", "reajustes", "reajustes_parquet", .x))

# Import each Parquet file into a list of tibbles
reajustes_list <- map(file_paths, ~ read_parquet(.x))

# Combine into a single tibble, automatically filling missing columns with NA
reajustes <- bind_rows(reajustes_list) %>% 
  clean_names()

# IMPORTANTE: em algum momento, houve uma mudança na base RPC que renomeou,
# tirou e adicionou colunas. abaixo estão os ajustes necessários para conciliar
# as bases.

# Colapsando colunas renomeadas:

reajustes <- reajustes %>%
  mutate(
    benef_comunicado = coalesce(qt_benef_comunicado, benef_comunicado),
    percentual       = coalesce(percentual, pc_percentual)
  ) %>%
  select('id_contrato', 'id_plano', 'cd_operadora', 'cd_plano', "dt_inic_aplicacao",
         "benef_comunicado", "percentual", "lg_parcelado", "lg_retificacao",
         "lg_cancelamento", "sg_uf_contrato_reaj", "cd_agrupamento",
         "lg_negociacao") %>%
  filter(cd_agrupamento == 0,
         is.na(lg_negociacao) | lg_negociacao != 1,
         lg_parcelado != 1) %>%
  mutate(percentual = as.numeric(gsub(",", ".", percentual)))

# O cd_agrupamento == 0 tira planos empresariais com menos de 30 vidas, cujos 
# reajustes são negociados de forma agrupada por operadora.

# O lg_negociacao == 0 tira reajustes que ainda estão em negociação

hospitais_planos <- hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar')

# -------------------------------
# DATABASE COMPARISON
# -------------------------------

# Extract unique values from the 'id_plano' column in each data frame
set_reajustes <- unique(reajustes$id_plano)
set_hospitais <- unique(hospitais_planos$id_plano)
set_beneficiarios <- unique(beneficiarios_cons_202012$id_plano)

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
  distinct(cd_cnes, .keep_all = TRUE)

# Descoberta: todos os hospitais da Hapvida em SP vieram de aquisições, como da
# GNDI, São Francisco, e Ultra Som.

# CNPJ de hospitais Hapvida (e.g. CNES 2081601) estão desatualizados, e não
# podem ser utilizados para identificar Hap_hospitals.

# Próximo passo: identificar planos Hapvida atuando em cidades paulistas com
# hospitais GNDI.

GNDI_hospital_munics <- GNDI_hospitals %>% distinct(cd_municipio) %>% pull()

hap_plans_w_GNDI <- beneficiarios_cons_202012 %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% GNDI_hospital_munics) %>%
  pull(id_plano)

# IMPORTANTE: pra identificar mesmo MB Plans, tem que pegar as bases de
# beneficiarios de todos os estados. Tem planos que sobram com o filtro
# abaixo mas que são predominantemente de tocantins, por exemplo.

municipally_bound_plans <- beneficiarios_cons_202012 %>%
  group_by(id_plano) %>%
  summarize(cidades = n_distinct(cd_municipio)) %>%
  arrange(desc(cidades)) %>%
  filter(cidades == 1) %>%
  pull(id_plano)

#----------

# Defining relevant markets by Hap network + GNDI hospitals

# Defining treated plans as MB plans operating in municipalities where there is 
# vertical integration between merging parties. i.e. 1) there
# is at least one Hapvida plan with a credentialed hospital there and 
# 2) there is at least one GNDI hospital

# Defining control plans as MB plans operating in municipalities where there
# is either hapvida plan network or GNDI hospitals, but not both.

# To identify these plans we first flag municipalities that satisfy the
# conditions above, and then find plans bound in these municipalities.

hap_network_munics <- hospitais_planos %>%
  filter(cd_operadora == 368253) %>%
  distinct(cd_municipio) %>%
  pull()

treated_munics <- intersect(hap_network_munics, GNDI_hospital_munics)

control_munics <- union(
  setdiff(hap_network_munics, GNDI_hospital_munics),
  setdiff(GNDI_hospital_munics, hap_beneficiarios_munics)
)

treated_plans <- hospitais_planos %>% 
  filter(cd_municipio %in% treated_munics) %>%
  distinct(id_plano) %>%
  pull()

control_plans <- hospitais_planos %>% 
  filter(cd_municipio %in% control_munics) %>%
  distinct(id_plano) %>%
  pull()

#----------

# PROBLEMA: muitos hospitais Hapvida não podem ser identificados pelo seu CNPJ.
# e.g. cnes = 6430120

aux <- hospitais_planos %>%
  filter(cd_operadora == 359017) %>%
  count(cd_cnpj_estb_saude) %>%
  arrange(desc(n))

#----------
# Defining relevant markets by Hap beneficiaries + GNDI hospitals

# Defining treated plans as MB plans operating in municipalities where there is 
# vertical integration between merging parties. i.e. 1) there
# is at least one Hapvida beneficiary and 2) there is at least one GNDI hospital

# Defining control plans as MB plans operating in municipalities where there
# is either hapvida beneficiaries or GNDI hospitals, but not both.

# To identify these plans we first flag municipalities that satisfy the
# conditions above, and then find plans bound in these municipalities.

hap_beneficiarios_munics <- beneficiarios_cons_202012 %>%
  filter(cd_operadora == 368253) %>%
  distinct(cd_municipio) %>%
  pull()

treated_munics <- intersect(hap_beneficiarios_munics, GNDI_hospital_munics)

control_munics <- union(
  setdiff(hap_beneficiarios_munics, GNDI_hospital_munics),
  setdiff(GNDI_hospital_munics, hap_beneficiarios_munics)
)

# IMPORTANTE: todos os vinte municípios com hospitais GNDI têm também
# beneficiários Hapvida, e estão no tratamento. Portanto, na prática,
# os municípios controle são apenas aqueles com beneficiários Hapvida mas sem
# hospitais GNDI.

treated_plans <- beneficiarios_cons_202012 %>% 
  filter(cd_municipio %in% treated_munics) %>%
  distinct(id_plano) %>%
  pull()

# DÚVIDA: definir treated_plans pela base de beneficiários retorna bem menos
# planos do que fazendo pela base de convênios. Por que?

control_plans <- beneficiarios_cons_202012 %>% 
  filter(cd_municipio %in% control_munics) %>%
  distinct(id_plano) %>%
  pull()

# Comparação de controle ao longo do tempo
# 172 control munics com base benef 202001
# 195 control munics com base benef 202012
# 488 control munics com base benef 202305

# IMPORTANTE: apesar do tratamento ter 20 municípios e o controle 172, os dois
# têm número parecido de planos (6520 vs 6560) -> tratamento seleciona
# municípios mais relevantes.

#----------



reajustes_sample <- reajustes %>%
  filter(id_plano %in% treated_plans |
           id_plano %in% control_plans,
         id_plano %in% planos_hospitalares,
         sg_uf_contrato_reaj == 'SP',
         cd_agrupamento == 0,
         benef_comunicado > 1) %>%
  distinct()

reajustes_sample_MB <- reajustes %>%
  filter(id_plano %in% treated_plans |
           id_plano %in% control_plans,
         id_plano %in% municipally_bound_plans,
         id_plano %in% planos_hospitalares,
         sg_uf_contrato_reaj == 'SP',
         cd_agrupamento == 0,
         benef_comunicado > 1) %>%
  distinct()


# We have a fucking database! Now we need to decide a date for the treatment.

# Now we have to:
# 1) get readjustment data for the full period for that sample. CHECK
# 2) assign treatment status based on the merger date. CHECK
# 3) add controls. 
# 4) estimate

# -------------------------------
# DESCRITIVAS
# -------------------------------

merger_date <- as.Date('2022-02-11')

# -------------------------------
# TREATMENT STATUS & ESTIMATION
# -------------------------------

# Extract month and year first, then define treated and post in one step
reajustes_sample <- reajustes_sample %>%
  mutate(
    month = month(dt_inic_aplicacao),
    year = year(dt_inic_aplicacao),
    treated = if_else(id_plano %in% treated_plans, 1, 0),
    post = if_else(year >= year(merger_date), 1, 0)
  )

reajustes_sample_MB <- reajustes_sample_MB %>%
  mutate(
    month = month(dt_inic_aplicacao),
    year = year(dt_inic_aplicacao),
    treated = if_else(id_plano %in% treated_plans, 1, 0),
    post = if_else(year >= year(merger_date), 1, 0)
  )

readjustment_check <- reajustes_sample %>%
  group_by(id_contrato, year) %>%
  summarize(
    equal_adjustment = n_distinct(percentual) == 1,
    adjustment_value = first(percentual),  # representative adjustment value if they are all equal
    plan_count = n_distinct(id_plano),
    .groups = "drop"
  )

# removing overriden readjustments when there are corrections
reajustes_sample <- reajustes_sample %>%
  group_by(id_contrato, id_plano, year) %>%
  filter(
    if (any(lg_retificacao == 1, na.rm = TRUE)) {
      lg_retificacao == 1 | is.na(lg_retificacao)
    } else {
      TRUE
    }
  ) %>%
  ungroup()


# there still remain multiple readjustments for a same contract-plan-year.
# e.g. id_contrato = 000037CD55458BB4BE07E4E618425E1669FBD4413CAFFB478D3A17E842AB3682
# those are cases where there were multiple successive readjustments within a 
# single year, which shouldn't happen. We remove these units.

# checking and removing contract-plan-year units with more than one observation

contract_plan_year_counts <- reajustes_sample %>%
  group_by(id_contrato, id_plano, year) %>%
  summarize(count = n(), .groups = "drop")

reajustes_sample <- reajustes_sample %>%
  group_by(id_contrato, id_plano, year) %>%
  filter(n() == 1) %>%
  ungroup()

# describing the gaps in the unbalanced panel data

all_years <- seq(min(reajustes_sample$year), max(reajustes_sample$year))

# Step 1: Compute the number of years per contract-plan
contract_plan_years <- reajustes_sample %>%
  group_by(id_contrato, id_plano) %>%
  summarize(n_years = n_distinct(year), .groups = "drop")

contract_plans_multiple_readj <- contract_plan_year_counts %>%
  filter(count != 1) %>%
  distinct(id_contrato, id_plano)

contract_plan_year_counts %>%
  distinct(id_contrato, id_plano)

# 6.847 de 622.715 contrato-planos possuem anos com mais de um reajuste. 
# Vamos removê-los da base.

reajustes_sample <- reajustes_sample %>%
  anti_join(contract_plans_multiple_readj, by = c("id_contrato", "id_plano"))

# Step 2: Compute quantiles across contract-plan panel lengths
# Here, we create a sequence of quantile probabilities from 0 to 1.
quantile_probs <- seq(0, 1, by = 0.0001)
quantile_data <- tibble(
  prob = quantile_probs,
  n_years = as.numeric(quantile(contract_plan_years$n_years, probs = quantile_probs))
)

# Step 3: Plot the quantile line graph
ggplot(quantile_data, aes(x = prob, y = n_years)) +
  geom_line() +
  labs(
    x = "Quantile",
    y = "Years of Data",
    title = "Quantiles of Years of Data per Contract-Plan"
  ) +
  theme_classic()

# Tibble 1: For each year, count how many contract-plans have data.
contract_plans_per_year <- reajustes_sample %>%
  distinct(id_contrato, id_plano, year) %>%
  group_by(year) %>%
  summarise(n_contract_plans = n(), .groups = "drop")

# Tibble 2: For each unique combination of years, count how many contract-plans have that combination.
year_combinations <- reajustes_sample %>%
  distinct(id_contrato, id_plano, year) %>%
  group_by(id_contrato, id_plano) %>%
  summarise(years = paste(sort(unique(year)), collapse = ", "), .groups = "drop") %>%
  group_by(years) %>%
  summarise(n_contract_plans = n(), .groups = "drop") %>%
  arrange(length(years))

# IMPORTANTE: o contrato-plano com dado para mais anos tem 7 anos de dados. Ou
# seja, não é nem possível estimar a nível do contrato-plano para o período.
# Estimaremos a nível do plano, agregando pela média de reajustes de todos os
# contratos do plano.

reajustes_avg <- reajustes_sample %>%
  group_by(id_plano, year) %>%  # Aggregate by plan and year
  summarize(percentual_avg = mean(percentual, na.rm = TRUE), .groups = "drop")

# Checking balance status on the new plan-level average readjustment dataset

plans_per_year <- reajustes_sample %>%
  distinct(id_plano, year) %>%
  group_by(year) %>%
  summarise(n_plans = n(), .groups = "drop")

year_combinations <- reajustes_sample %>%
  distinct(id_plano, year) %>%  # Get unique plan-year combinations
  group_by(id_plano) %>%  # Group by plan
  summarise(years = paste(sort(unique(year)), collapse = ", "), .groups = "drop") %>%  # Create year string
  group_by(years) %>%  # Group by unique year combinations
  summarise(n_plans = n(), .groups = "drop") %>%  # Count how many plans share the same year combination
  arrange(nchar(years))  # Sort by number of years in the combination

# Let's see, for each year x, how many plans have data for all years from 
# x to 2024:

plans_with_full_data <- reajustes_sample %>%
  distinct(id_plano, year) %>%                      # Unique plan–year combinations
  group_by(id_plano) %>%                            
  summarise(years = list(sort(unique(year))), .groups = "drop") %>% 
  { 
    plans <- .
    tibble(
      year_start = setdiff(unique(unlist(plans$years)), 2024)
    ) %>%
      mutate(
        n_plans = map_int(year_start, 
                          ~ sum(map_lgl(plans$years, function(y) all(seq(.x, 2024) %in% y))))
      )
  } %>%
  arrange(year_start)

reajustes_avg_complete <- reajustes_avg %>%
  group_by(id_plano) %>%
  filter(n_distinct(year) == n_distinct(reajustes_avg$year)) %>%
  ungroup() %>%
  mutate(
    treated = as.integer(id_plano %in% treated_plans),
    post = as.integer(year > year(merger_date))
  )

reajustes_avg_complete_MB <- reajustes_avg_complete %>%
  filter(id_plano %in% municipally_bound_plans)

# Adding covariates to final sample

# --- 1. Compute beneficiary covariates from beneficiarios_cons_202001 ---
benef_cov <- beneficiarios_cons_202012 %>%
  group_by(id_plano) %>%
  summarise(
    total_benef = sum(qt_beneficiario_ativo, na.rm = TRUE),
    pct_59_ou_mais = mean(de_faixa_etaria_reaj == "59 ou mais", na.rm = TRUE),
    .groups = "drop"
  )

# --- 2. Extract plan characteristics from caracteristicas_planos ---
# (Assuming these variables are constant at the plan level.)
plan_char <- caracteristicas_planos %>%
  select(id_plano, obstetricia, abrangencia_cobertura, fator_moderador, 
         acomodacao_hospitalar, livre_escolha) %>%
  distinct()

# --- 3. Compute hospital covariates from hospitais_planos ---
hosp_cov <- hospitais_planos %>%
  group_by(id_plano) %>%
  summarise(
    n_hospitals = n_distinct(cd_cnes),
    .groups = "drop"
  )

# --- 4. Merge all plan-level covariates ---
plan_cov <- benef_cov %>%
  left_join(plan_char, by = "id_plano") %>%
  left_join(hosp_cov, by = "id_plano")

# --- Merge covariates into the final plan-year panel for both samples ---

final_panel <- reajustes_avg_complete %>%
  left_join(plan_cov, by = "id_plano")

final_panel_MB <- reajustes_avg_complete_MB %>%
  left_join(plan_cov, by = "id_plano")


# 2.1 Create indicators
final_panel <- final_panel %>%
  mutate(
    obstetric_coverage = if_else(obstetricia == "Com Obstetrícia", 1, 0),
    copayment = if_else(fator_moderador %in% c("Coparticipação", "Franquia + Coparticipação"), 1, 0),
    individual_accommodations = if_else(acomodacao_hospitalar == "Individual", 1, 0)
  )

# 2.2 Dictionaries for the multi-level categorical variables
abrangencia_labels <- c(
  "Nacional"            = "National Coverage",
  "Estadual"            = "State Coverage",
  "Grupo de municípios" = "Group of Municipalities Coverage",
  "Grupo de estados"    = "Group of States Coverage"
)

# Changed label from "Full Free Choice" to "Full Out-of-Network Coverage"
livre_escolha_labels <- c(
  "Não se aplica"         = "Not Applicable",
  "Total"                 = "Full Out-of-Network Coverage"
)

# 2.3 Recode the original variables using these dictionaries
final_panel <- final_panel %>%
  mutate(
    abrangencia_cobertura_recode = if_else(
      abrangencia_cobertura %in% names(abrangencia_labels),
      abrangencia_labels[abrangencia_cobertura],
      abrangencia_cobertura
    ),
    livre_escolha_recode = if_else(
      livre_escolha %in% names(livre_escolha_labels),
      livre_escolha_labels[livre_escolha],
      livre_escolha
    )
  )


# --- Export both datasets to Stata (.dta) files ---

write_dta(final_panel, here("data", "processed_data", "reajustes_avg_complete_benef_202012_from2015.dta"))
write_dta(final_panel_MB, here("data", "processed_data", "reajustes_avg_complete_benef_MB_202012_from2015.dta"))

#--------
# Producing descriptive tables

#-------------------------------------------------
# 1. Compute the number of plans in each group
#-------------------------------------------------
n_control_plans <- final_panel %>%
  filter(treated == 0) %>%
  distinct(id_plano) %>%
  nrow()

n_treated_plans <- final_panel %>%
  filter(treated == 1) %>%
  distinct(id_plano) %>%
  nrow()

n_diff <- n_treated_plans - n_control_plans

#-------------------------------------------------
# 2. Helper function to compute stats with significance
#-------------------------------------------------
compute_stats <- function(vec, group_var) {
  vec_control <- vec[group_var == 0]
  vec_treated <- vec[group_var == 1]
  
  mean_control <- mean(vec_control, na.rm = TRUE)
  sd_control   <- sd(vec_control, na.rm = TRUE)
  mean_treated <- mean(vec_treated, na.rm = TRUE)
  sd_treated   <- sd(vec_treated, na.rm = TRUE)
  
  n_control <- sum(group_var == 0, na.rm = TRUE)
  n_treated <- sum(group_var == 1, na.rm = TRUE)
  
  diff_val <- mean_treated - mean_control
  diff_se  <- sqrt((sd_control^2 / n_control) + (sd_treated^2 / n_treated))
  
  # Welch's degrees of freedom
  if (n_control < 2 | n_treated < 2) {
    df <- Inf
  } else {
    df <- ((sd_control^2 / n_control + sd_treated^2 / n_treated)^2) / 
      (((sd_control^2 / n_control)^2)/(n_control - 1) + ((sd_treated^2 / n_treated)^2)/(n_treated - 1))
    if (is.na(df) || df <= 0) df <- Inf
  }
  
  t_val <- diff_val / diff_se
  p_val <- 2 * pt(-abs(t_val), df)
  
  sig <- ifelse(p_val < 0.01, "***",
                ifelse(p_val < 0.05, "**",
                       ifelse(p_val < 0.10, "*", "")))
  
  list(
    control = sprintf("%.2f (%.2f)", mean_control, sd_control),
    treated = sprintf("%.2f (%.2f)", mean_treated, sd_treated),
    diff    = sprintf("%.2f (%.2f)%s", diff_val, diff_se, sig)
  )
}

#-------------------------------------------------
# 3. Build the final table row by row
#-------------------------------------------------
rows_list <- list()

# 3.1 Insert row for the number of plans in each group
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Number of Plans",
  Control    = as.character(n_control_plans),
  Treated    = as.character(n_treated_plans),
  Difference = as.character(n_diff),
  stringsAsFactors = FALSE
)

# 3.2 Network Size
res_net <- compute_stats(final_panel$n_hospitals, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Network Size",
  Control    = res_net$control,
  Treated    = res_net$treated,
  Difference = res_net$diff,
  stringsAsFactors = FALSE
)

# 3.3 Total Beneficiaries
res_benef <- compute_stats(final_panel$total_benef, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Total Beneficiaries",
  Control    = res_benef$control,
  Treated    = res_benef$treated,
  Difference = res_benef$diff,
  stringsAsFactors = FALSE
)

# 3.4 Percentage Aged 59+ (convert fraction to %)
res_59 <- compute_stats(final_panel$pct_59_ou_mais * 100, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Percentage Aged 59+",
  Control    = res_59$control,
  Treated    = res_59$treated,
  Difference = res_59$diff,
  stringsAsFactors = FALSE
)

# 3.5 Percentage with Obstetric Coverage
res_ob <- compute_stats(final_panel$obstetric_coverage * 100, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Percentage with Obstetric Coverage",
  Control    = res_ob$control,
  Treated    = res_ob$treated,
  Difference = res_ob$diff,
  stringsAsFactors = FALSE
)

# 3.6 Geographic Coverage (recode) - multi-level
levels_abr <- sort(unique(final_panel$abrangencia_cobertura_recode))
for (lev in levels_abr) {
  vec_cat <- as.numeric(final_panel$abrangencia_cobertura_recode == lev) * 100
  row_name <- paste("Percentage with", lev)
  
  res_cat <- compute_stats(vec_cat, final_panel$treated)
  rows_list[[length(rows_list) + 1]] <- data.frame(
    Variable   = row_name,
    Control    = res_cat$control,
    Treated    = res_cat$treated,
    Difference = res_cat$diff,
    stringsAsFactors = FALSE
  )
}

# 3.7 Percentage with Copayment
res_co <- compute_stats(final_panel$copayment * 100, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Percentage with Copayment",
  Control    = res_co$control,
  Treated    = res_co$treated,
  Difference = res_co$diff,
  stringsAsFactors = FALSE
)

# 3.8 Free Choice (recode) - multi-level
excluded_categories <- c("Ausente", "Parcial com internação", "Parcial sem internação")
valid_indices <- !final_panel$livre_escolha %in% excluded_categories

levels_le <- sort(unique(final_panel$livre_escolha_recode))
for (lev in levels_le) {
  vec_le <- rep(0, nrow(final_panel))
  vec_le[final_panel$livre_escolha_recode == lev & valid_indices] <- 1
  vec_le <- vec_le * 100
  
  if (sum(vec_le) == 0) next
  
  row_name <- paste("Percentage with", lev)
  res_le <- compute_stats(vec_le, final_panel$treated)
  
  rows_list[[length(rows_list) + 1]] <- data.frame(
    Variable   = row_name,
    Control    = res_le$control,
    Treated    = res_le$treated,
    Difference = res_le$diff,
    stringsAsFactors = FALSE
  )
}

# 3.9 Percentage with Individual Accommodations
res_ac <- compute_stats(final_panel$individual_accommodations * 100, final_panel$treated)
rows_list[[length(rows_list) + 1]] <- data.frame(
  Variable   = "Percentage with Individual Accommodations",
  Control    = res_ac$control,
  Treated    = res_ac$treated,
  Difference = res_ac$diff,
  stringsAsFactors = FALSE
)

# Combine all rows into a single data frame
final_table <- do.call(rbind, rows_list)

#-------------------------------------------------
# STEP 5: CREATE A PUBLICATION-QUALITY TABLE
#-------------------------------------------------
ft_final <- flextable(final_table) %>%
  set_header_labels(
    Variable   = "Variable",
    Control    = "Control",
    Treated    = "Treated",
    Difference = "Difference"
  ) %>%
  theme_booktabs() %>%
  autofit() %>%
  fontsize(size = 10, part = "all") %>%
  align(align = "center", part = "all")

# Save the flextable as a PNG image (named "final_descriptive_table.png")
save_as_image(ft_final, path = here('data', 'images', "descriptive_table.png"))

#--------
final_panel %>% distinct(livre_escolha_recode)
# A amostra definida pela base de beneficiários de 2023_05 tem 1333 planos
# A amostra definida pela base de beneficiários de 2020_12 tem 1312 planos
# As duas amostras tem 1308 planos em comum. 
# 2023_05 tem 25 próprios
# 2020_12 tem 4 próprios

for(x in 2015:2019) {
  expected_years <- seq(x, 2024)
  
  subset_df <- reajustes_avg %>%
    filter(year >= x) %>% 
    group_by(id_plano) %>%
    filter(n_distinct(year) == length(expected_years)) %>%  # keep plans with data for every year from x to 2024
    ungroup() %>%
    mutate(
      treated = as.integer(id_plano %in% treated_plans),
      post = as.integer(year > year(merger_date))
    )
  
  file_name <- paste0("reajustes_avg_complete_benef_202012_from", x, ".dta")
  write_dta(subset_df, here("data", "processed_data", file_name))
}

# Create a plan-to-municipality mapping for municipally bound plans
munic_mapping <- beneficiarios_cons_202012 %>%
  filter(id_plano %in% municipally_bound_plans) %>%
  distinct(id_plano, cd_municipio)

# Aggregate plan-year data to municipality-year panel
municipality_panel <- reajustes_avg_complete %>%
  filter(id_plano %in% municipally_bound_plans) %>%
  left_join(munic_mapping, by = "id_plano") %>%
  group_by(cd_municipio, year) %>%
  summarize(
    avg_percentual = mean(percentual_avg, na.rm = TRUE),
    treated = mean(treated),   # average; if all plans are treated this equals 1, if all are control equals 0
    post = mean(post),
    n_plans = n_distinct(id_plano),
    .groups = "drop"
  )


#------------------
# Estimating with contract-years

# Step 1: Identify contract-plans with data in all required years
valid_contract_plans <- reajustes_sample %>%
  distinct(id_contrato, id_plano, year) %>%
  group_by(id_contrato, id_plano) %>%
  summarize(has_required = all(c(2020, 2021, 2022, 2023) %in% year),
            .groups = "drop") %>%
  filter(has_required) %>%
  select(id_contrato, id_plano)

# Step 2: Create the subsample dataset by keeping only valid contract-plans
subsample_data <- reajustes_sample %>%
  semi_join(valid_contract_plans, by = c("id_contrato", "id_plano"))

# checking if we have a balanced panel
subsample_data %>%
  distinct(id_contrato, id_plano, year) %>%
  group_by(year) %>%
  summarise(n_contract_plans = n(), .groups = "drop")

# Let's understand what kind of monster is this subsample_data

glimpse(subsample_data)

balanced_treated <- subsample_data %>%
  filter(treated == 1) %>%
  distinct(id_plano, cd_operadora)

balanced_treated_operadoras <- balanced_treated %>% 
  count(cd_operadora) %>%
  left_join(hospitais_planos %>% distinct(cd_operadora, no_razao), by = 'cd_operadora')

balanced_control <- subsample_data %>%
  filter(treated == 0) %>%
  distinct(id_plano, cd_operadora)

balanced_control_operadoras <- balanced_control %>% 
  count(cd_operadora) %>%
  left_join(hospitais_planos %>% distinct(cd_operadora, no_razao), by = 'cd_operadora')

# Apenas 13 dos 1255 planos tratados na base balanceada são Hapvida.
# 0 dos 182 planos controle na base balanceada são Hapvida. Mais de metade é unimed.

## (A) Get unique plan-level treatment status from subsample_data

plan_status <- reajustes_sample %>%
  distinct(id_plano, cd_operadora, treated)

plan_status <- subsample_data %>%
  distinct(id_plano, cd_operadora, treated)

plan_status <- subsample_data_singletons %>%
  distinct(id_plano, cd_operadora, treated)

## (B) Plan-level continuous summaries from beneficiaries and hospitals

# 1. Beneficiary summary (continuous): total beneficiaries and percentage female
benef_summary_cont <- beneficiarios_cons_202012 %>%
  group_by(id_plano, cd_operadora) %>%
  summarise(
    total_benef = sum(qt_beneficiario_ativo, na.rm = TRUE),
    pct_female = mean(tp_sexo == "F", na.rm = TRUE),
    .groups = "drop"
  )

# 2. Age distribution: calculate, for each plan, the proportion in each age group
age_prop <- beneficiarios_cons_202012 %>%
  group_by(id_plano, cd_operadora, de_faixa_etaria) %>%
  summarise(n_age = n(), .groups = "drop") %>%
  group_by(id_plano, cd_operadora) %>%
  mutate(prop_age = n_age / sum(n_age)) %>%
  ungroup() %>%
  select(id_plano, cd_operadora, de_faixa_etaria, prop_age) %>%
  pivot_wider(names_from = de_faixa_etaria, values_from = prop_age, values_fill = 0)

# 3. Geographic coverage: calculate, for each plan, the proportion in each coverage category
geog_prop <- beneficiarios_cons_202012 %>%
  group_by(id_plano, cd_operadora, de_abrg_geografica_plano) %>%
  summarise(n_geog = n(), .groups = "drop") %>%
  group_by(id_plano, cd_operadora) %>%
  mutate(prop_geog = n_geog / sum(n_geog)) %>%
  ungroup() %>%
  select(id_plano, cd_operadora, de_abrg_geografica_plano, prop_geog) %>%
  pivot_wider(names_from = de_abrg_geografica_plano, values_from = prop_geog, values_fill = 0)

# 4. Hospital summary: number of distinct hospitals and average moderator factor
hosp_summary <- hospitais_planos %>%
  group_by(id_plano, cd_operadora) %>%
  summarise(
    num_hospitals = n_distinct(cd_cnes),
    avg_fator_moderador = mean(lg_fator_moderador, na.rm = TRUE),
    .groups = "drop"
  )

## (C) Merge all summaries with the plan-level treatment status
combined <- plan_status %>%
  left_join(benef_summary_cont, by = c("id_plano", "cd_operadora")) %>%
  left_join(age_prop, by = c("id_plano", "cd_operadora")) %>%
  left_join(geog_prop, by = c("id_plano", "cd_operadora")) %>%
  left_join(hosp_summary, by = c("id_plano", "cd_operadora"))

## (D) Define a helper function to run a t-test for a given variable
balance_test <- function(var) {
  t_res <- t.test(get(var) ~ treated, data = combined)
  data.frame(
    Variable = var,
    Treated_Mean = mean(combined[[var]][combined$treated == 1], na.rm = TRUE),
    Control_Mean = mean(combined[[var]][combined$treated == 0], na.rm = TRUE),
    p_value = t_res$p.value
  )
}

## (E) Run t-tests for continuous variables
vars_cont <- c("total_benef", "pct_female", "num_hospitals", "avg_fator_moderador")
balance_cont <- do.call(rbind, lapply(vars_cont, balance_test))

## (F) Run t-tests for age distribution proportions
# Identify the columns corresponding to age groups (from age_prop pivot)
age_vars <- setdiff(names(age_prop), c("id_plano", "cd_operadora"))
balance_age <- do.call(rbind, lapply(age_vars, balance_test))

## (G) Run t-tests for geographic coverage proportions
geog_vars <- setdiff(names(geog_prop), c("id_plano", "cd_operadora"))
balance_geog <- do.call(rbind, lapply(geog_vars, balance_test))

## (H) Combine all balance test results into one table
balance_all <- bind_rows(balance_cont, balance_age, balance_geog)

# Ensure correct encoding
formatC(balance_all$Control_Mean, format = "f", digits = 2)
# Format the table (assuming 'balance_all' is already loaded in your environment)
balance_all$Treated_Mean <- formatC(balance_all$Treated_Mean, format = "f", digits = 2)
balance_all$Control_Mean <- formatC(balance_all$Control_Mean, format = "f", digits = 2)
balance_all$p_value <- formatC(balance_all$p_value, format = "e", digits = 2)

balance_all[] <- lapply(balance_all, function(x) iconv(x, from = "latin1", to = "UTF-8"))

kable(balance_all, format = "html", caption = "Comparison of Treated and Control Means", digits = 2) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed"), full_width = F) %>%
  column_spec(1, width = "20em") %>%
  column_spec(2, width = "10em") %>%
  column_spec(3, width = "10em")

# Step 3: Join the treated plans with the hospitals summary
treated_plan_hosp <- balanced_treated %>%
  left_join(hosp_summary, by = c("id_plano", "cd_operadora"))

contract_year_counts <- subsample_data %>%
  group_by(id_contrato, year) %>%
  summarise(count = n(), .groups = "drop")

# Create a summary data frame by contract-year
contract_year_diff <- subsample_data %>%
  group_by(id_contrato, year) %>%
  summarise(
    n_plans = n_distinct(id_plano),
    n_percentual = n_distinct(percentual),
    # Flag if there is more than one distinct percentual value
    percentual_different = if_else(n_percentual > 1, 1, 0),
    .groups = "drop"
  ) %>%
  # Keep only contract-years with more than one plan
  filter(n_plans > 1)

# Calculate overall summary statistics: the proportion of contract-years with differing percentuals
diff_summary <- contract_year_diff %>%
  summarise(
    total_contract_years = n(),
    contract_years_with_diff = sum(percentual_different),
    prop_diff = contract_years_with_diff / total_contract_years
  )

#################

duplicates_vs_singletons <- contract_year_counts %>%
  mutate(type = if_else(count == 1, "singleton", "duplicate")) %>%
  group_by(type) %>%
  summarise(count = n())

# Filter for singleton groups (only one observation)
singleton_contract_years <- contract_year_counts %>%
  filter(count == 1)

# Create a new dataset keeping only rows corresponding to singleton contract-years
subsample_data_singletons <- subsample_data %>%
  semi_join(singleton_contract_years, by = c("id_contrato", "year"))


#--------------------------

aux <- hospitais_planos %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics) %>%
  distinct(id_plano) %>%
  pull()

hap_tratados_MB <- beneficiarios_cons_202012 %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics,
         id_plano %in% municipally_bound_plans) %>%
  distinct(id_plano) %>%
  pull()

hap_tratados <- beneficiarios_cons_202012 %>%
  filter(cd_operadora == 368253,
         cd_municipio %in% treated_munics) %>%
  distinct(id_plano) %>%
  pull()

planos_com_dados <- reajustes %>% distinct(id_plano) %>% pull()

intersect(planos_com_dados, hap_tratados_MB)


# Existem 306 planos da Hapvida atuando em cidades com hospitais GNDI, mas
# apenas 271 deles tem dados RPC e apenas 51 deles são MB.

df_did <- reajustes_sample_MB %>%
  select(id_contrato, cd_operadora, id_plano, dt_inic_aplicacao, percentual,
         lg_introducao_franquia_copt, treatment_status) %>%
  mutate(year = year(dt_inic_aplicacao),
         id_contrato_numeric = as.numeric(as.factor(id_contrato)))


# Os 258 planos tratados atendem 19.310 vidas

# ---------------

# -----------------------------
# Load Required Packages
# -----------------------------
library(tidyverse)
library(geobr)
library(sf)

# -----------------------------
# Define Helper Function
# -----------------------------
# This function removes the last digit from the municipality code
# and converts the result to numeric.
trim_last_char <- function(column) {
  as.numeric(substr(as.character(column), 1, nchar(column) - 1))
}

# -----------------------------
# 1. Read Municipality Shapes
# -----------------------------
# Here we read municipality shapes for the state of São Paulo (code_muni = 35)
# and apply the trim_last_char function so the codes become numeric.
sp_municipalities <- read_municipality(code_muni = 35, year = 2020) %>%
  mutate(code_muni = trim_last_char(code_muni))

# -----------------------------
# 2. Define Treated and Control Municipality Codes
# -----------------------------
# Replace these example codes with the actual codes in your dataset.

# We combine them and label them as "treated" or "control."
# Convert them to numeric so they match sp_municipalities$code_muni.
munics <- c(treated_munics, control_munics)
munic_treatment <- c(rep("treated", length(treated_munics)),
                     rep("control", length(control_munics)))

munics_map <- tibble(
  code_muni  = as.numeric(munics),    # ensure numeric
  treatment  = munic_treatment
)

# -----------------------------
# 3. Join and Classify
# -----------------------------
# We join sp_municipalities with munics_map by "code_muni"
# and classify any municipality not in munics_map as "Out of Sample."
sp_municipalities <- sp_municipalities %>%
  left_join(munics_map, by = "code_muni") %>%
  mutate(group = ifelse(is.na(treatment), "Out of Sample", treatment))

# -----------------------------
# 4. Plot the Map
# -----------------------------
# We use ggplot2 to visualize which municipalities are treated vs. control.
ggplot(data = sp_municipalities) +
  geom_sf(aes(fill = group), color = "white", size = 0.2) +
  scale_fill_manual(
    values = c("treated" = "orange", "control" = "darkcyan", "Out of Sample" = "lightgrey"),
    name = "Group"
  ) +
  labs(
    title = "Treatment Status of Municipalities in São Paulo",
    caption = "Data source: IBGE & geobr"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    panel.grid = element_blank()
  )


sp_municipalities %>%
  left_join(munics_map, by = "code_muni") %>%
  mutate(group = ifelse(is.na(treatment), "Out of Sample", treatment),
         code_muni = trim_last_char(code_muni))


#----------

# --- BENEFICIÁRIOS BRANCH ---

# Define municipalities from beneficiaries data
hap_beneficiarios_munics <- beneficiarios_cons_202012 %>%
  filter(cd_operadora == 368253) %>%
  distinct(cd_municipio) %>%
  pull()

# Treated and control municipalities (beneficiários approach)
treated_munics_ben <- intersect(hap_beneficiarios_munics, GNDI_hospital_munics)
control_munics_ben <- union(
  setdiff(hap_beneficiarios_munics, GNDI_hospital_munics),
  setdiff(GNDI_hospital_munics, hap_beneficiarios_munics)
)

# Define treated and control plans based on beneficiary data
treated_plans_ben <- beneficiarios_cons_202012 %>% 
  filter(cd_municipio %in% treated_munics_ben) %>%
  distinct(id_plano) %>%
  pull()

control_plans_ben <- beneficiarios_cons_202012 %>% 
  filter(cd_municipio %in% control_munics_ben) %>%
  distinct(id_plano) %>%
  pull()

# Initial beneficiaries-based sample: plan-year observations in reajustes_avg for plans in either group
sample_ben_initial <- reajustes_avg %>%
  filter(id_plano %in% c(treated_plans_ben, control_plans_ben)) %>%
  mutate(treatment = if_else(id_plano %in% treated_plans_ben, "Treated", "Control"))

count_initial_ben <- sample_ben_initial %>%
  group_by(treatment) %>%
  summarize(n_plans = n_distinct(id_plano), .groups = "drop") %>%
  mutate(step = "Initial", branch = "Beneficiários")

# Balanced sample: only those plans with observations for every year (i.e. balanced panel)
sample_ben_balanced <- sample_ben_initial %>%
  group_by(id_plano) %>%
  filter(n_distinct(year) == n_distinct(reajustes_avg$year)) %>%
  ungroup()

count_balanced_ben <- sample_ben_balanced %>%
  group_by(treatment) %>%
  summarize(n_plans = n_distinct(id_plano), .groups = "drop") %>%
  mutate(step = "Balanced", branch = "Beneficiários")

# --- HOSPITAL NETWORK BRANCH ---

# Define municipalities from hospital network data
hap_network_munics <- hospitais_planos %>%
  filter(cd_operadora == 368253) %>%
  distinct(cd_municipio) %>%
  pull()

# Treated and control municipalities (hospital network approach)
treated_munics_net <- intersect(hap_network_munics, GNDI_hospital_munics)
control_munics_net <- union(
  setdiff(hap_network_munics, GNDI_hospital_munics),
  setdiff(GNDI_hospital_munics, hap_network_munics)
)

# Define treated and control plans based on hospital network data
treated_plans_net <- hospitais_planos %>% 
  filter(cd_municipio %in% treated_munics_net) %>%
  distinct(id_plano) %>%
  pull()

control_plans_net <- hospitais_planos %>% 
  filter(cd_municipio %in% control_munics_net) %>%
  distinct(id_plano) %>%
  pull()

# Initial hospital network-based sample
sample_net_initial <- reajustes_avg %>%
  filter(id_plano %in% c(treated_plans_net, control_plans_net)) %>%
  mutate(treatment = if_else(id_plano %in% treated_plans_net, "Treated", "Control"))

count_initial_net <- sample_net_initial %>%
  group_by(treatment) %>%
  summarize(n_plans = n_distinct(id_plano), .groups = "drop") %>%
  mutate(step = "Initial", branch = "Hospital Network")

# Balanced sample for hospital network branch
sample_net_balanced <- sample_net_initial %>%
  group_by(id_plano) %>%
  filter(n_distinct(year) == n_distinct(reajustes_avg$year)) %>%
  ungroup()

count_balanced_net <- sample_net_balanced %>%
  group_by(treatment) %>%
  summarize(n_plans = n_distinct(id_plano), .groups = "drop") %>%
  mutate(step = "Balanced", branch = "Hospital Network")

# --- Combine the counts for reporting ---
report <- bind_rows(count_initial_ben, count_balanced_ben, count_initial_net, count_balanced_net) %>%
  select(branch, step, treatment, n_plans)

#-------

# ------------------------------
# Helper function (no geographic coverage)
# ------------------------------
create_balance_table_no_geog <- function(sample_df, branch, step) {
  
  # Create plan-level status using only id_plano and the treatment indicator.
  plan_status <- sample_df %>%
    distinct(id_plano, treatment) %>%
    mutate(treated = if_else(treatment == "Treated", 1, 0))
  
  # 1. Beneficiary summary: total beneficiaries and percentage female
  benef_summary_cont <- beneficiarios_cons_202012 %>%
    group_by(id_plano) %>%
    summarise(
      total_benef = sum(qt_beneficiario_ativo, na.rm = TRUE),
      pct_female = mean(tp_sexo == "F", na.rm = TRUE),
      .groups = "drop"
    )
  
  # 2. Age distribution: calculate, for each plan, the proportion in each age group
  age_prop <- beneficiarios_cons_202012 %>%
    group_by(id_plano, de_faixa_etaria) %>%
    summarise(n_age = n(), .groups = "drop") %>%
    group_by(id_plano) %>%
    mutate(prop_age = n_age / sum(n_age)) %>%
    ungroup() %>%
    select(id_plano, de_faixa_etaria, prop_age) %>%
    pivot_wider(names_from = de_faixa_etaria, values_from = prop_age, values_fill = 0)
  
  # 3. Hospital summary: number of distinct hospitals and average moderator factor
  hosp_summary <- hospitais_planos %>%
    group_by(id_plano) %>%
    summarise(
      num_hospitals = n_distinct(cd_cnes),
      avg_fator_moderador = mean(lg_fator_moderador, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Merge summaries with plan-level status
  combined <- plan_status %>%
    left_join(benef_summary_cont, by = "id_plano") %>%
    left_join(age_prop, by = "id_plano") %>%
    left_join(hosp_summary, by = "id_plano")
  
  # Helper function for t-tests
  balance_test <- function(var) {
    t_res <- t.test(get(var) ~ treated, data = combined)
    data.frame(
      Variable = var,
      Treated_Mean = mean(combined[[var]][combined$treated == 1], na.rm = TRUE),
      Control_Mean = mean(combined[[var]][combined$treated == 0], na.rm = TRUE),
      p_value = t_res$p.value
    )
  }
  
  # Run t-tests for the continuous variables from the beneficiary and hospital summaries
  vars_cont <- c("total_benef", "pct_female", "num_hospitals", "avg_fator_moderador")
  balance_cont <- do.call(rbind, lapply(vars_cont, balance_test))
  
  # Run t-tests for age distribution proportions (all columns from age_prop except id_plano)
  age_vars <- setdiff(names(age_prop), "id_plano")
  balance_age <- do.call(rbind, lapply(age_vars, balance_test))
  
  # Combine results
  balance_all <- bind_rows(balance_cont, balance_age) %>%
    mutate(Branch = branch, Step = step)
  
  # Format numeric columns for reporting
  balance_all <- balance_all %>%
    mutate(across(c(Treated_Mean, Control_Mean), ~ formatC(.x, format = "f", digits = 2)),
           p_value = formatC(p_value, format = "e", digits = 2))
  
  return(balance_all)
}

# ------------------------------
# Apply the function to each subgroup
# ------------------------------

# Beneficiários branch:
balance_ben_initial   <- create_balance_table_no_geog(sample_ben_initial, branch = "Beneficiários", step = "Initial")
balance_ben_balanced  <- create_balance_table_no_geog(sample_ben_balanced, branch = "Beneficiários", step = "Balanced")

# Combine the two beneficiaries subgroups into one table.
balance_ben <- bind_rows(balance_ben_initial, balance_ben_balanced)

# Network branch:
balance_net_initial   <- create_balance_table_no_geog(sample_net_initial, branch = "Network", step = "Initial")
balance_net_balanced  <- create_balance_table_no_geog(sample_net_balanced, branch = "Network", step = "Balanced")

# Combine the two network subgroups into one table.
balance_net <- bind_rows(balance_net_initial, balance_net_balanced)

# ------------------------------
# Report: one table for each branch
# ------------------------------

# Beneficiários balance check table:
balance_ben %>%
  kable(format = "html", caption = "Balance Check – Beneficiários Branch", digits = 2) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed"), full_width = FALSE) %>%
  column_spec(1, width = "20em") %>%
  column_spec(2, width = "10em") %>%
  column_spec(3, width = "10em") %>%
  print()

# Network balance check table:
balance_net %>%
  kable(format = "html", caption = "Balance Check – Network Branch", digits = 2) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed"), full_width = FALSE) %>%
  column_spec(1, width = "20em") %>%
  column_spec(2, width = "10em") %>%
  column_spec(3, width = "10em") %>%
  print()



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
         str_detect(str_to_lower(sgmt_assistencial), 'hospitalar')) %>%
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
                                            '2020',
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

relevant_plans <- caracteristicas_planos %>%
  filter(
    contratacao == 'Coletivo empresarial',
    gr_sgmt_assistencial != 'Ambulatorial'
  ) %>%
  distinct(id_plano) %>%
  pull()

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
         lg_parcelado != 1,
         id_plano %in% relevant_plans
  ) %>%
  mutate(percentual = as.numeric(gsub(",", ".", percentual)))




# O cd_agrupamento == 0 tira planos empresariais com menos de 30 vidas, cujos 
# reajustes são negociados de forma agrupada por operadora.

# O lg_negociacao == 0 tira reajustes que ainda estão em negociação

hospitais_planos <- hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar')

pib_municipios <- read_csv(here('data',
                                'raw_data',
                                'municipios',
                                'br_ibge_pib_municipio.csv'))

pib_municipios %>% 
  mutate(id_municipio = substring(as.character(id_municipio), 1, 6)) %>% 
  filter(ano == 2020) %>%
  select(id_municipio, pib)



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

intersect(control_plans, treated_plans)

# Comparação de controle ao longo do tempo
# 172 control munics com base benef 202001
# 195 control munics com base benef 202012
# 488 control munics com base benef 202305

# IMPORTANTE: apesar do tratamento ter 20 municípios e o controle 172, os dois
# têm número parecido de planos (6520 vs 6560) -> tratamento seleciona
# municípios mais relevantes.


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

# ------------------------------
# Estimating with municipality-level aggregation
# ------------------------------

# To estimate in the municipality level, we're gonna need beneficiary data for
# all years to calculate the weighted average of readjustments for each year.


# -------------------------------
# STEP 1: PROCESS BENEFICIARIES DATA
# -------------------------------

years <- 2015:2024

# Prepare output directories
dir.create(here("data", "processed_data"), showWarnings = FALSE, recursive = TRUE)
dir.create(here("data", "processed_data", "temp"), showWarnings = FALSE, recursive = TRUE)

# Load caracteristicas_planos (assuming it's needed and loaded somewhere above)
# caracteristicas_planos <- ... load your data here ...

# Loop through each year and process it in chunks
for (yr in years) {
  file_path <- here("data", "raw_data", "ANS", "beneficiarios", as.character(yr),
                    paste0("pda-024-icb-SP-", yr, "_12.csv"))
  
  message("Processing year: ", yr)
  
  # Create a temp directory for this year's chunks
  year_temp_dir <- here("data", "processed_data", "temp", as.character(yr))
  dir.create(year_temp_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Check if file exists before processing
  if (!file.exists(file_path)) {
    warning("File not found for year ", yr, ": ", file_path)
    next # Skip to the next year
  }
  
  # Define callback function for chunked processing
  chunk_callback <- DataFrameCallback$new(function(chunk, pos) {
    processed_chunk <- chunk %>%
      clean_names() %>%
      rename(id_cmpt_movel = any_of(c("id_cmpt_movel", "#id_cmpt_movel", "number_id_cmpt_movel"))) %>%
      mutate(
        id_cmpt_movel = case_when(
          str_detect(as.character(id_cmpt_movel), "-") ~ as.character(id_cmpt_movel),
          TRUE ~ paste0(substr(as.character(id_cmpt_movel), 1, 4), "-", substr(as.character(id_cmpt_movel), 5, 6))
        ),
        dt_carga = as.character(dt_carga),
        cd_operadora = as.character(cd_operadora),
        year = yr
      ) %>%
      filter(
        str_to_lower(de_contratacao_plano) %in% c("coletivo empresarial", "coletivo por adesão"),
        !str_detect(str_to_lower(de_segmentacao_plano), "odontol[óo]gico")
      ) %>%
      left_join(
        caracteristicas_planos %>% select(cd_plano, cd_operadora, id_plano),
        by = c("cd_plano", "cd_operadora")
      ) %>%
      select(
        id_cmpt_movel, cd_operadora, nm_razao_social, nr_cnpj, modalidade_operadora, sg_uf,
        cd_municipio, tp_sexo, de_faixa_etaria, de_faixa_etaria_reaj, cd_plano,
        de_contratacao_plano, de_abrg_geografica_plano, qt_beneficiario_ativo, dt_carga,
        id_plano, year
      )
    
    # Write to individual chunk file
    chunk_file <- file.path(year_temp_dir, paste0("chunk_", sprintf("%05d", pos), ".parquet"))
    write_parquet(processed_chunk, chunk_file)
    
    return(data.frame())  # Return empty dataframe to save memory
  })
  
  # Process file in chunks
  tryCatch({
    read_csv2_chunked(
      file = file_path,
      callback = chunk_callback,
      chunk_size = 100000,
      locale = locale(encoding = "latin1")
    )
  }, error = function(e) {
    warning("Error processing file for year ", yr, ": ", e$message)
  })
  
  # Combine chunks for the year into a single partitioned dataset directory
  message("Combining chunks for year ", yr)
  
  year_chunk_files <- list.files(year_temp_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  if (length(year_chunk_files) > 0) {
    year_dataset <- open_dataset(year_chunk_files)
    
    # Define final output DIRECTORY path, using Hive-style partitioning
    output_dir_year <- here("data", "processed_data", "national", "beneficiaries_yearly") # Base directory
    output_partition_dir <- file.path(output_dir_year, paste0("year=", yr)) # Subdirectory like year=2015
    
    # Check if output directory for the year already exists and remove it if necessary
    if (dir.exists(output_partition_dir)) {
      warning("Overwriting existing directory: ", output_partition_dir)
      unlink(output_partition_dir, recursive = TRUE)
    }
    
    # Write the combined data as a partitioned dataset (partitioned by year)
    # This writes potentially multiple files within the year=YYYY directory but avoids collect()
    write_dataset(
      dataset = year_dataset,
      path = output_dir_year,
      format = "parquet",
      partitioning = "year" # Tell Arrow the structure uses year
    )
    message("  Data for year ", yr, " written to partitioned directory.")
    
  } else {
    warning("No chunks found or processed successfully to combine for year ", yr)
  }
  
  # Clean up temp directory for the year
  unlink(year_temp_dir, recursive = TRUE)
  
  message("Completed processing for year: ", yr)
  gc()  # Force garbage collection
}
# Clean up main temp directory
unlink(here("data", "processed_data", "national", "temp"), recursive = TRUE)


# -------------------------------
# STEP 2: IDENTIFY TREATMENT GROUPS (NATIONAL)
# -------------------------------
message("Starting Step 2: Identifying National Treatment Groups...")

# Point to the partitioned yearly beneficiary directory
processed_files_dir <- here("data", "processed_data", "national", "beneficiaries_yearly")
if (!dir.exists(processed_files_dir)) {
  stop("Beneficiary data directory not found: ", processed_files_dir)
}

# Open the partitioned dataset directly; Arrow will detect the 'year' partitions
ds <- open_dataset(processed_files_dir, partitioning = c("year"))
if (length(ds$files) == 0) {
  stop("No parquet files found under: ", processed_files_dir)
}

# Calculate baseline (2020) market shares for ALL municipalities
message("  Calculating baseline (2020) totals...")
baseline_market_shares <- ds %>%
  filter(year == 2020) %>% # This filter should work with the partition
  group_by(cd_municipio) %>%
  summarize(total_benef_munic = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect()

# Calculate insurer-level presence by municipality
message("  Calculating baseline (2020) insurer presence...")
insurer_presence <- ds %>%
  filter(year == 2020) %>%
  group_by(cd_municipio, cd_operadora) %>%
  summarize(insurer_benef = sum(qt_beneficiario_ativo, na.rm = TRUE)) %>%
  collect() %>%
  left_join(baseline_market_shares, by = "cd_municipio") %>%
  mutate(
    market_share = ifelse(total_benef_munic > 0, insurer_benef / total_benef_munic, 0),
    insurer_type = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE ~ "Other"
    ),
    has_presence = insurer_benef >= 50
  )

# Identify municipalities with significant presence of each insurer
message("  Classifying municipalities...")
municipality_treatment <- insurer_presence %>%
  filter(insurer_type %in% c("Hapvida", "GNDI")) %>%
  select(cd_municipio, insurer_type, has_presence) %>%
  pivot_wider(
    id_cols = cd_municipio,
    names_from = insurer_type,
    values_from = has_presence,
    values_fill = FALSE
  ) %>%
  # Ensure both Hapvida and GNDI columns exist
  { if (!"Hapvida" %in% names(.)) mutate(., Hapvida = FALSE) else . } %>%
  { if (!"GNDI" %in% names(.)) mutate(., GNDI = FALSE) else . } %>%
  mutate(
    treatment_status = case_when(
      # Removed SP capital exclusion: cd_municipio == 355030 ~ "Out of Sample",
      Hapvida & GNDI ~ "Treated",
      Hapvida | GNDI ~ "Control",
      TRUE ~ "Out of Sample"
    ),
    treated = case_when(
      treatment_status == "Treated" ~ 1,
      treatment_status == "Control" ~ 0,
      TRUE ~ NA_real_
    )
  )

# Save treatment assignments
output_treatment_file <- here("data", "processed_data", "national", "municipality_treatment.parquet")
write_parquet(municipality_treatment, output_treatment_file)
message("  Treatment assignments saved to: ", output_treatment_file)

# -------------------------------
# STEP 3: CALCULATE PLAN MARKET SHARES
# -------------------------------

message("Calculating plan market shares for all years...")

# Calculate municipality-level beneficiary totals by year
municipality_year_totals <- ds %>%
  group_by(cd_municipio, year) %>%
  summarize(total_benef_munic = sum(qt_beneficiario_ativo)) %>%
  collect()

# Calculate plan-level shares by municipality-year
plan_market_shares <- ds %>%
  group_by(cd_municipio, year, id_plano, cd_operadora) %>%
  summarize(plan_benef = sum(qt_beneficiario_ativo)) %>%
  collect() %>%
  left_join(municipality_year_totals, by = c("cd_municipio", "year")) %>%
  mutate(
    market_share = plan_benef / total_benef_munic,
    insurer = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE ~ "Other"
    ),
    # From 2021 onwards, mark plans as belonging to merged entity
    merged_insurer = case_when(
      year >= 2021 & insurer %in% c("Hapvida", "GNDI") ~ "HAP-GNDI-merged",
      TRUE ~ insurer
    )
  ) %>%
  # Join with treatment status
  left_join(
    select(municipality_treatment, cd_municipio, treatment_status, treated),
    by = "cd_municipio"
  )

# Save to parquet for later use
write_parquet(plan_market_shares, here("data", "processed_data", "plan_market_shares.parquet"))

# -------------------------------
# STEP 4: PROCESS READJUSTMENT DATA
# -------------------------------

message("Processing readjustment data...")

# Define the merger date
merger_date <- as.Date('2022-02-11')

# Path to readjustment parquet files
reajustes_dir <- here("data", "raw_data", "ANS", "operadoras", "reajustes", "reajustes_parquet")
temp_dir <- here("data", "processed_data", "temp", "reajustes")
dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)

# Get all parquet files
reajustes_files <- list.files(reajustes_dir, pattern = "\\.parquet$", full.names = TRUE) # More specific pattern

# Process each file separately
for (i in seq_along(reajustes_files)) {
  file_path <- reajustes_files[i]
  message("Processing file ", i, " of ", length(reajustes_files), ": ", basename(file_path))
  
  # Read this file
  reajustes_chunk <- read_parquet(file_path) %>%
    clean_names() # Apply clean_names first
  
  # --- Dynamically handle column name variations ---
  
  # 1. Standardize 'benef_comunicado'
  if ("qt_benef_comunicado" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% rename(benef_comunicado_std = qt_benef_comunicado)
  } else if ("benef_comunicado" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% rename(benef_comunicado_std = benef_comunicado)
  } else {
    warning("Beneficiary count column not found in: ", basename(file_path), ". Setting to NA.")
    reajustes_chunk <- reajustes_chunk %>% mutate(benef_comunicado_std = NA_integer_)
  }
  
  # 2. Standardize 'percentual'
  if ("pc_percentual" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% rename(percentual_std = pc_percentual)
  } else if ("percentual" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% rename(percentual_std = percentual)
  } else {
    warning("Percentual column not found in: ", basename(file_path), ". Setting to NA.")
    reajustes_chunk <- reajustes_chunk %>% mutate(percentual_std = NA_character_) # Keep as char for gsub
  }
  
  # 3. Check for other necessary columns and add NA if missing
  if (!"lg_retificacao" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% mutate(lg_retificacao = NA)
  }
  if (!"lg_negociacao" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% mutate(lg_negociacao = NA)
  }
  if (!"lg_parcelado" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% mutate(lg_parcelado = NA)
  }
  if (!"cd_agrupamento" %in% names(reajustes_chunk)) {
    reajustes_chunk <- reajustes_chunk %>% mutate(cd_agrupamento = NA) 
  }
  # --- End of dynamic handling ---
  
  # Continue processing using the standardized names
  reajustes_processed <- reajustes_chunk %>%
    mutate(
      percentual_numeric = as.numeric(gsub(",", ".", percentual_std)), # Apply gsub/numeric conversion
      year = year(dt_inic_aplicacao)
    ) %>%
    filter(
      cd_agrupamento == 0 | is.na(cd_agrupamento), # Allow NA if column was missing
      is.na(lg_negociacao) | lg_negociacao != 1,
      is.na(lg_parcelado) | lg_parcelado != 1,
      !is.na(id_plano)
    ) %>%
    # Select standardized and other necessary columns, renaming back
    select(
      id_contrato, id_plano, cd_operadora, dt_inic_aplicacao, # Removed cd_plano here
      benef_comunicado = benef_comunicado_std,
      percentual = percentual_numeric,
      year,
      lg_retificacao
    )
  
  # Handle corrections and multiple readjustments
  reajustes_cleaned <- reajustes_processed %>%
    group_by(id_contrato, id_plano, year) %>%
    filter(
      if (any(lg_retificacao == 1, na.rm = TRUE)) {
        lg_retificacao == 1 | is.na(lg_retificacao)
      } else {
        TRUE
      }
    ) %>%
    filter(n() == 1) %>%  # Keep only single observations per contract-plan-year
    ungroup()
  
  # Write to temp file
  if (nrow(reajustes_cleaned) > 0) { # Only write if there's data after filtering
    chunk_output <- file.path(temp_dir, paste0("reajustes_chunk_", sprintf("%03d", i), ".parquet"))
    write_parquet(reajustes_cleaned, chunk_output)
  } else {
    message("  No valid data after filtering for file: ", basename(file_path))
  }
  
  
  # Clean up
  rm(reajustes_chunk, reajustes_processed, reajustes_cleaned)
  gc()
}

# Combine all processed chunks
message("Combining readjustment chunks...")
reajustes_chunks_list <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)

if (length(reajustes_chunks_list) > 0) {
  reajustes_ds <- open_dataset(reajustes_chunks_list)
  output_filtered_dir <- here("data", "processed_data", "reajustes_filtered") # Use a directory for write_dataset
  if (dir.exists(output_filtered_dir)) unlink(output_filtered_dir, recursive = TRUE) # Remove if exists
  write_dataset(reajustes_ds, output_filtered_dir)
  message("  Combined filtered readjustments written to: ", output_filtered_dir)
} else {
  warning("No readjustment chunks found to combine.")
}


# Clean up temp directory
unlink(temp_dir, recursive = TRUE)

# Calculate plan-level averages from the combined dataset
message("Calculating plan-level readjustment averages...")

# Check if the output directory exists before opening
output_filtered_dir <- here("data", "processed_data", "reajustes_filtered") # Re-define path just in case

if (dir.exists(output_filtered_dir)) {
  # Open the dataset, select necessary columns, filter NAs needed for calculation, THEN collect
  plan_reajustes_data_for_avg <- open_dataset(output_filtered_dir) %>%
    select(id_plano, year, percentual, benef_comunicado) %>%
    # Filter necessary data - non-NA plan/year, and non-NA/positive values for weighted mean
    filter(!is.na(id_plano) & !is.na(year) & !is.na(percentual) & !is.na(benef_comunicado) & benef_comunicado > 0) %>%
    collect() # Pull the required data into R memory
  
  # Now perform the aggregation in R
  plan_reajustes_avg <- plan_reajustes_data_for_avg %>%
    group_by(id_plano, year) %>%
    summarize(
      # na.rm=TRUE might be redundant due to the filter, but good practice
      percentual_avg = weighted.mean(percentual, w = benef_comunicado, na.rm = TRUE),
      total_benef = sum(benef_comunicado, na.rm = TRUE),
      n_contracts = n(),
      .groups = "drop" # Drop grouping after summarize
    )
  
  # Save the result (which is now an R data frame)
  write_parquet(plan_reajustes_avg, here("data", "processed_data", "plan_reajustes_avg.parquet"))
  message("  Plan readjustment averages saved.")
  
  # Clean up the collected data frame if memory is tight
  rm(plan_reajustes_data_for_avg)
  gc()
  
} else {
  warning("Filtered readjustment dataset directory not found. Skipping average calculation.")
  plan_reajustes_avg <- data.frame() # Create empty df if no data
}


# -------------------------------
# STEP 5: CREATE MUNICIPALITY PANEL
# -------------------------------

message("Creating municipality panel...")

# Join market shares with readjustment data
municipality_panel <- plan_market_shares %>%
  left_join(
    select(plan_reajustes_avg, id_plano, year, percentual_avg),
    by = c("id_plano", "year")
  ) %>%
  # Calculate municipality-level weighted average readjustments
  group_by(cd_municipio, year) %>%
  summarize(
    readjustment = weighted.mean(percentual_avg, w = market_share, na.rm = TRUE),
    n_plans = n(),
    n_plans_with_readj = sum(!is.na(percentual_avg)),
    total_market_share = sum(market_share),
    coverage = sum(market_share[!is.na(percentual_avg)]),
    treatment_status = first(treatment_status),
    treated = first(treated),
    .groups = "drop"
  ) %>%
  # Remove São Paulo capital
  filter(cd_municipio != 355030) %>%
  # Add event study variables
  mutate(
    merger_year = 2022,
    event_time = year - merger_year,
    post = year >= merger_year
  )

# -------------------------------
# STEP 6: ADD MUNICIPALITY CHARACTERISTICS
# -------------------------------

message("Adding municipality characteristics...")

# Ensure municipality_panel exists and has data
if (exists("municipality_panel") && nrow(municipality_panel) > 0) {
  # Load municipality GDP data and convert key to numeric
  pib_municipios <- read_csv(here('data', 'raw_data', 'municipios', 'br_ibge_pib_municipio.csv')) %>%
    filter(ano == 2020) %>%
    mutate(
      # Extract 6-digit code as character
      id_municipio_char = substring(as.character(id_municipio), 1, 6),
      # Convert the 6-digit code to numeric for joining
      cd_municipio = as.numeric(id_municipio_char) 
    ) %>%
    select(cd_municipio, pib) 
  
  # Check the type after conversion
  message("Type of pib_municipios$cd_municipio: ", class(pib_municipios$cd_municipio))
  message("Type of municipality_panel$cd_municipio: ", class(municipality_panel$cd_municipio))
  
  # Add to panel (now types should match)
  municipality_panel_with_chars <- municipality_panel %>%
    # Ensure the panel's join key is also numeric (it should be already)
    mutate(cd_municipio = as.numeric(cd_municipio)) %>% 
    left_join(pib_municipios, by = "cd_municipio") %>%
    rename(
      municipality_code = cd_municipio,
      readjustment_rate = readjustment,
      total_plans = n_plans,
      plans_with_readjustment = n_plans_with_readj,
      market_share_coverage = coverage,
      municipality_gdp = pib
    )
} else {
  warning("Municipality panel is empty. Skipping adding characteristics.")
  municipality_panel_with_chars <- data.frame()
}


# -------------------------------
# STEP 7: CREATE BALANCED PANEL & EXPORT TO STATA
# -------------------------------

message("Creating balanced panel and exporting to Stata...")

# Check panel balance
panel_check <- municipality_panel_with_chars %>%
  group_by(municipality_code) %>%
  summarize(
    n_years = n_distinct(year),
    min_year = min(year),
    max_year = max(year),
    is_balanced = n_years == length(unique(municipality_panel_with_chars$year)),
    .groups = "drop"
  )

# Print balance summary
print(panel_check %>%
        group_by(is_balanced) %>%
        summarize(
          n_municipalities = n(),
          avg_years = mean(n_years)
        ))

# Export full (unbalanced) panel
write_dta(
  municipality_panel_with_chars,
  here("data", "processed_data", "municipality_panel_event_study.dta")
)


message("Data exports complete!")

# -------------------------------
# STEP 8: CREATE TREATMENT MAP (São Paulo Only)
# -------------------------------

message("Creating treatment status map for São Paulo...")

# Ensure municipality_treatment exists
if (exists("municipality_treatment") && nrow(municipality_treatment) > 0) {
  
  # Helper function
  trim_last_char <- function(column) {
    as.numeric(substr(as.character(column), 1, nchar(column) - 1))
  }
  
  # Read São Paulo municipality shapes
  sp_municipalities <- read_municipality(code_muni = 35, year = 2020) %>%
    mutate(code_muni = trim_last_char(code_muni)) # Ensure numeric code for joining
  
  # Prepare treatment data
  map_data <- select(municipality_treatment, cd_municipio, treatment_status) %>%
    mutate(cd_municipio = as.numeric(cd_municipio)) # Ensure numeric key for joining
  
  # Join treatment status with spatial data for São Paulo
  sp_municipalities_with_treatment <- sp_municipalities %>%
    left_join(map_data, by = c("code_muni" = "cd_municipio")) %>%
    mutate(
      # If treatment status is NA after join, it means it was "Out of Sample" originally
      treatment = ifelse(is.na(treatment_status), "Out of Sample", treatment_status)
    )
  
  # Create map focused on São Paulo
  treatment_map_sp <- ggplot(data = sp_municipalities_with_treatment) +
    # Base map layer for SP municipalities
    geom_sf(aes(fill = treatment), color = "white", size = 0.2) + # Add thin white borders
    # Color scheme (excluding "Other State")
    scale_fill_manual(
      values = c(
        "Treated" = "orange",
        "Control" = "darkcyan",
        "Out of Sample" = "lightgrey"
      ),
      name = "Treatment Status",
      # Ensure all levels are included in the legend even if some aren't present
      limits = c("Treated", "Control", "Out of Sample"),
      na.value = "grey80" # Color for any unexpected NAs
    ) +
    # Labels and title
    labs(
      title = "Treatment Status of São Paulo Municipalities",
      subtitle = "Based on Hapvida and GNDI Beneficiary Presence in 2020",
      caption = paste(
        "Data source: ANS & IBGE\n",
        "Note: Municipalities classified as treated if both insurers have ≥50 beneficiaries.\n",
        "São Paulo capital excluded from sample."
      )
    ) +
    # Theme customization
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 12),
      plot.caption = element_text(size = 8, hjust = 0),
      legend.position = "bottom",
      legend.title = element_text(size = 10),
      legend.text = element_text(size = 9),
      panel.grid = element_blank(),
      axis.text = element_blank(),
      axis.title = element_blank()
    )
  
  # Save map
  dir.create(here("data", "images"), showWarnings = FALSE, recursive = TRUE)
  ggsave(
    here("data", "images", "treatment_status_map_SP.png"), # Changed filename
    treatment_map_sp,
    width = 8, # Adjusted size for single state
    height = 7,
    dpi = 300
  )
  
  message("São Paulo treatment map saved.")
  
} else {
  warning("municipality_treatment data not available. Skipping map creation.")
}


# -------------------------------
# STEP 9: PRINT SUMMARY STATISTICS
# -------------------------------

message("Generating summary statistics...")

# Check if the panel with characteristics exists and has data
if (exists("municipality_panel_with_chars") && nrow(municipality_panel_with_chars) > 0) {
  
  # Use the data frame with the renamed columns
  summary_stats <- municipality_panel_with_chars %>% 
    # Filter out 'Out of Sample' before summarizing if desired
    filter(treatment_status %in% c("Treated", "Control")) %>% 
    group_by(treatment_status, year) %>%
    summarize(
      n_municipalities = n(),
      # Now use the correct column name from this data frame
      avg_readjustment = mean(readjustment_rate, na.rm = TRUE), 
      avg_n_plans = mean(total_plans, na.rm = TRUE),
      avg_n_plans_with_readj = mean(plans_with_readjustment, na.rm = TRUE), 
      .groups = "drop"
    )
  
  print(summary_stats)
  
} else {
  warning("Final municipality panel (with characteristics) is empty or does not exist. Skipping summary statistics.")
}

# -------------------------------
# NATIONAL ESTIMATION
# -------------------------------

# national_analysis.R

# -------------------------------
# 0.  LIBRARIES & GLOBAL DIRS
# -------------------------------

# Create all top–level output dirs
dir.create(here("data","processed_data","national"),       showWarnings = FALSE, recursive = TRUE)
dir.create(here("data","processed_data","national","temp"), showWarnings = FALSE, recursive = TRUE)
dir.create(here("data","processed_data","national","beneficiaries_yearly"),
           showWarnings = FALSE, recursive = TRUE)
dir.create(here("data","processed_data","national","reajustes_filtered"),
           showWarnings = FALSE, recursive = TRUE)
dir.create(here("data","images","national"),               showWarnings = FALSE, recursive = TRUE)

# -------------------------------
# 1.  LOAD PLAN CHARACTERISTICS
# -------------------------------
message("Loading plan characteristics…")
caracteristicas_planos <- read_csv2(
  here("data","raw_data","ANS","operadoras","planos",
       "caracteristicas_produtos_saude_suplementar.csv"),
  locale = locale(encoding="latin1")
) %>%
  clean_names() %>%
  filter(
    str_to_lower(contratacao) %in% c("coletivo empresarial","coletivo por adesão"),
    !str_detect(str_to_lower(sgmt_assistencial),"odontol[óo]gico")
  ) %>%
  rename(cd_operadora = registro_operadora) %>%
  mutate(cd_operadora = as.character(cd_operadora)) %>%
  group_by(cd_plano, cd_operadora) %>%
  slice_max(dt_situacao, with_ties = FALSE) %>%
  ungroup()

# -------------------------------
# 2.  PROCESS BENEFICIARY FILES
# -------------------------------
message("Step 2: Process beneficiaries data (2015–2024)…")
years <- 2015:2024

for (yr in years) {
  message("→ Year ", yr)
  year_dir <- here("data","raw_data","ANS","beneficiarios", as.character(yr))
  state_files <- list.files(
    path        = year_dir,
    pattern     = paste0("pda-024-icb-.*-",yr,"_12\\.csv$"),
    full.names  = TRUE
  )
  if (length(state_files)==0) {
    warning("  No files for ", yr); next
  }
  
  # create year‑specific temp
  year_temp_dir <- here("data","processed_data","national","temp",as.character(yr))
  dir.create(year_temp_dir, showWarnings = FALSE, recursive = TRUE)
  
  # process each state file in chunks
  for (file_path in state_files) {
    message("   • processing ", basename(file_path))
    cb <- DataFrameCallback$new(function(chunk, pos) {
      df <- chunk %>%
        clean_names() %>%
        rename(id_cmpt_movel = any_of(c("id_cmpt_movel","#id_cmpt_movel","number_id_cmpt_movel"))) %>%
        mutate(id_cmpt_movel = as.character(id_cmpt_movel)) %>%
        mutate(
          id_cmpt_movel = case_when(
            str_detect(id_cmpt_movel, "-") ~ id_cmpt_movel,
            TRUE ~ paste0(substr(id_cmpt_movel,1,4), "-", substr(id_cmpt_movel,5,6))
          ),
          dt_carga     = as.character(dt_carga),
          cd_operadora = as.character(cd_operadora),
          year         = yr
        ) %>%
        filter(
          str_to_lower(de_contratacao_plano) %in% c("coletivo empresarial","coletivo por adesão"),
          !str_detect(str_to_lower(de_segmentacao_plano),"odontol[óo]gico")
        ) %>%
        left_join(
          caracteristicas_planos %>% select(cd_plano,cd_operadora,id_plano),
          by = c("cd_plano","cd_operadora")
        ) %>%
        select(
          id_cmpt_movel, cd_operadora, nm_razao_social, nr_cnpj,
          modalidade_operadora, sg_uf, cd_municipio, tp_sexo,
          de_faixa_etaria, de_faixa_etaria_reaj, cd_plano,
          de_contratacao_plano, de_abrg_geografica_plano,
          qt_beneficiario_ativo, dt_carga, id_plano, year
        )
      
      if (nrow(df)>0) {
        fn <- file.path(year_temp_dir, paste0("chunk_",sprintf("%05d",pos),".parquet"))
        write_parquet(df, fn)
      }
      return(data.frame())
    })
    
    read_csv2_chunked(
      file      = file_path,
      callback  = cb,
      chunk_size= 100000,
      locale    = locale(encoding="latin1")
    )
  }
  
  # now combine & write as partitioned dataset
  message("  Combining & writing partition for ",yr)
  parts <- list.files(year_temp_dir,pattern="\\.parquet$",full.names=TRUE)
  if (length(parts)>0) {
    ds <- open_dataset(parts)
    write_dataset(
      dataset      = ds,
      path         = here("data","processed_data","national","beneficiaries_yearly"),
      format       = "parquet",
      partitioning = "year"
    )
  } else {
    warning("   no chunks to combine for ",yr)
  }
  unlink(year_temp_dir, recursive=TRUE)
  gc()
}

unlink(here("data","processed_data","national","temp"),recursive=TRUE)


# -------------------------------
# 3.  IDENTIFY TREATMENT GROUPS
# -------------------------------
message("Step 3: Identify treatment groups…")
ds <- open_dataset(
  here("data","processed_data","national","beneficiaries_yearly"),
  partitioning = "year"
)
baseline <- ds %>%
  filter(year==2020) %>%
  group_by(cd_municipio) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo,na.rm=TRUE)) %>%
  collect()

ins_pres <- ds %>%
  filter(year==2020) %>%
  group_by(cd_municipio,cd_operadora) %>%
  summarise(insurer_benef = sum(qt_beneficiario_ativo,na.rm=TRUE)) %>%
  collect() %>%
  left_join(baseline, by="cd_municipio") %>%
  mutate(
    market_share  = ifelse(total_benef>0, insurer_benef/total_benef, 0),
    insurer_type  = case_when(
      cd_operadora=="368253" ~ "Hapvida",
      cd_operadora=="359017" ~ "GNDI",
      TRUE                  ~ "Other"
    ),
    has_presence  = insurer_benef>=50
  )

municipality_treatment <- ins_pres %>%
  filter(insurer_type %in% c("Hapvida","GNDI")) %>%
  select(cd_municipio,insurer_type,has_presence) %>%
  pivot_wider(
    id_cols     = cd_municipio,
    names_from  = insurer_type,
    values_from = has_presence,
    values_fill = FALSE
  ) %>%
  mutate(
    Hapvida = coalesce(Hapvida,FALSE),
    GNDI    = coalesce(GNDI,   FALSE),
    treatment_status = case_when(
      Hapvida & GNDI ~ "Treated",
      Hapvida | GNDI ~ "Control",
      TRUE           ~ "Out of Sample"
    ),
    treated = as.numeric(treatment_status=="Treated")
  )

write_parquet(
  municipality_treatment,
  here("data","processed_data","national","municipality_treatment.parquet")
)


# -------------------------------
# 4.  PLAN MARKET SHARES
# -------------------------------
message("Step 4: Compute plan market shares…")
by_mun_year <- ds %>%
  group_by(cd_municipio,year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo,na.rm=TRUE)) %>%
  collect()

plan_market_shares <- ds %>%
  filter(!is.na(id_plano)) %>%
  group_by(cd_municipio,year,id_plano,cd_operadora) %>%
  summarise(plan_benef = sum(qt_beneficiario_ativo,na.rm=TRUE)) %>%
  collect() %>%
  left_join(by_mun_year, by=c("cd_municipio","year")) %>%
  mutate(
    market_share = ifelse(total_benef>0, plan_benef/total_benef, 0),
    insurer      = case_when(
      cd_operadora=="368253" ~ "Hapvida",
      cd_operadora=="359017" ~ "GNDI",
      TRUE                   ~ "Other"
    ),
    merged_insurer = ifelse(year>=2021 & insurer %in% c("Hapvida","GNDI"),
                            "HAP‑GNDI‑merged", insurer)
  ) %>%
  left_join(
    municipality_treatment %>% select(cd_municipio,treatment_status,treated),
    by = "cd_municipio"
  )

write_parquet(
  plan_market_shares,
  here("data","processed_data","national","plan_market_shares.parquet")
)

# -------------------------------
# 5.  READJUSTMENT DATA
# -------------------------------
message("Step 5: Process readjustments…")
reaj_dir  <- here("data","raw_data","ANS","operadoras","reajustes","reajustes_parquet")
r_files   <- list.files(reaj_dir, pattern="\\.parquet$", full.names=TRUE)

# 5a) Compute the full union of *all* column names (cleaned) across every file:
all_unique_cols <- sort(unique(unlist(lapply(r_files, function(path) {
  tbl <- read_parquet(path, as_data_frame = FALSE)
  clean_names(as.data.frame(tbl)[0, ]) %>% names()
}))))

temp_r    <- here("data","processed_data","national","temp","reajustes")
dir.create(temp_r, recursive=TRUE, showWarnings=FALSE)

chunk_out <- vector()
n_files <- length(r_files)

for (i in seq_along(r_files)) {
  path <- r_files[i]
  message(sprintf("Processing file %3d of %3d: %s", i, n_files, basename(path)))
  
  df <- tryCatch(
    read_parquet(path) %>% clean_names(),
    error = function(e) {
      message("  ↳ failed to read, skipping: ", basename(path))
      NULL
    }
  )
  if (is.null(df)) next
  
  # 5b) Ensure every column in all_unique_cols exists:
  missing <- setdiff(all_unique_cols, names(df))
  for (col in missing) df[[col]] <- NA
  
  # 5c) Explicitly rename or create the two “std” columns:
  if ("benef_comunicado" %in% names(df)) {
    df <- rename(df, benef_comunicado_std = benef_comunicado)
  } else if ("qt_benef_comunicado" %in% names(df)) {
    df <- rename(df, benef_comunicado_std = qt_benef_comunicado)
  } else {
    df$benef_comunicado_std <- NA_integer_
  }
  
  if ("percentual" %in% names(df)) {
    df <- rename(df, percentual_std = percentual)
  } else if ("pc_percentual" %in% names(df)) {
    df <- rename(df, percentual_std = pc_percentual)
  } else {
    df$percentual_std <- NA_real_
  }
  
  # 5d) Type‑casts, coalesces & filters:
  df <- df %>%
    mutate(
      benef_comunicado = as.integer(benef_comunicado_std),
      percentual       = as.numeric(gsub(",", ".", percentual_std)),
      year             = year(dt_inic_aplicacao),
      lg_retificacao   = coalesce(lg_retificacao,   NA_integer_),
      lg_negociacao    = coalesce(lg_negociacao,    NA_integer_),
      lg_parcelado     = coalesce(lg_parcelado,     NA_integer_),
      cd_agrupamento   = coalesce(cd_agrupamento,   NA_integer_)
    ) %>%
    filter(
      (cd_agrupamento == 0 | is.na(cd_agrupamento)),
      is.na(lg_negociacao) | lg_negociacao != 1,
      is.na(lg_parcelado)  | lg_parcelado  != 1,
      !is.na(id_plano)
    ) %>%
    group_by(id_contrato, id_plano, year) %>%
    filter(
      if (any(lg_retificacao == 1, na.rm = TRUE))
        lg_retificacao == 1 | is.na(lg_retificacao)
      else
        TRUE
    ) %>%
    filter(n() == 1) %>%
    ungroup()
  
  if (nrow(df) > 0) {
    out <- file.path(temp_r, sprintf("rchunk_%03d.parquet", i))
    write_parquet(df, out)
    chunk_out <- c(chunk_out, out)
    message("  ↳ wrote chunk (", nrow(df), " rows)")
  } else {
    message("  ↳ no rows after filtering, skipping write")
  }
}


if (length(chunk_out) > 0) {
  message("Combining filtered reajuste chunks…")
  dsr <- open_dataset(chunk_out) 
  write_dataset(
    dataset      = dsr,
    path         = here("data","processed_data","national","reajustes_filtered"),
    format       = "parquet"
  )
  message("Filtered reajustes written to data/processed_data/national/reajustes_filtered")
  
  # now it’s safe to delete the temp files:
  unlink(temp_r, recursive = TRUE)
  message("Cleaned up temp directory")
} else {
  message("No filtered chunks to combine — temp directory retained for inspection")
}




# -------------------------------
# 6.  MUNICIPALITY PANEL
# -------------------------------
message("Step 6: Build municipality panel…")
mp <- plan_market_shares %>%
  left_join(
    plan_reajustes_avg,
    by = c("id_plano","year")
  ) %>%
  group_by(cd_municipio,year) %>%
  summarise(
    readjustment      = weighted.mean(percentual_avg,w=market_share,na.rm=TRUE),
    n_plans           = n(),
    n_plans_with_readj= sum(!is.na(percentual_avg)),
    coverage          = sum(market_share[!is.na(percentual_avg)],na.rm=TRUE),
    treatment_status  = first(treatment_status),
    treated           = first(treated),
    .groups="drop"
  ) %>%
  mutate(
    merger_year = 2022,
    event_time  = year - merger_year,
    post        = year >= merger_year
  )

# attach GDP
pib <- read_csv(here("data","raw_data","municipios","br_ibge_pib_municipio.csv")) %>%
  clean_names() %>%
  filter(ano==2020) %>%
  mutate(cd_municipio=as.numeric(substr(id_municipio,1,6))) %>%
  select(cd_municipio,pib)

mp <- mp %>%
  left_join(pib,by="cd_municipio") %>%
  rename(municipality_gdp=pib)

# Export to Stata
message("Step 7: Export panels to Stata…")
write_dta(
  mp,
  here("data","processed_data","national","municipality_panel_event_study_unbalanced.dta")
)
balanced_ids <- mp %>%
  count(cd_municipio) %>%
  filter(n==length(unique(mp$year))) %>%
  pull(cd_municipio)

write_dta(
  filter(mp,cd_municipio %in% balanced_ids),
  here("data","processed_data","national","municipality_panel_event_study_balanced.dta")
)


# -------------------------------
# 8.  NATIONAL TREATMENT MAP
# -------------------------------
message("Step 8: Plot national treatment map…")
br_munis  <- read_municipality(code_muni="all",year=2020) %>%
  mutate(cd_municipio = as.numeric(substr(code_muni,1,nchar(code_muni)-1)))

map_data  <- municipality_treatment %>%
  mutate(cd_municipio=as.numeric(cd_municipio))

br_map    <- left_join(br_munis,map_data,by=c("cd_municipio"))

treatment_map_br <- ggplot(br_map) +
  geom_sf(aes(fill=treatment_status), color=NA, size=0.01) +
  geom_sf(data=read_state(year=2020), fill=NA, color="white", size=0.3) +
  scale_fill_manual(
    values=c("Treated"="orange","Control"="darkcyan","Out of Sample"="lightgrey"),
    name="Treatment"
  ) +
  labs(
    title    = "Treatment Status – Brazil",
    subtitle = "(Hapvida & GNDI ≥50 beneficiaries in 2020)"
  ) +
  theme_void() +
  theme(legend.position="bottom")

ggsave(
  here("data","images","national","treatment_status_map_BR.png"),
  treatment_map_br, width=8, height=8, dpi=300
)


# -------------------------------
# 9.  SUMMARY STATS
# -------------------------------
message("Step 9: Summary statistics…")
mp %>%
  filter(treatment_status %in% c("Treated","Control")) %>%
  group_by(treatment_status,year) %>%
  summarise(
    n_municipalities       = n(),
    avg_readjustment_rate  = mean(readjustment,na.rm=TRUE),
    avg_plans              = mean(n_plans,na.rm=TRUE),
    avg_plans_with_readj   = mean(n_plans_with_readj,na.rm=TRUE)
  ) %>%
  print()

message("All done — national pipeline complete!")


# 1) For each file, grab its cleaned column names
cols_list <- lapply(r_files, function(path) {
  # read only schema for speed
  tbl <- read_parquet(path, as_data_frame = FALSE)
  clean_names(as.data.frame(tbl)[0, ]) %>% names()
})

# 2) Compute the set of *all* unique column names across every file
all_unique_cols <- sort(unique(unlist(cols_list)))

# 3) Print them out
cat("All unique column names across your Parquet files:\n")
cat(paste0(" - ", all_unique_cols), sep = "\n")

aux <- read_parquet(here('data', 'processed_data', 'national', 'beneficiarios_filtered_2015_2024.parquet'))
aux %>% glimpse()

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
# EXPLORAÇÃO REDE D'OR
# -------------------------------

cnpj_dor <- '06047087'

dor_hospitais <- hospitais_planos %>%
  filter(str_detect(cd_cnpj_estb_saude, cnpj_dor)) %>%
  distinct(cd_cnes, .keep_all = TRUE)

dor_munics <- dor_hospitais %>% distinct(cd_municipio)
dor_munics

# Load São Paulo municipalities (SP state has code 35)
sp_municipalities <- read_municipality(code_muni = 35, year = 2020)

# Ensure the municipality codes are numeric
sp_municipalities <- sp_municipalities %>%
  mutate(code_muni = as.numeric(substr(code_muni, 1, nchar(code_muni) - 1)))

# Create a mapping DataFrame for classification
munics_map <- dor_munics %>%
  mutate(group = "DOR Municipalities")

# Merge with the spatial dataset
sp_municipalities <- sp_municipalities %>%
  left_join(munics_map, by = c("code_muni" = "cd_municipio"))

# Assign a group label: Highlighted municipalities vs. others
sp_municipalities <- sp_municipalities %>%
  mutate(group = ifelse(is.na(group), "Out of Sample", group))

# Plot the map
ggplot(data = sp_municipalities) +
  geom_sf(aes(fill = group), color = "white", size = 0.2) +
  scale_fill_manual(
    values = c("DOR Municipalities" = "orange", "Out of Sample" = "gray"),
    name = "Group"
  ) +
  labs(title = "Municipalities Highlighted for 'DOR'",
       caption = "Data source: IBGE") +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    panel.grid = element_blank()
  )

library(tidyverse)
library(here)
library(readxl)
library(janitor)
library(VennDiagram)

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

reajustes_202305 <- read_csv2(here('data', 
                                   'raw_data',
                                   'ANS',
                                   'operadoras',
                                   'reajustes',
                                   'PDA_RPC_202305.csv')) %>%
  clean_names()


reajustes_202305 <- reajustes_202305 %>%
  filter(sg_uf_contrato_reaj == 'SP',
         cd_agrupamento == 0)


# O cd_agrupamento == 0 tira planos empresariais com menos de 30 vidas, cujos 
# reajustes são negociados de forma agrupada por operadora.

hospitais_planos <- hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar')

# -------------------------------
# EXPLORAÇÃO
# -------------------------------

# Dúvida: planos HapVida dão acesso a hospitais não-hapvida?

single_insurer_hospitals <- hospitais_planos %>%
  group_by(cd_cnes) %>%
  summarize(operadoras = n_distinct(cd_operadora)) %>%
  arrange(desc(operadoras)) %>%
  filter(operadoras == 1)

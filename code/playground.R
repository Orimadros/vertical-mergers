library(tidyverse)
library(here)
library(readxl)
library(janitor)
library(VennDiagram)

# usamos a base de operadoras ativas para conseguir o registro ANS a partir do
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
# usamos a base de informações consolidadas dos beneficiariosiciários para identificar
# o grupo de cidades com pelo menos um beneficiariosiciário para cada operadora

beneficiarios_cons <- read_csv2(here('data', 
                             'raw_data',
                             'ANS',
                             'beneficiarios',
                             'ben202301_DF.csv')) %>%
  clean_names()

beneficiarios_cons <- beneficiarios_cons %>%
  filter(de_contratacao_plano == 'COLETIVO EMPRESARIAL')

exemplo_cnpj <- 84313741000112

cd_munic_map <- read_excel(here('data', 
                             'raw_data',
                             'ANS',
                             'misc',
                             'codigos_municipios_ibge.xlsx')) %>%
  clean_names() %>%
  select(uf, nome_uf, c_udigo_munic_ipio_completo, nome_munic_ipio) %>%
  rename(cd_munic = c_udigo_munic_ipio_completo,
         nome_munic = nome_munic_ipio) %>%
  mutate(cd_munic = str_sub(cd_munic, 1, -2))

cnpj_to_geo <- function(cnpj_input) {
  
  # Get the registro ANS for the input CNPJ
  registro_ans <- operadoras %>%
    filter(cnpj == cnpj_input) %>%
    select(registro_ans) %>%
    pull()
  
  # Check if the CNPJ exists in the operadoras table
  if (length(registro_ans) == 0) {
    stop("CNPJ not found in the operadoras table.")
  }
  
  # Get the geography (list of unique municipality codes) for the given registro ANS
  geografia <- beneficiarios_cons %>%
    filter(cd_operadora == registro_ans) %>%
    distinct(cd_municipio) %>%
    pull(cd_municipio) %>%
    as.character()
  
  # Return the geography as a list
  return(geografia)
}

cnpj_to_geo(exemplo_cnpj)

# usamos a base de prestadores para conseguir a localidade de um hospital a 
# partir de seu CNPJ

hospitais_planos <- read_csv2(here('data', 
                            'raw_data',
                            'ANS',
                            'prestadores',
                            'produtos e prestadores hospitalares',
                            'produtos_prestadores_hospitalares_DF.csv')) %>%
  clean_names()

hospitais_planos <- hospitais_planos %>%
  filter(
    de_tipo_contratacao == 'COLETIVO EMPRESARIAL',
    de_clas_estb_saude == 'Assistencia Hospitalar'
  )

# usamos a base de reajustes de planos coletivos para obter nossa variável
# dependente

reajustes <- read_csv2(here('data', 
                            'raw_data',
                            'ANS',
                            'operadoras',
                            'reajustes',
                            'PDA_RPC_202305.csv')) %>%
  clean_names()

reajustes <- reajustes %>%
  filter(sg_uf_contrato_reaj == 'DF')

hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar') %>%
  count(de_tipo_abrangencia_geografica)

hospitais_planos %>%
  group_by(id_plano) %>%
  summarize(hosps = n_distinct(id_estabelecimento_saude)) %>%
  arrange(desc(hosps))


# Um plano é tratado se a operadora que o oferece fundiu com um provedor
# localizado na cidade em que ele atua.

# os controles podem ser planos cuja operadora 

# Ou seja: filtrar apenas planos cujos beneficiários estão concentrados em 
# uma única cidade.

municipally_bound_plans <- beneficiarios_cons %>%
  group_by(cd_plano) %>%
  summarize(cidades = n_distinct(cd_municipio)) %>%
  arrange(desc(cidades)) %>%
  filter(cidades == 1) %>%
  pull(cd_plano)
municipally_bound_plans

# No caso do DF todos aparecem como municipally bound. Tem que ver se não tem
# um pessoal em Goiás que contrata esses planos.

# A df usada para estimar o DiD deve ter as colunas:
# id do plano, município, ano, tratamento, controles, reajuste

# Há reajustes diferentes para contratos diferentes de um mesmo plano!
# O que significa isso? O que são esses contratos?

# Como identificar planos empresariais grandes (>30) nos dados? Basta filtrar
# Por planos com mais de 30 beneficiários?

reajustes <- reajustes %>%
  filter(cd_plano %in% municipally_bound_plans)

# Começamos escolhendo uma fusão para ver se conseguimos construir a df do DiD
# Montante - Hospital Regional de Franca / CNES 2081601
# Jusante: - São Francisco Sistemas de Saúde

# Surgiram dúvidas e problemas com essa fusão:

# Dúvida: Por quê tem hospitais com o mesmo CNES (em tese o mesmo hospital),
# mas com nomes diferentes? Ex.: CNES 2081601

# Dúvida: Um plano pode ter sido tratado (integrado verticalmente) mais 
# de uma vez. Por exemplo, o Hospital Regional de Franca foi adquirido pela
# São Francisco, que em seguida foi adquirida pela HapVida. O que acontece com
# o plano da São Francisco nesse caso? Ele deixa de existir, e os dados acabam
# no ano da fusão com a HapVide? Ou ele continua vivo e com o mesmo cd_plano,
# mas agora aparece como da HapVida?

# Dúvida: muitas fusões são tanto horizontais quanto verticais. Isso traz um
# confundidor à análise. O que fazer? Será que não é melhor pegar uma fusão
# entre uma operadora pura grande e um sistema hospitalar puro grande?

# Dúvida: como fazer quando o player a montante tem mais de um hospital? Acho 
# que é deboa, faz o mesmo processo pra cada um dos hospitais (vê, na cidade
# que ele tá, quais planos são da operadora que fundiu)

# Descoberta: um mesmo plano pode ter reajustes diferentes em diferentes
# contratos.

# A base de redes (produtos e prestadores hospitalares) só existe para 2024? Se
# sim, temos um problema. Digamos que o hospital H fundiu com a operadora O1 em
# 2016. Em 2020, a operadora O1 foi adquirida pela operadora O2. Quando formos
# definir as unidades tratadas, procuraremos aquelas cuja operadora fundiu com
# um hospital na sua cidade. Mas se usarmos a base de redes de 2024,
# consideraremos como tratados em 2017-2020 que eram da O2, e portanto eram
# competidores dos planos da O1 que se integraram verticalmente. Precisamos
# conseguir identificar quais planos na base de reajustes eram da O1 em 
# 2017-2020. Aparentemente é possível: vide cd_operadora 302091 da São
# Francisco. Mas precisamos conseguir identificar o cd dessas operadoras.

# Assim, tentamos encontrar uma fusão mais simples para conseguir montar uma
# metodologia preliminar.

# Montante: Viventi Hospital Asa Sul / CNES 0797146
# Jusante: Hapvida / cd_operadora 368253

# vamos identificar os planos tratados.

treated <- hospitais_planos %>%
  filter(cd_cnes == '0797146',
         cd_operadora == '368253') %>%
  pull(cd_plano)

same_insurer_untreated <- hospitais_planos %>%
  filter(cd_operadora == '368253',
         !cd_plano %in% tratados) %>%
  distinct(cd_plano) %>%
  pull()

same_hospital_untreated <- hospitais_planos %>%
  filter(cd_cnes == '0797146',
         cd_operadora != '368253') %>%
  distinct(cd_plano) %>%
  pull()

sample <- c(treated, same_hospital_untreated, same_insurer_untreated)

df_did <- reajustes %>%
  filter(cd_plano %in% sample) %>%
  distinct(cd_plano)

same_hospital_untreated
hospitais_planos %>%
  distinct(de_porte)

hospitais_planos %>%
  filter(cd_plano %in% municipally_bound_plans,
         cd_plano %in% (reajustes %>% distinct(cd_plano) %>% pull())) %>%
  distinct(cd_plano)

set1 <- unique(hospitais_planos$cd_plano)
set2 <- unique(beneficiarios_cons$cd_plano)
set3 <- unique(reajustes$cd_plano)
venn.plot <- draw.triple.venn(
  area1 = length(set1),
  area2 = length(set2),
  area3 = length(set3),
  n12 = length(intersect(set1, set2)),
  n13 = length(intersect(set1, set3)),
  n23 = length(intersect(set2, set3)),
  n123 = length(intersect(intersect(set1, set2), set3)),
  category = c("Hospitais Planos", "Beneficiários Cons", "Reajustes"),
  lty = "dashed",
  cex = 1.5,
  cat.cex = 1.5,
)

grid.draw(venn.plot)

# Onde paramos: tentamos construir a df_did para a fusão exemplo. Conseguimos
# identificar todos os planos tratados e controle, mas encontramos dados de
# reajuste para apenas 11 deles. Por que?

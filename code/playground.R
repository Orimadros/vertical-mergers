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
                             'ben202305_DF.csv')) %>%
  clean_names()

beneficiarios_cons_202301 <- read_csv2(here('data', 
                                            'raw_data',
                                            'ANS',
                                            'beneficiarios',
                                            'ben202301_DF.csv')) %>%
  clean_names()

beneficiarios_cons_202305 <- beneficiarios_cons_202305 %>%
  filter(de_contratacao_plano == 'COLETIVO EMPRESARIAL')

# Usamos a base de relações de credenciamento para identificar as redes

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

# Usamos a base de reajustes_202305 de planos coletivos para obter nossa
# variável dependente

reajustes_202305 <- read_csv2(here('data', 
                            'raw_data',
                            'ANS',
                            'operadoras',
                            'reajustes',
                            'PDA_RPC_202305.csv')) %>%
  clean_names()

reajustes_202304 <- read_csv2(here('data', 
                                   'raw_data',
                                   'ANS',
                                   'operadoras',
                                   'reajustes',
                                   'PDA_RPC_202304.csv')) %>%
  clean_names()

reajustes_202205 <- read_csv2(here('data', 
                                   'raw_data',
                                   'ANS',
                                   'operadoras',
                                   'reajustes',
                                   'PDA_RPC_202205.csv')) %>%
  clean_names()

reajustes_202205 <- reajustes_202305 %>%
  filter(sg_uf_contrato_reaj == 'DF',
         cd_agrupamento == 0)

reajustes_202305 <- reajustes_202305 %>%
  filter(sg_uf_contrato_reaj == 'DF',
         cd_agrupamento == 0)

reajustes_202304 <- reajustes_202304 %>%
  filter(sg_uf_contrato_reaj == 'DF',
         cd_agrupamento == 0)

# O cd_agrupamento == 0 tira planos empresariais com menos de 30 vidas, cujos 
# reajustes são negociados de forma agrupada por operadora.

hospitais_planos <- hospitais_planos %>%
  filter(de_clas_estb_saude == 'Assistencia Hospitalar')

# -------------------------------
# EXPLORAÇÃO
# -------------------------------

# Qual o tamanho da rede credenciada dos planos?
hospitais_planos %>%
  group_by(id_plano) %>%
  summarize(hosps = n_distinct(id_estabelecimento_saude)) %>%
  arrange(desc(hosps))


# Um plano é tratado se a operadora que o oferece fundiu com um provedor
# localizado na cidade em que ele atua.


# Ou seja: filtrar apenas planos cujos beneficiários estão concentrados em 
# uma única cidade.

municipally_bound_plans_05 <- beneficiarios_cons_202305 %>%
  group_by(cd_plano) %>%
  summarize(cidades = n_distinct(cd_municipio)) %>%
  arrange(desc(cidades)) %>%
  filter(cidades == 1) %>%
  pull(cd_plano)

municipally_bound_plans_01 <- beneficiarios_cons_202301 %>%
  filter(cd_plano %in% municipally_bound_plans_05) %>%
  group_by(cd_plano) %>%
  summarize(cidades = n_distinct(cd_municipio)) %>%
  arrange(desc(cidades)) %>%
  filter(cidades == 1) %>%
  pull(cd_plano)

# No caso do DF todos aparecem como municipally bound. Tem que ver se não tem
# um pessoal em Goiás que contrata esses planos.

# A df usada para estimar o DiD deve ter as colunas:
# id do plano, município, ano, tratamento, controles, reajuste

# Há reajustes_202305 diferentes para contratos diferentes de um mesmo plano!
# O que significa isso? O que são esses contratos?

# Como identificar planos empresariais grandes (>30) nos dados? Basta filtrar
# Por planos com mais de 30 beneficiários? Descobri: tem uma coluna
# cd_agrupamento para isso

reajustes_202305 <- reajustes_202305 %>%
  filter(cd_plano %in% municipally_bound_plans_05)

reajustes_202304 <- reajustes_202304 %>%
  filter(cd_plano %in% municipally_bound_plans_05)

reajustes_202205 <- reajustes_202205 %>%
  filter(cd_plano %in% municipally_bound_plans_05)

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
# definir os planos tratados, procuraremos aqueles cuja operadora fundiu com
# um hospital na sua cidade. Mas se usarmos a base de redes de 2024,
# consideraremos como tratados em 2017-2020 planos que eram da O2, e portanto
# eram competidores dos planos da O1 que se integraram verticalmente. Precisamos
# conseguir identificar quais planos na base de reajustes_202305 eram da O1 em 
# 2017-2020. Aparentemente é possível: vide cd_operadora 302091 da São
# Francisco. Mas precisamos conseguir identificar o cd dessas operadoras.

# Assim, tentamos encontrar uma fusão mais simples para conseguir montar uma
# metodologia preliminar.

# Montante: Viventi Hospital Asa Sul / CNES 0797146
# Jusante: Hapvida / cd_operadora 368253

# vamos identificar os planos tratados.

# -------------------------
# THIS IS USING THE WRONG DEFINITION OF TREATED PLANS
#
# treated_plans <- hospitais_planos %>%
#   filter(cd_cnes == '0797146',
#          cd_operadora == '368253') %>%
#   pull(cd_plano)
# 
# same_insurer_untreated_plans <- hospitais_planos %>%
#   filter(cd_operadora == '368253',
#          !cd_plano %in% treated_plans) %>%
#   distinct(cd_plano) %>%
#   pull()
# 
# same_hospital_untreated_plans <- hospitais_planos %>%
#   filter(cd_cnes == '0797146',
#          cd_operadora != '368253') %>%
#   distinct(cd_plano) %>%
#   pull()
# -------------------------

df_did_treated_attempt <- reajustes_202305 %>%
  filter(cd_plano %in% treated_plans)

df_did_untreated_attempt <- reajustes_202305 %>%
  filter(cd_plano %in% c(same_hospital_untreated_plans, same_insurer_untreated_plans))

set1 <- unique(hospitais_planos$cd_plano)
set2 <- unique(beneficiarios_cons_202305$cd_plano)
set3 <- unique(reajustes_202305$cd_plano)
venn.plot <- draw.triple.venn(
  area1 = length(set1),
  area2 = length(set2),
  area3 = length(set3),
  n12 = length(intersect(set1, set2)),
  n13 = length(intersect(set1, set3)),
  n23 = length(intersect(set2, set3)),
  n123 = length(intersect(intersect(set1, set2), set3)),
  category = c("Hospitais Planos", "Beneficiários Cons", "reajustes_202305"),
  lty = "dashed",
  cex = 1.5,
  cat.cex = 1.5,
)

grid.draw(venn.plot)

reajustes_202305 %>%
  distinct(cd_plano)

# Onde paramos: tentamos construir a df_did para a fusão exemplo. Conseguimos
# identificar todos os planos tratados e controle, mas encontramos dados de
# reajuste para contratos de apenas 7 deles. Por que?

# Burro! B!!!! Cada contrato só pode ser reajustado uma vez ao ano. Portanto,
# a base de junho provavelmente só contem ~1/12 dos contratos, e provavelmente 
# uma parcela maior dos planos, já que um plano pode aparecer em meses
# consecutivos por contratos diferentes. Teste disso:

reajustes_202304 %>%
  filter(id_contrato %in% (reajustes_202305 %>% pull(id_contrato))) %>%
  distinct(id_contrato)

# Extract unique values from the 'cd_plano' column in each data frame
set_reajustes_202304 <- unique(reajustes_202304$id_contrato)
set_reajustes_202305 <- unique(reajustes_202305$id_contrato)

# Create the Venn diagram
venn_04_05 <- draw.pairwise.venn(
  area1 = length(set_reajustes_202304),
  area2 = length(set_reajustes_202305),
  cross.area = length(intersect(set_reajustes_202304, set_reajustes_202305)),
  category = c("Reajustes 202304", "Reajustes 202305"),
  lty = "dashed",
  alpha = 0.5,
  cex = 1.5,
  cat.cex = 1.5
)

# Tem três malditos contratos que são reajustados em meses consecutivos.
# Provavelmente eu tava certo, e esses são exceções.

# Em tese, cada contrato só deve ser reajustado no seu mês de aniversário.
# Portanto, salvo contratos novos ou desligados, os contratos da base RPC de
# maio de 2023 devem ser os mesmos de maio de 2022. Teste disso:

# Plot the diagram
grid.draw(venn_04_05)

# Extract unique values from the 'cd_plano' column in each data frame
set_reajustes_202205 <- unique(reajustes_202205$id_contrato)
set_reajustes_202305 <- unique(reajustes_202305$id_contrato)

# Create the Venn diagram
venn_22_23 <- draw.pairwise.venn(
  area1 = length(set_reajustes_202205),
  area2 = length(set_reajustes_202305),
  cross.area = length(intersect(set_reajustes_202205, set_reajustes_202305)),
  category = c("Reajustes 202205", "Reajustes 202305"),
  lty = "dashed",
  alpha = 0.5,
  cex = 1.5,
  cat.cex = 1.5
)

# Plot the diagram
grid.draw(venn_22_23)

# Certo de novo.

# Dúvida: como pode ter um contrato com dois planos diferentes? 
# Ex.: BF02FE1EB41589CE09FC074D5DEF48AEACFAD67245436BF93EE1B2A48D9D8D4A

# Dúvida: por que os dados da operadora 301949 não aparecem em hospitais_planos,
# mas aparecem nos reajustes?

# Agora chega de explorar. Vamos pros finalmentes

# -------------------------------
# MONTAGEM DA df_dif
# -------------------------------

# ATENÇÃO. Vou começar cometendo uma heresia: vou eliminar todas as duplicatas
# de contratos. Cada contrato só poderá ter uma linha. Depois que eu descobrir
# por que diabos um contrato pode ter duas linhas, resolvo isso.

# Deduplicate reajustes_202305
reajustes_202305_unique <- reajustes_202305 %>%
  distinct(id_contrato, .keep_all = TRUE)

# Deduplicate reajustes_202205
reajustes_202205_unique <- reajustes_202205 %>%
  distinct(id_contrato, .keep_all = TRUE)

reajustes_consolidado <- reajustes_202305_unique %>%
  left_join(
    reajustes_202205_unique,
    by = 'id_contrato',
    relationship = "many-to-many"
    )

reajustes_consolidado <- reajustes_consolidado %>%
  rename(
    cd_plano = cd_plano.x, cd_operadora = cd_operadora.x,
    reaj_2023 = percentual.x, reaj_2022 = percentual.y,
    sg_uf = sg_uf_contrato_reaj.x
    ) %>%
  select(
    cd_plano, id_contrato, cd_operadora, reaj_2022, 
    reaj_2023, sg_uf, -cd_plano.y
    )

df_did_treated <- reajustes_consolidado %>%
  filter(cd_plano %in% treated_plans) %>%
  mutate(treated = 1)

df_did_untreated <- reajustes_consolidado %>%
  filter(
    cd_plano %in% same_hospital_untreated_plans |
    cd_plano %in% same_insurer_untreated_plans
    ) %>%
  mutate(treated = 0)

df_did <- rbind(df_did_treated, df_did_untreated)

reajustes_202205 %>%
  group_by(cd_plano) %>%
  summarize(contratos = n_distinct(id_contrato)) %>%
  arrange(desc(contratos))

# ── 0. Packages ──────────────────────────────────────────────────────────────
library(tidyverse)
library(arrow)       # for parquet reading/writing
library(here)
library(janitor)

# ── 1. Build your relevant_plans vector ────────────────────────────────────
caracteristicas_planos <- read_csv2(
  here("data","raw_data","ANS","operadoras","planos",
       "caracteristicas_produtos_saude_suplementar.csv"),
  locale = locale(encoding = "latin1")
) %>%
  clean_names() %>%
  filter(
    str_to_lower(contratacao) %in% c("coletivo empresarial", "coletivo por adesão"),
    !str_detect(str_to_lower(sgmt_assistencial), "odontol[óo]gico")
  ) %>%
  rename(cd_operadora = registro_operadora) %>%
  mutate(cd_operadora = as.character(cd_operadora)) %>%
  group_by(cd_plano, cd_operadora) %>%
  slice_max(dt_situacao, with_ties = FALSE) %>%
  ungroup()

relevant_plans <- caracteristicas_planos %>%
  filter(
    contratacao == "Coletivo empresarial",
    gr_sgmt_assistencial != "Ambulatorial"
  ) %>%
  distinct(id_plano) %>%
  pull()

# ── 3. Loop through each year, read + filter + select + collect ─────────────
years <- 2015:2024
panel_list <- vector("list", length(years))

for (i in seq_along(years)) {
  yr <- as.character(years[i])
  
  parquet_dir <- here("data","raw_data","ANS","beneficiarios", yr, "parquet")
  files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  panel_list[[i]] <- purrr::map_dfr(files, ~ {
    arrow::read_parquet(.x) %>%
      as_tibble() %>%
      filter(id_plano %in% relevant_plans) %>%
      select(
        cd_operadora,
        sg_uf,
        cd_municipio,
        tp_sexo,
        de_faixa_etaria,
        de_faixa_etaria_reaj,
        id_plano,
        de_abrg_geografica_plano,
        qt_beneficiario_ativo,
        year
      )
  })
  
  # free memory
  rm(files)
  gc()
}

# ── 4. Bind all years into one tibble ───────────────────────────────────────
full_panel <- bind_rows(panel_list)

full_panel %>% glimpse()

# ── 5. Write out to a single Parquet file ─────────────────────────────────
arrow::write_parquet(
  full_panel,
  here("data","processed_data","beneficiarios_filtered_2015_2024.parquet")
)
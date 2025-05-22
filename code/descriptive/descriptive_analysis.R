library(tidyverse)
library(arrow)
library(here)
library(janitor)
library(haven)
library(geobr)
library(sf)
library(viridis)
library(scales)


# -------------------------------
# 1.  LOAD PROCESSED READJUSTMENTS
# -------------------------------
message("Step 1: Load processed readjustments & compute averages…")
reaj <- open_dataset(
  here("data","processed_data","national","reajustes_panel_final.parquet")
)
relevant_plans <- reaj %>% distinct(id_plano) %>% pull()

# -------------------------------
# 2.  LOAD NETWORKS DATASET
# -------------------------------
message("Step 2: Load networks dataset…")
parquet_dir <- here(
  'data','raw_data','ANS','prestadores',
  'produtos e prestadores hospitalares',
  'produtos_prestadores_parquet'
)
networks <- open_dataset(parquet_dir, format = "parquet")
networks %>% glimpse()

# -------------------------------
# 3.  LOAD BENEFICIARIES DATASET
# -------------------------------
message("Step 3: Load filtered beneficiaries…")
beneficiarios <- open_dataset(
  here("data","processed_data","national","beneficiarios_filtered_2015_2024.parquet")
) %>%
  filter(id_plano %in% relevant_plans,
         qt_beneficiario_ativo > 0)
beneficiarios %>% glimpse()


# -------------------------------
# 4.  INSURANCE ROLLOUT MAP
# -------------------------------

# 0. Cache borders once
state_borders <- read_state(year = 2020, simplified = TRUE) %>%
  st_transform(4326)
country_border <- read_country(year = 2020, simplified = TRUE) %>%
  st_transform(4326)

# 1. All municipalities (2020) with 6-digit code
br_munis <- read_municipality(code_muni = "all", year = 2020, simplified = TRUE) %>%
  st_transform(4326) %>%
  mutate(cd_municipio = as.numeric(substr(code_muni, 1, nchar(code_muni) - 1)))

# 2. Summarize beneficiaries & collect()
hap_benef <- beneficiarios %>%
  filter(cd_operadora == "368253") %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo), .groups = "drop") %>%
  collect()

gndi_benef <- beneficiarios %>%
  filter(cd_operadora %in% c("359017", "348520")) %>%
  group_by(cd_municipio, year) %>%
  summarise(total_benef = sum(qt_beneficiario_ativo), .groups = "drop") %>%
  collect()


# 3. First year ≥50
hap_first <- hap_benef %>%
  filter(total_benef >= 50) %>%
  group_by(cd_municipio) %>%
  summarise(first_year = min(year), .groups = "drop") %>%
  mutate(operator = "Hapvida")

gndi_first <- gndi_benef %>%
  filter(total_benef >= 50) %>%
  group_by(cd_municipio) %>%
  summarise(first_year = min(year), .groups = "drop") %>%
  mutate(operator = "NotreDame Intermédica")

# 4. Join back to map & stack, then drop rows with no operator
br_map <- bind_rows(
  left_join(br_munis, hap_first,  by = "cd_municipio"),
  left_join(br_munis, gndi_first, by = "cd_municipio")
) %>%
  filter(!is.na(operator))    # remove the NA “operator” rows

# 5. Bin into three categories with correct factor levels (unchanged)
br_map <- br_map %>%
  mutate(
    first50_cat = case_when(
      !is.na(first_year) & first_year <= 2015               ~ "2015 or earlier",
      !is.na(first_year) & between(first_year, 2016, 2021)  ~ "2016–2021",
      !is.na(first_year) & first_year >= 2022               ~ "2022 or later",
      TRUE                                                  ~ NA_character_
    ),
    first50_cat = factor(
      first50_cat,
      levels = c("2015 or earlier", "2016–2021", "2022 or later")
    )
  )

# 6. Plot

ggplot(br_map) +
  # fill with three shades of grey rather than color
  geom_sf(aes(fill = first50_cat), color = NA, size = 0.01) +
  geom_sf(data  = state_borders,  fill = NA, color = "grey70", size = 0.2) +
  geom_sf(data  = country_border, fill = NA, color = "black", size = 0.4) +
  scale_fill_grey(
    start  = 0.78,  # lightest category
    end    = 0.1,  # darkest category
    na.value = "white",
    name      = "Year reached\n≥50 policyholders",
    labels    = c("2015 or earlier", "2016–2021", "2022 or later")
  ) +
  facet_wrap(~ operator, ncol = 2) +
  labs(
    # AER figures typically put the title in the main text, not on the figure,
    # so we often omit a title here or use a short “Figure x.” caption instead.
    caption = NULL
  ) +
  theme_bw(base_family = "Times", base_size = 12) +
  theme(
    panel.grid       = element_blank(),         # no grid
    strip.background = element_blank(),         # no background behind facet labels
    strip.text       = element_text(face = "bold", size = 16),
    legend.title     = element_text(size = 12, face = "bold"),
    legend.text      = element_text(size = 12),
    legend.position  = "bottom",                # horizontal legend below
    legend.direction = "horizontal",
    legend.key.width = unit(1.2, "cm"),
    legend.key       = element_rect(),
    axis.text        = element_blank(),
    axis.ticks       = element_blank(),
    axis.title       = element_blank()
  )

# Save output
ggsave(
  filename = here("output", "images", "national", "maps", "benef_rollout_map.png"),
  width    = 10,
  height   = 7,
  dpi      = 1000
)


# -------------------------------
# 4.  HAPVIDA + GNDI MARKET SHARE
# -------------------------------

# 1. Compute Hapvida, GNDI, and combined market shares in one pipeline
market_share_df <- beneficiarios %>%
  collect() %>%
  group_by(year) %>%
  summarise(
    total_benef    = sum(qt_beneficiario_ativo),
    hap_benef      = sum(qt_beneficiario_ativo[cd_operadora == "368253"]),
    gndi_benef     = sum(qt_beneficiario_ativo[cd_operadora %in% c("359017", "348520")]),
    .groups = "drop"
  ) %>%
  mutate(
    Hapvida       = hap_benef / total_benef,
    GNDI          = gndi_benef / total_benef,
    `Hapvida+GNDI` = (hap_benef + gndi_benef) / total_benef
  ) %>%
  select(year, Hapvida, GNDI, `Hapvida+GNDI`) %>%
  pivot_longer(
    cols = c(Hapvida, GNDI, `Hapvida+GNDI`),
    names_to  = "Series",
    values_to = "Share"
  )

ggplot(market_share_df, aes(x = as.integer(year), y = Share,
                            linetype = Series, shape = Series)) +
  geom_line(color = "black", size = 0.8) +
  geom_point(color = "black", size = 2) +
  scale_x_continuous(
    breaks = sort(unique(market_share_df$year))
  ) +
  scale_y_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(0, NA)
  ) +
  scale_linetype_manual(
    values = c(
      Hapvida         = "dashed",
      GNDI            = "dotted",
      `Hapvida+GNDI`  = "solid"
    )
  ) +
  scale_shape_manual(
    values = c(
      Hapvida         = 15,  # filled square
      GNDI            = 17,  # filled triangle
      `Hapvida+GNDI`  = 16   # filled circle
    )
  ) +
  labs(
    x     = "Year",
    y     = "Market Share"
  ) +
  theme_bw(base_family = "Times", base_size = 12) +
  theme(
    panel.grid       = element_blank(),
    panel.border     = element_rect(color = "black", fill = NA),
    axis.line        = element_line(color = "black"),
    axis.ticks       = element_line(color = "black"),
    axis.text        = element_text(size = 10, color = "black"),
    axis.title       = element_text(size = 11),
    plot.title       = element_text(size = 12, face = "plain", hjust = 0),
    legend.position  = "bottom",
    legend.direction = "horizontal",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 10),
    legend.key.width = unit(1.5, "cm")
  )

# Save a narrow figure for publication (e.g., single-column format)
ggsave(
  filename = here("output", "images", "national", "descriptive", "market_share_breakdown_narrow.png"),
  width    = 6,   # narrower width in inches
  height   = 5,     # maintain a reasonable aspect ratio
  dpi      = 1000
)


# -------------------------------
# 5.  PLANS WITH HAP/GNDI HOSPITALS
# -------------------------------

networks %>% glimpse()

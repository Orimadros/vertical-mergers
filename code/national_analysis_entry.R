# -------------------------------
# 0.  LIBRARIES
# -------------------------------
library(tidyverse)
library(arrow)
library(here)
library(janitor)
library(haven)
library(geobr)
library(sf)
library(lubridate)
library(patchwork)


# -------------------------------
# 1.  LOAD PLAN CHARACTERISTICS
# -------------------------------

caracteristicas_planos_path <- here('data',
                                    'raw_data',
                                    'ANS',
                                    'operadoras',
                                    'planos',
                                    'caracteristicas_produtos_saude_suplementar.csv')

caracteristicas_planos <- read_csv2(caracteristicas_planos_path) %>%
  clean_names()

# Filtering for employer-sponsored plans with hospital coverage.
caracteristicas_planos <- caracteristicas_planos %>%
  filter(contratacao == 'Coletivo empresarial',
         str_detect(str_to_lower(sgmt_assistencial), 'hospitalar')) %>%
  rename(cd_operadora = 'registro_operadora')

# -------------------------------
# 2. CANCELLATIONS TIME SERIES
# -------------------------------

# Define constants
year_range <- 2000:2024
operadoras  <- c("Hapvida", "GNDI")
color_map   <- c("Hapvida" = "orange", "GNDI" = "darkblue")

# Filter cancelled plans and prepare year + operator label
data_cancellations <- caracteristicas_planos %>%
  filter(situacao_plano == "Cancelado") %>%
  transmute(
    ano      = year(dt_situacao),
    operadora = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ NA_character_
    )
  ) %>%
  filter(!is.na(operadora))

# Summarize and complete missing years
summary_cancellations <- data_cancellations %>%
  count(ano, operadora) %>%
  complete(ano = year_range, operadora = operadoras, fill = list(n = 0))

# Plot cancellations
plot_cancellations <- ggplot(summary_cancellations,
                             aes(x = ano, y = n, color = operadora)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  scale_x_continuous(breaks = year_range, limits = c(min(year_range), max(year_range))) +
  scale_color_manual(values = color_map) +
  labs(
    title = "Plans Cancelled per Year for Hapvida and GNDI (2000–2024)",
    x     = "Year",
    y     = "Number of Cancelled Plans",
    color = "Operadora"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title      = element_text(hjust = 0.5),
    axis.text.x     = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

print(plot_cancellations)

# -------------------------------
# 3. REGISTRATIONS TIME SERIES
# -------------------------------

# Filter new plan entries and prepare year + operator label
data_registrations <- caracteristicas_planos %>%
  transmute(
    ano       = year(dt_registro_plano),
    operadora = case_when(
      cd_operadora == "368253" ~ "Hapvida",
      cd_operadora == "359017" ~ "GNDI",
      TRUE                     ~ NA_character_
    )
  ) %>%
  filter(!is.na(operadora))

# Summarize and complete missing years
summary_registrations <- data_registrations %>%
  count(ano, operadora) %>%
  complete(ano = year_range, operadora = operadoras, fill = list(n = 0))

# Plot registrations
plot_registrations <- ggplot(summary_registrations,
                             aes(x = ano, y = n, color = operadora)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  scale_x_continuous(breaks = year_range, limits = c(min(year_range), max(year_range))) +
  scale_color_manual(values = color_map) +
  labs(
    title = "Plans Created per Year for Hapvida and GNDI (2000–2024)",
    x     = "Year",
    y     = "Number of New Plans",
    color = "Operadora"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title      = element_text(hjust = 0.5),
    axis.text.x     = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

print(plot_registrations)

# Remove legend from the top panel
top_noleg <- plot_cancellations +
  theme(legend.position = "none")

# Keep the bottom panel as is (with its legend)
bottom <- plot_registrations

# Stack them without a master title
combined_plot <- top_noleg / bottom +
  plot_layout(heights = c(1, 1))

print(combined_plot)

# Define output directory using here()
output_dir <- here("output", "images", "national", "descriptive")

ggsave(
  filename = file.path(output_dir, "plan_entry_exit_hapgndi_2000-2024.png"),
  plot     = combined_plot,
  width    = 10,
  height   = 6,
  dpi      = 1000
)

# 1. Prepare df_totals, making sure 'ano' is integer and non-missing
df_totals <- cancel_total %>%
  full_join(reg_total, by = "ano") %>%
  replace_na(list(total_cancelled = 0, total_created = 0)) %>%
  pivot_longer(
    cols = c(total_cancelled, total_created),
    names_to  = "type",
    values_to = "count"
  ) %>%
  mutate(
    type = recode(type,
                  total_cancelled = "Cancelled",
                  total_created   = "Created"),
    ano  = as.integer(ano)
  ) %>%
  filter(!is.na(ano))

# 2. Build a clean year sequence
yrs <- seq(min(df_totals$ano, na.rm = TRUE),
           max(df_totals$ano, na.rm = TRUE),
           by = 1)

# 3. Plot
total_entry_exit <- ggplot(df_totals, aes(x = ano, y = count, color = type)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  scale_x_continuous(
    breaks = yrs,
    limits = range(yrs)
  ) +
  labs(
    title = "Plan creations and cancellations by Hapvida and GNDI",
    x     = "Year",
    y     = "Number of Plans",
    color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title      = element_text(hjust = 0.5),
    axis.text.x     = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

# Save the combined creations/cancellations plot
ggsave(
  filename = here(output_dir, "total_entry_exit_hapgndi.png"),
  plot     = last_plot(),
  width    = 10,
  height   = 6,
  dpi      = 1000
)


# sanity_checks.R

# Load libraries
library(arrow)
library(dplyr)
library(here)
library(tidyr)

# Define file path
plan_market_shares_file <- here("data", "processed_data", "national", "plan_market_shares.parquet")

# Check if file exists
if (!file.exists(plan_market_shares_file)) {
  stop("File not found: ", plan_market_shares_file)
}

# Read the parquet file
message("Reading ", plan_market_shares_file, "...")
shares_data <- read_parquet(plan_market_shares_file)
message("Data loaded. Dimensions: ", paste(dim(shares_data), collapse = " x "))

# --- Year Checks ---
message("\n--- Year Checks ---")
present_years <- shares_data %>% distinct(year) %>% arrange(year) %>% pull(year)
expected_years <- 2015:2024 # Based on original script intention for beneficiary data
missing_years <- setdiff(expected_years, present_years)
extra_years <- setdiff(present_years, expected_years)

message("Years present: ", paste(present_years, collapse=", "))
if (length(missing_years) > 0) {
  message("WARNING: Expected years missing: ", paste(missing_years, collapse=", "))
}
if (length(extra_years) > 0) {
  message("WARNING: Unexpected years found: ", paste(extra_years, collapse=", "))
}

# --- Data Volume Checks ---
message("\n--- Data Volume per Year ---")
rows_per_year <- shares_data %>% 
  count(year, sort = TRUE)
print(rows_per_year)

message("\n--- Data Volume per Insurer (Original) ---")
rows_per_insurer <- shares_data %>% 
  count(insurer, sort = TRUE)
print(rows_per_insurer)

message("\n--- Data Volume per Insurer (Merged) ---")
rows_per_merged_insurer <- shares_data %>% 
  count(merged_insurer, sort = TRUE)
print(rows_per_merged_insurer)


# --- State Check (Note: sg_uf is not in this dataset) ---
message("\n--- State Checks ---")
if ("sg_uf" %in% names(shares_data)) {
  present_states <- shares_data %>% distinct(sg_uf) %>% arrange(sg_uf) %>% pull(sg_uf)
  message("States present: ", paste(present_states, collapse=", "))
  # Add comparison to expected states if list is known
} else {
  message("NOTE: 'sg_uf' (state) column is not present in the final plan_market_shares dataset.")
  message("State-level checks cannot be performed on this file.")
  # Optional: Check distinct municipalities instead
  distinct_mun <- shares_data %>% distinct(cd_municipio) %>% summarise(n=n()) %>% pull(n)
  message("Number of distinct municipalities found: ", distinct_mun)
}


# --- NA Checks ---
message("\n--- General NA Value Checks ---") # Renamed section title
na_summary <- shares_data %>% 
  summarise(across(everything(), ~sum(is.na(.)))) %>% 
  pivot_longer(everything(), names_to = "column", values_to = "na_count") %>% 
  filter(na_count > 0)

if (nrow(na_summary) > 0) {
  message("Columns with NA values:")
  print(na_summary)
} else {
  message("No NA values found in any column.")
}

# --- Years with Specific NAs ---
message("\n--- Years with Specific NA Values ---")

# NA in cd_municipio
na_mun_years <- shares_data %>% 
  filter(is.na(cd_municipio)) %>% 
  distinct(year) %>% 
  arrange(year) %>% 
  pull(year)

if(length(na_mun_years) > 0) {
  message("Years with NA in 'cd_municipio': ", paste(na_mun_years, collapse = ", "))
} else {
  message("No NAs found in 'cd_municipio'.")
}

# NA in id_plano
na_plano_years <- shares_data %>% 
  filter(is.na(id_plano)) %>% 
  distinct(year) %>% 
  arrange(year) %>% 
  pull(year)

if(length(na_plano_years) > 0) {
  message("Years with NA in 'id_plano': ", paste(na_plano_years, collapse = ", "))
} else {
  message("No NAs found in 'id_plano'.")
}

# NA in treatment_status
na_treat_years <- shares_data %>% 
  filter(is.na(treatment_status)) %>% 
  distinct(year) %>% 
  arrange(year) %>% 
  pull(year)

if(length(na_treat_years) > 0) {
  message("Years with NA in 'treatment_status': ", paste(na_treat_years, collapse = ", "))
} else {
  message("No NAs found in 'treatment_status'.")
}


# --- Specific Column Value Checks ---
message("\n--- Specific Column Value Checks ---")
message("Market Share summary:")
print(summary(shares_data$market_share))

message("\nTotal Beneficiaries summary (denominator for market share):")
print(summary(shares_data$total_benef))
zero_total_benef_count <- shares_data %>% filter(total_benef == 0) %>% count() %>% pull(n)
na_total_benef_count <- shares_data %>% filter(is.na(total_benef)) %>% count() %>% pull(n)
message("Rows with total_benef == 0: ", zero_total_benef_count)
message("Rows with is.na(total_benef): ", na_total_benef_count)

# Check treatment status distribution
message("\nTreatment Status summary:")
treatment_summary <- shares_data %>% 
  count(treatment_status, treated, name = "row_count") %>% 
  # Add check for NAs explicitly if not already covered
  arrange(desc(row_count))
print(treatment_summary)

message("\n--- Sanity Checks Complete ---") 
# check_market_shares.R

library(arrow)
library(dplyr)
library(here)

# Define file path
file_path <- here("data", "processed_data", "national", "plan_market_shares.parquet")

message("--- Sanity Checks for: ", file_path, " ---")

# Check if file exists
if (!file.exists(file_path)) {
  stop("Output file not found: ", file_path)
}

# --- 1. Read File & Check Dimensions ---
message("\n[1] Reading file and checking dimensions...")
df <- tryCatch({
  read_parquet(file_path)
}, error = function(e) {
  stop("Failed to read the parquet file: ", e$message)
})

message("  Dimensions: ", nrow(df), " rows, ", ncol(df), " columns.")

# --- 2. Check Key Columns (Types and Missingness) ---
message("\n[2] Checking key columns...")
message("  Column Types:")
glimpse(df)

message("\n  Missing values summary for key columns:")
key_cols <- c("cd_municipio", "year", "id_plano", "cd_operadora", 
              "market_share", "treatment_status", "treated")
missing_summary <- df %>% 
  select(any_of(key_cols)) %>% 
  summarise(across(everything(), ~sum(is.na(.)))) %>% 
  tidyr::pivot_longer(everything(), names_to = "column", values_to = "na_count") %>% 
  filter(na_count > 0)

if (nrow(missing_summary) > 0) {
  print(missing_summary)
} else {
  message("  No missing values found in key columns (", paste(key_cols, collapse=", "), ").")
}

# --- 3. Market Share Value Verification ---
message("\n[3] Verifying market share values...")

# Check range (allowing for floating point slightly > 1)
share_range <- df %>% 
  summarise(min_share = min(market_share, na.rm = TRUE), 
            max_share = max(market_share, na.rm = TRUE))
message("  Market Share Range: [", 
        round(share_range$min_share, 4), ", ", 
        round(share_range$max_share, 4), "] (Expected [0, ~1])")

# Check sum within groups (sample a few municipalities)
set.seed(123) # for reproducibility
sample_munis <- df %>% distinct(cd_municipio) %>% sample_n(5) %>% pull(cd_municipio)

message("\n  Sum of market shares for sample municipalities (Expected ~1 per year):")
df %>% 
  filter(cd_municipio %in% sample_munis) %>% 
  group_by(cd_municipio, year) %>% 
  summarise(total_share = sum(market_share, na.rm = TRUE), .groups = "drop") %>% 
  print(n = 15) # Print a few rows


# --- 4. Check Categorical Assignments ---
message("\n[4] Checking categorical assignments...")

message("  Unique values for 'insurer':")
df %>% count(insurer) %>% print()

message("\n  Unique values for 'merged_insurer':")
df %>% count(merged_insurer) %>% print()

message("\n  Unique values for 'treatment_status':")
df %>% count(treatment_status) %>% print()

message("\n  Cross-tab of 'treatment_status' and 'treated':")
df %>% count(treatment_status, treated) %>% print()

# --- 5. Year Range Check ---
message("\n[5] Checking year range...")
year_range_actual <- df %>% summarise(min_year = min(year, na.rm = TRUE), max_year = max(year, na.rm = TRUE))
message("  Year range found: ", year_range_actual$min_year, " to ", year_range_actual$max_year)

message("\n--- Checks Complete ---") 
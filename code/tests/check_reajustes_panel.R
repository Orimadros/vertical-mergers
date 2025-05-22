# check_reajustes_panel.R

library(arrow)
library(dplyr)
library(here)

# Define file path
file_path <- here("data", "processed_data", "national", "reajustes_panel_final.parquet")

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

message("\n  Missing values summary:")
missing_summary <- df %>% 
  summarise(across(everything(), ~sum(is.na(.)))) %>% 
  tidyr::pivot_longer(everything(), names_to = "column", values_to = "na_count") %>%
  filter(na_count > 0)

if (nrow(missing_summary) > 0) {
  print(missing_summary)
} else {
  message("  No missing values found in any column.")
}

# --- 3. Filter Verification ---
message("\n[3] Verifying filters...")

# Check cd_agrupamento (should be 0 or NA)
message("  Unique values in cd_agrupamento (expected 0 or NA):")
df %>% count(cd_agrupamento) %>% print()

# Check lg_negociacao (should not be 1)
message("\n  Unique values in lg_negociacao (expected not 1):")
df %>% count(lg_negociacao) %>% print()

# Check lg_parcelado (should not be 1)
message("\n  Unique values in lg_parcelado (expected not 1):")
df %>% count(lg_parcelado) %>% print()

# Check id_plano (should have no NA)
na_id_plano <- df %>% filter(is.na(id_plano)) %>% nrow()
message("\n  Rows with NA id_plano (expected 0): ", na_id_plano)

# --- 4. Uniqueness Check (id_contrato, id_plano, year) ---
message("\n[4] Verifying uniqueness of contract-plan-year...")
duplicates_check <- df %>% 
  count(id_contrato, id_plano, year) %>%
  filter(n > 1)

if (nrow(duplicates_check) == 0) {
  message("  Check PASSED: All contract-plan-year combinations are unique.")
} else {
  message("  Check FAILED: Found ", nrow(duplicates_check), " duplicate contract-plan-year combinations. Example:")
  print(head(duplicates_check))
}

# --- 5. Year Range Check ---
message("\n[5] Checking year range...")
year_range <- df %>% summarise(min_year = min(year, na.rm = TRUE), max_year = max(year, na.rm = TRUE))
message("  Year range found: ", year_range$min_year, " to ", year_range$max_year)

message("\n--- Checks Complete ---") 
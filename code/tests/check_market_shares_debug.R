# check_market_shares_debug.R

library(arrow)
library(dplyr)
library(here)

# Define file path
file_path <- here("data", "processed_data", "national", "plan_market_shares.parquet")

message("--- Debugging Checks for: ", file_path, " ---")

# Check if file exists
if (!file.exists(file_path)) {
  stop("Output file not found: ", file_path)
}

# --- Read File ---
message("\n[1] Reading file...")
df <- tryCatch({
  read_parquet(file_path)
}, error = function(e) {
  stop("Failed to read the parquet file: ", e$message)
})
message("  Read ", nrow(df), " rows.")

# --- Check cd_municipio ---
message("\n[2] Checking distinct cd_municipio values...")
distinct_munis <- df %>% distinct(cd_municipio)
message("  Found ", nrow(distinct_munis), " distinct municipality codes:")
print(distinct_munis)

# --- Check Counts by Municipality (if multiple exist) ---
if (nrow(distinct_munis) > 1 && nrow(distinct_munis) < 20) {
  message("\n[3] Checking row counts per municipality...")
  df %>% count(cd_municipio) %>% print()
} else if (nrow(distinct_munis) >= 20) {
   message("\n[3] Checking row counts for top 20 municipalities...")
   df %>% count(cd_municipio, sort=TRUE) %>% head(20) %>% print()
}

# --- Check Years Present for the single municipality ---
if (nrow(distinct_munis) == 1) {
    message("\n[4] Checking years present for the single municipality...")
    df %>% count(year) %>% print()
}

message("\n--- Debug Checks Complete ---") 
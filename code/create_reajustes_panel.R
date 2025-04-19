# create_reajustes_panel.R

# -------------------------------
# 0. Load Libraries & Setup Paths
# -------------------------------
library(dplyr)
library(arrow)
library(lubridate)
library(here)
library(purrr)
library(janitor)
library(tidyr) # For unnest_longer if needed, and potentially pivot_wider/longer if schemas were very different

# Define paths
reajustes_raw_dir <- here("data", "raw_data", "ANS", "operadoras", "reajustes", "reajustes_parquet")
output_dir <- here("data", "processed_data", "national")
output_file <- file.path(output_dir, "reajustes_panel_final.parquet")

# Ensure output directory exists
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# -------------------------------
# 1. Generate and Find All Monthly File Paths (2015-01 to 2024-11)
# -------------------------------
message("Step 1: Identifying monthly reajustes files...")

# Generate sequence of expected monthly filenames
months_seq <- seq(as.Date("2015-01-01"), as.Date("2024-11-01"), by = "month")
expected_filenames <- paste0("PDA_RPC_", format(months_seq, "%Y%m"), ".parquet")
expected_filepaths <- file.path(reajustes_raw_dir, expected_filenames)

# Filter for files that actually exist
existing_filepaths <- expected_filepaths[file.exists(expected_filepaths)]

if (length(existing_filepaths) == 0) {
  stop("No readjustment Parquet files found in: ", reajustes_raw_dir)
} else if (length(existing_filepaths) < length(expected_filepaths)) {
  missing_files <- setdiff(expected_filenames, basename(existing_filepaths))
  warning("Missing ", length(missing_files), " expected reajustes files. Processing only the ",
          length(existing_filepaths), " files found.")
  # Optional: print missing files if needed
  # print(missing_files)
}

message("Found ", length(existing_filepaths), " files to process.")

# -------------------------------
# 2. Read and Combine All Files
# -------------------------------
message("Step 2: Reading and combining all files...")

# Use map_df to read each file and combine, automatically handling schema differences
# clean_names is applied within the map function to standardize before binding
all_reajustes_raw <- map_df(existing_filepaths, ~{
    tryCatch({
        read_parquet(.x) %>% clean_names()
    }, error = function(e) {
        warning("Failed to read or clean file: ", basename(.x), " | Error: ", e$message)
        return(NULL) # Return NULL for failed files
    })
})

if (nrow(all_reajustes_raw) == 0) {
    stop("Failed to read any data from the found files.")
}

message("Successfully combined data from ", length(existing_filepaths), " files.")
message("Raw combined data has ", nrow(all_reajustes_raw), " rows.")

# -------------------------------
# 3. Standardize Columns and Apply Initial Filters
# -------------------------------
message("Step 3: Standardizing key columns and applying initial filters...")

# Ensure essential columns exist (add as NA if completely missing from all files)
# Note: bind_rows usually handles this, but this is a safeguard.
essential_cols <- c("id_plano", "id_contrato", "cd_operadora", "dt_inic_aplicacao",
                    "lg_retificacao", "lg_negociacao", "lg_parcelado", "cd_agrupamento")

for(col in essential_cols){
    if (!col %in% names(all_reajustes_raw)) {
        all_reajustes_raw[[col]] <- NA
        message("  Added missing essential column: ", col)
    }
}


reajustes_std <- all_reajustes_raw %>% 
  # Standardize beneficiary count and percentage names using coalesce
  mutate(
    # Create standardized count, preferring qt_benef_comunicado if available
    benef_comunicado_std = coalesce(
        if ("qt_benef_comunicado" %in% names(.)) qt_benef_comunicado else NA,
        if ("benef_comunicado" %in% names(.)) benef_comunicado else NA
        ),
    # Create standardized percentage, preferring pc_percentual if available
    percentual_std = coalesce(
        if ("pc_percentual" %in% names(.)) pc_percentual else NA_character_,
        if ("percentual" %in% names(.)) percentual else NA_character_
        )
  ) %>% 
  # Convert standardized columns to final types
  mutate(
    benef_comunicado_final = as.integer(benef_comunicado_std),
    percentual_final = as.numeric(gsub(",", ".", percentual_std)),
    year = year(dt_inic_aplicacao),
    # Ensure flag/group columns are integer/numeric for filtering consistency
    # Coalesce with NA of the correct type if they might contain NAs
    lg_retificacao = coalesce(as.integer(lg_retificacao), NA_integer_),
    lg_negociacao  = coalesce(as.integer(lg_negociacao),  NA_integer_),
    lg_parcelado   = coalesce(as.integer(lg_parcelado),   NA_integer_),
    cd_agrupamento = coalesce(as.integer(cd_agrupamento), NA_integer_)
  ) %>% 
  # Apply core filters
  filter(
    (cd_agrupamento == 0 | is.na(cd_agrupamento)),
    is.na(lg_negociacao) | lg_negociacao != 1,
    is.na(lg_parcelado)  | lg_parcelado  != 1,
    !is.na(id_plano)
  )

message("After standardization and initial filters: ", nrow(reajustes_std), " rows remaining.")

# -------------------------------
# 4. Handle Retifications and Duplicates
# -------------------------------
message("Step 4: Handling retifications and removing duplicates...")

reajustes_cleaned <- reajustes_std %>% 
  group_by(id_contrato, id_plano, year) %>% 
  # Apply retification logic
  filter(
    # If lg_retificacao column existed and has any 1s in the group, keep only rows where it's 1 or NA
    if ("lg_retificacao" %in% names(.) && any(lg_retificacao == 1, na.rm = TRUE)) {
      lg_retificacao == 1 | is.na(lg_retificacao)
    } else {
      # Otherwise (column doesn't exist or no 1s), keep all rows for this group
      TRUE
    }
  ) %>% 
  # Keep only groups that now have exactly one observation
  filter(n() == 1) %>% 
  ungroup()

message("After retification handling and duplicate removal: ", nrow(reajustes_cleaned), " rows remaining.")

# -------------------------------
# 5. Final Selection and Save
# -------------------------------
message("Step 5: Selecting final columns and saving panel...")

# Select all original columns plus the standardized ones, renaming back if desired
# Or just select the key ones needed downstream
final_panel <- reajustes_cleaned %>% 
  select(
    # Identifiers
    id_contrato, id_plano, cd_operadora, cd_plano, year,
    # Key Values
    dt_inic_aplicacao, 
    benef_comunicado = benef_comunicado_final, # Rename standardized back
    percentual = percentual_final,             # Rename standardized back
    # Flags (keep original names as they are standard now)
    lg_retificacao, lg_negociacao, lg_parcelado, cd_agrupamento,
    # Other potentially useful columns (add any others needed from 'all_reajustes_raw')
    any_of(c("dt_fim_aplicacao", "dt_protocolo", "nm_protocolo", "sg_uf_contrato_reaj", "dt_carga", "dt_atualizacao"))
  )

final_panel <- final_panel %>%
  select('id_contrato',
         'id_plano',
         'cd_operadora',
         'year',
         'benef_comunicado',
         'percentual',
         'lg_retificacao',
         'sg_uf_contrato_reaj')

# Save the final panel
write_parquet(final_panel, output_file)

message("Successfully created final readjustment panel with ", 
        nrow(final_panel), " rows and ", ncol(final_panel), " columns.")
message("Output saved to: ", output_file)
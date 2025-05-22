# Load required libraries
library(arrow)
library(data.table)
library(janitor)

# Define the folder containing the CSV files
input_folder <- "../../data/raw_data/ANS/prestadores/produtos e prestadores hospitalares/produtos_e_prestadores_hospitalares"

# Define the output folder for Parquet files
output_folder <- "../../data/raw_data/ANS/prestadores/produtos e prestadores hospitalares/produtos_prestadores_parquet"

# Create the output directory if it doesn't exist
if (!dir.exists(output_folder)) {
  dir.create(output_folder, recursive = TRUE)
}

# List all CSV files in the folder
csv_files <- list.files(input_folder, pattern = "\\.csv$", full.names = TRUE)

# Function to convert a single CSV to Parquet with cleaning and filtering
to_parquet <- function(csv_path) {
  # Read CSV with data.table::fread for efficiency
  dt <- fread(csv_path)
  # Clean column names
  dt <- janitor::clean_names(dt)
  # Filter rows
  dt <- dt[
    de_tipo_contratacao == 'COLETIVO EMPRESARIAL' &
      de_clas_estb_saude == 'Assistencia Hospitalar',
  ]
  # Convert columns to character
  dt[, cd_operadora := as.character(cd_operadora)]
  dt[, cd_cnpj_estb_saude := as.character(cd_cnpj_estb_saude)]
  # Create output file name in the output folder
  parquet_filename <- sub("\\.csv$", ".parquet", basename(csv_path))
  parquet_path <- file.path(output_folder, parquet_filename)
  # Write to Parquet
  write_parquet(dt, parquet_path)
  cat(sprintf("Converted: %s -> %s\n", csv_path, parquet_path))
}

# Apply the conversion to all CSV files
invisible(lapply(csv_files, to_parquet))

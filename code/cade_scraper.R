# This is a script to scrape the Brazilian competition authority's decision
# memorandums, which contain the information on the mergers we want to analyze.

library(tidyverse)
library(rvest)
library(here)
library(stringr)

# getting the memo file names
pareceres_directory <- here('data', 'raw_data', 'CADE', 'pareceres') %>%
  list.files()

parse_memo <- function(file_path) {
  
  # Define the full path to the HTML file
  full_path <- here('data', 'raw_data', 'CADE', 'pareceres', file_path)
  
  # Read the HTML file
  page <- read_html(full_path)
  
  # Extract the "processo" field
  # Note: Adjust the CSS selector based on the actual HTML structure
  processo <- page %>% 
    html_nodes('tr:nth-child(2) .Texto_Alinhado_Esquerda_Espacamento_Simples_Maiusc') %>%
    html_text() %>%
    .[-1] %>%                           # Remove the first element if it's not needed
    str_trim() %>%                      # Trim whitespace
    paste(collapse = " ")               # Combine into a single string if multiple elements
  
  # Extract the relevant table's text
  # Note: Adjust the index [4] if the relevant table is in a different position
  text <- page %>%
    html_nodes('table') %>%
    html_text(trim = TRUE) %>%
    .[4]                                # Select the 4th table; adjust as necessary
  
  # Check if the table exists
  if (is.na(text)) {
    warning(paste("No table found in file:", file_path))
    return(NULL)  # Skip this file
  }
  
  # Split the text by the delimiter \r\n followed by two or more tabs
  split_text <- strsplit(text, "\\r\\n\\t{2,}")[[1]] %>%
    str_trim() %>%                        # Trim whitespace
    discard(~ .x == "")                    # Remove empty strings
  
  # Check if split_text has at least one key-value pair
  if (length(split_text) < 2) {
    warning(paste("Insufficient key-value pairs in file:", file_path))
    return(NULL)  # Skip this file
  }
  
  # Extract keys (odd-indexed elements)
  keys <- split_text[seq(1, length(split_text), by = 2)]
  
  # Extract values (even-indexed elements)
  values <- split_text[seq(2, length(split_text), by = 2)]
  
  # Handle cases where there's an odd number of elements
  if (length(keys) > length(values)) {
    values <- c(values, NA)  # Append NA for the missing value
  }
  
  # Create a named list where names are keys and elements are values
  key_value_list <- set_names(values, keys)
  
  # Combine "processo" with the key-value pairs
  # We'll add "Processo" as a separate field
  named_list <- c(list(Processo = processo), key_value_list)
  
  # Convert the named list to a tibble row
  df_row <- as_tibble(named_list)
  
  # Optionally, clean column names to make them syntactic
  df_row <- df_row %>%
    clean_names()  # Converts names to snake_case and removes special characters
  
  return(df_row)
}


page <- read_html(here('data', 'raw_data', 'CADE', 'pareceres', '[5]-1025487_Parecer_74.html'))
text <- page %>%
  html_nodes('table') %>%
  html_text(trim = TRUE) %>%
  .[4]

# Split the text by the delimiter \r\n\t\t\t
split_text <- strsplit(text, "\\r\\n\\t{2,}")[[1]] %>%
  str_trim() %>%
  discard(~ .x == "")

# Extract keys (odd-indexed elements)
keys <- split_text[seq(1, length(split_text), by = 2)]

# Extract values (even-indexed elements)
values <- split_text[seq(2, length(split_text), by = 2)]

# Create a named list where names are keys and elements are values
df <- set_names(values, keys) %>% as.list() %>% as_tibble()

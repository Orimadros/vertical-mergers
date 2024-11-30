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
  
  # reading html file
  file <- here('data', 'raw_data', 'CADE', 'pareceres', file_path)
  page <- read_html(file)
  
  # extracting the process id
  processo <- page %>% 
    html_nodes('tr:nth-child(2) .Texto_Alinhado_Esquerda_Espacamento_Simples_Maiusc') %>%
    html_text() %>%
    .[-1]
  
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

}

# Iterating through all HTML files and parsing them
aux <- parse_memo('[05]-0925806_Parecer_11.html')

#! /usr/bin/env Rscript

# File description -------------------------------------------------------------
# This code computes summary statistics to evaluate the results of alternative
# geocoding methods. It assumes csv tables for the output of geocoding experiments
# in a raw_data directory. In the current experiment, we are looking at geocoding
# results using the intersection and street address level approaches, as well as 
# a composite approach that chooses either street or intersection level depending
# on whether the sublocality agrees with the known sublocality.

# Setup  -----------------------------------------------------------------------
# List of packages for session
.packages  <-  c("data.table", "plyr", "dplyr", "stringi")

# Install CRAN packages (if not already installed)
.inst <- .packages %in% installed.packages()
if (any(!.inst)) {
  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
}

# Load packages into session 
sapply(.packages, require, character.only = TRUE)
graphics.off() # Close all open plots

street_results <- fread("raw_data/geocode_table_jul4.csv")
intersection_results <- fread("raw_data/geocode_table_intersections.csv")

## ---- utilities ----
remove_accents <- function(strings) {
  gsub("u0","\\u0", strings, fixed = TRUE) %>%
    stri_unescape_unicode() %>%
    stri_trans_general("Latin-ASCII")
}

process_for_match <- function(string) {
  processed <- gsub(" ", "", string) %>%
    tolower() %>%
    remove_accents()

  # this escapes characters for the grep in the next step
  # http://stackoverflow.com/questions/14836754/is-there-an-r-function-to-escape-a-string-for-regex-characters
  gsub("([.|()\\^{}+$*?]|\\[|\\])", "\\\\\\1", processed) 
}

## ---- flag-match-correctness ---- 
evaluate_matches <- function(x, y) {
  x <- process_for_match(x)
  y <- process_for_match(y)

  indic  <- vector(length = length(x))
  for (i in seq_along(x)) {
    indic[i] <- (grepl(x[i], y[i]) | grepl(y[i], x[i])) & !(x[i] == "" | y[i] == "")
  }
  data.frame(x, y, indic = indic)
}

###############################################################################
# same study, but with intersection level data
###############################################################################
m_street_locality  <- street_results %>%
  group_by(input_sublocality, sublocality) %>%
  summarise(count = n()) %>%
  arrange(input_sublocality, desc(count))

match_sublocality <- evaluate_matches(street_results$input_sublocality,
                                      street_results$sublocality)
street_results$match_sublocality <- match_sublocality$indic

match_locality <- evaluate_matches(street_results$input_locality,
                                   street_results$locality)
street_results$match_locality <- match_locality$indic

###############################################################################
# same study, but with intersection level data
###############################################################################
m_street_locality  <- intersection_results %>%
  group_by(input_sublocality, sublocality) %>%
  summarise(count = n()) %>%
  arrange(input_sublocality, desc(count))

match_sublocality <- evaluate_matches(intersection_results$input_sublocality,
                                      intersection_results$sublocality)
match_locality <- evaluate_matches(intersection_results$input_locality,
                                   intersection_results$locality)
intersection_results$match_sublocality <- match_sublocality$indic
intersection_results$match_locality <- match_locality$indic

###############################################################################
# define a composite method, that chooses either intersection or street level
# methods according to whether the sublocalities match
###############################################################################
subset_cols <- c("lat", "long", "input_sublocality", "sublocality",
                 "input_locality", "locality", "match_sublocality",
                 "match_locality")
inter_subset <- intersection_results[, subset_cols, with = F]
setnames(inter_subset, paste0(subset_cols, "_inter"))

street_subset <- street_results[, subset_cols, with = F]
setnames(street_subset, paste0(subset_cols, "_street"))

impute_composite <- function(street_results, intersection_results) {
  composite_results <- street_results
  replace_ix <- intersection_results$match_sublocality & (!street_results$match_sublocality)
  composite_results[replace_ix, c("lat", "long")] <- intersection_results[replace_ix, c("lat", "long"), with = F]

  composite_results$match_sublocality[replace_ix] <- TRUE
  composite_results$match_locality[replace_ix] <- TRUE
  composite_results$sublocality[replace_ix] <- intersection_results$sublocality[replace_ix]
  composite_results$result_type[replace_ix] <- intersection_results$result_type[replace_ix]

  composite_results
}

composite_results <- impute_composite(street_results, intersection_results)

###############################################################################
# combine datasets and compute summaries statistics
###############################################################################
all_results <- rbind(data.frame(intersection_results, query_type = "intersection"),
                     data.frame(street_results, query_type = "street"),
                     data.frame(composite_results, query_type = "composite"))

all_results$returned_sublocality <- all_results$sublocality != ""

all_results %>%
  filter(!returned_sublocality,
         sublocality != "") %>%
  select(query_type, sublocality, returned_sublocality)

all_results %>%
  group_by(query_type, result_type, returned_sublocality) %>%
  summarise(count = n(),
            match_sublocality = round(mean(match_sublocality), 2),
            match_locality = round(mean(match_locality), 2)) %>%
  arrange(query_type, desc(count), returned_sublocality) %>%
  data.frame()
  
all_results %>%
  group_by(query_type, returned_sublocality) %>%
  summarise(count = n(),
            match_sublocality = round(mean(match_sublocality), 2),
            match_locality = round(mean(match_locality), 2)) %>%
  arrange(query_type, desc(count), returned_sublocality) %>%
  data.frame()

# IDs that Cesar Ramirez used in evaluating the street level info
study_ids <- c("947496", "2853296", "433506", "3233670", "2087813", "1376031",
               "123574", "3784965", "5340335", "3608735", "2996970", "871898")
all_results %>%
  filter(search_id %in% study_ids) %>%
  arrange(search_id)

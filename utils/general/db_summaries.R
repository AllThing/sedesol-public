#! /usr/bin/env Rscript

## ---- setup ----

# Install CRAN packages (if not already installed)
.packages  <-  c("data.table", "plyr", "dplyr")
.inst <- .packages %in% installed.packages()
if(any(!.inst)) {
  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
}

# Load packages into session 
sapply(.packages, require, character.only = TRUE)
graphics.off() # Close all open plots

################################################################################
# This function includes functions for summarizing a postgres database
# without loading it all into memory.
################################################################################

#' @title Count number of entries that are not NA, and return them
#' @param field [dplyr database connection] A connection to the
#'   geocoding database, on which we can compute dplyr mutate /
#'   summarise.
#' @param field_name [string] A string specifying the column on which
#'   to compute NA information.
#' @return A list with the following elements,
#'     $num_non_na: The number of entries that are not NA, for the
#'     specified field.
#'     $non_na_data: The dplyr filtered database of non-na entries.
na_info <- function(field, field_name) {
  non_na_data <- field %>%
    filter_(paste0("!is.na(", field_name, ")"))
  list(num_non_na = nrow(non_na_data), non_na_data = non_na_data)
}

#' @title Given a dplyr connection for a numeric field, output summary stats
#' @param field [dplyr database connection] A connection to the
#'   geocoding database, on which we can compute dplyr mutate /
#'   summarise.
#' @param field_name [string] A string specifying the column on which
#'   to compute summary statistics.
#' @return A data.frame with the proportion NAs, min, max, mean, and
#'   sd for the queried field.
numeric_stats <- function(field, field_name) {
  cast_str <- sprintf("CAST( %s AS NUMERIC)", field_name)
  field %>%
    summarise(num = sql(cast_str)) %>%
    summarise(min = min(num), max = max(num),
              mean = mean(num), sd = sd(num)) %>%
    as.data.frame()
}

#' @title Given a dplyr connection for a categorical field, output
#'   summary stats
#' @param field [dplyr database connection] A connection to the
#'   geocoding database, on which we can compute dplyr mutate /
#'   summarise.
#' @param field_name [string] A string specifying the column on which
#'   to compute summary statistics.
#' @return A data.frame with the number of distinct values, maximum
#'   proportion, minimum proportion, and shannon entropy for the
#'   queried field.
categorical_stats <- function(counts) {
  n_levels <- nrow(counts)
  N <- sum(counts$n_distinct)
  p <- counts$n_distinct / N
  list(n_levels = n_levels, max_prop = p[1], min_prop = p[n_levels],
       shannon = - sum(p * log(p)))
}

#' @title Given a dplyr connection, get counts for categories in a field
#' @param field [dplyr database connection] A connection to the
#'   geocoding database, on which we can compute dplyr mutate /
#'   summarise.
#' @param field_name [string] A string specifying the column on which
#'   to compute quantiles.
#' @param min_count [int] The minimum number of times a category level
#'   must in order for us to include it in the output.
#' @return A data.table with counts for each unique value in the
#'   field_name column, arranged in descending order
get_counts <- function(field, field_name, min_count = 0) {
  field %>%
    group_by_(field_name) %>%
    summarise(n_distinct = n()) %>%
    arrange(desc(n_distinct)) %>%
    filter(n_distinct > min_count) %>%
    collect() %>%
    as.data.table()
}

#' @title Given a dplyr connection, get quantiles for a field
#' @param field [dplyr database connection] A connection to the
#'   geocoding database, on which we can compute dplyr mutate /
#'   summarise.
#' @param field_name [string] A string specifying the column on which
#'   to compute quantiles.
#' @param n_q [integer] An integer specifying the number of quantiles
#'   to use. Must have "L" at the end, for SQL to understand it.
#' @return A data.table with two columns encoding the quantiles of the
#'   queried field.
get_quantiles <- function(field, field_name, n_q = 1000L) {
  cast_str <- sprintf("CAST( %s AS NUMERIC)", field_name)
  field %>%
    summarise(num = sql(cast_str)) %>%
    mutate(tile = ntile(num, n_q)) %>%
    group_by(tile) %>%
    filter(row_number()==1) %>%
    as.data.table() %>%
    summarise(num = num, quantile = tile / n_q)
}

#! /usr/bin/env Rscript

###############################################################################
# This script calculates summaries of geocoding data in a postgresql
# database. It needs the working directory to be the sedesol
# repository. It expects database and connection information to be
# stored in db_profile.yaml and connection.yaml, respectively.
###############################################################################

## ---- setup ----
# assumed to run from repository base directory
source("utils/db_summaries.R")

# Install CRAN packages (if not already installed)
.packages  <-  c("plyr", "dplyr", "yaml", "data.table", "RPostgreSQL")
.inst <- .packages %in% installed.packages()
if(any(!.inst)) {
  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
}

# Load packages into session 
sapply(.packages, require, character.only = TRUE)
rm(list = ls()) # Delete all existing variables
graphics.off() # Close all open plots

## ---- connect-db ----
db_info <- yaml.load_file("db_profile.yaml")
conn_info <- yaml.load_file("connection.yaml")

tunnel <- sprintf(
  "ssh -fNT -L %s:%s:%s -i %s %s@%s",
  db_info$PGPORT,
  conn_info$CONNECTION_HOST,
  conn_info$CONNECTION_PORT,
  conn_info$KEY,
  conn_info$TUNNEL_USER,
  conn_info$TUNNEL_HOST
)
                  
system(tunnel)
db_conn <- src_postgres(dbname = db_info$PGDATABASE,
                        host = db_info$PGHOST,
                        user = db_info$PGUSER,
                        password = db_info$PGPASSWORD,
                        port = db_info$PGPORT,
                        options="-c search_path=raw")

## ---- initialize-output ----
geo <- tbl(db_conn, "geo_cuis")
fields <- colnames(geo)

#TODO: we want to write this to our AWS area via ssh -Fs
output_dir <- file.path("summary_data", "geocode")
dir.create(output_dir, recursive = TRUE)
num_path <- file.path(output_dir, "num_summaries.tsv")
cat_path <- file.path(output_dir, "cat_summaries.tsv")

num_header <- c("variable", "prop_na", "min", "max", "mean", "sd")
cat_header <- c("variable", "prop_na", "n_levels", "max_prop",
                "min_prop", "shannon")

cat(paste0(num_header, collapse = "\t"), "\n", file = num_path)
cat(paste0(cat_header, collapse = "\t"), "\n", file = cat_path)

## ---- write-summaries ----
numeric_fields <- c("geo_id", "longitud", "latitud", "s_longitud", "s_latitud")
N <- nrow(geo)

for (j in seq_along(fields)) {
  cat(sprintf("Processing %s \n", fields[j]))
  cur_field <- geo %>%
    select_(fields[j])

  # na info
  non_na <- na_info(cur_field, fields[j])
  prop_na <- 1 - non_na$num_non_na / N

  # summary stats
  if (fields[j] %in% numeric_fields) {
    detail <- get_quantiles(non_na$non_na_data, fields[j])
    stats <- numeric_stats(cur_field, fields[j])
    out_path <- num_path
  } else {
    min_thresh <- ifelse(grepl("desc", fields[j]), 10, 0) # truncate the description fields
    detail <- get_counts(non_na$non_na_data, fields[j], min_thresh)
    stats <- categorical_stats(detail)
    out_path <- cat_path
  }
   
  # write results
  write.csv(detail, paste0(output_dir, "/detail_", fields[j], ".csv"),
            row.names = FALSE)
  stats_string <- c(fields[j], prop_na, stats) %>%
    as.character() %>%
    paste0(collapse = "\t")
  cat(stats_string, "\n", file = out_path, append = TRUE)  
}

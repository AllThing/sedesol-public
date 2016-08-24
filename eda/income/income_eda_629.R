#! /usr/bin/env Rscript

# File description -------------------------------------------------------------
# This file contains exploratory analysis of income data used in the
# data stories for the call with Sedesol on June 29, 2016. The main
# figures we created are
#
#      (1) Plots of the estimated (ingreso_pc) vs. self-reported
#          (monto) incomes for individuals within a single localidad.
#      (2) Histograms of the estimated, self-reported, and IMSS incomes
#          within a single localidad.

# Setup packages ---------------------------------------------------------------
# List of packages for session
.packages  <-  c("data.table", "plyr", "dplyr", "ggplot2", "yaml",
                 "RPostgreSQL")

# Install CRAN packages (if not already installed)
.inst <- .packages %in% installed.packages()
if (any(!.inst)) {
  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
}

# Load packages into session
sapply(.packages, require, character.only = TRUE)

# close graphics and setup theme
graphics.off()
theme_set(theme_bw())
min_theme <- theme_update(panel.border = element_blank(),
                          panel.grid = element_blank(),
                          axis.ticks = element_blank(),
                          legend.title = element_text(size = 8),
                          legend.text = element_text(size = 6),
                          axis.text = element_text(size = 6),
                          axis.title = element_text(size = 8),
                          strip.background = element_blank(),
                          strip.text = element_text(size = 8),
                          legend.key = element_blank())

## ---- setup-connection ----
source(file.path("utils", "general", "db_conn.R"))
source(file.path("eda", "income", "income_eda_utils_629.R"))
conn <- db_conn("conf/db_profile.yaml", "conf/connection.yaml")

## ---- high-imss-earner ----
loc_incomes <- get_income_data(conn, "50300001")
income_data <- merge_income_data(loc_incomes$sifode_univ,
                  loc_incomes$se_inte,
                  loc_incomes$imss)

p <- plot_merged_incomes(income_data, "5317765")
ggsave(file = "income_high_imss.png", p[[1]], dpi = 500, width = 3, height = 3)
ggsave(file = "income_high_imss_facet.png", p[[3]], dpi = 500)
ggsave(file = "histo_high_imss.png", p[[5]], dpi = 500, height = 2, width = 3.5)
ggsave(file = "histo_no_zeros_high_imss.png", p[[6]], dpi = 500, height = 2, width = 3.5)

## ---- Fernanda's example ----
loc_incomes <- get_income_data(conn, "240540089")
income_data <- merge_income_data(loc_incomes$sifode_univ,
                  loc_incomes$se_inte,
                  loc_incomes$imss)

p <- plot_merged_incomes(income_data, "1727255")
ggsave(file = "income_known_fraud.png", p[[1]], dpi = 500, width = 3, height = 3)
ggsave(file = "income_known_fraud_facet.png", p[[3]], dpi = 500)
ggsave(file = "histo_known_fraud.png", p[[5]], dpi = 500, height = 2, width = 3.5)
ggsave(file = "histo_no_zeros_known_fraud.png", p[[6]], dpi = 500, height = 2, width = 3.5)

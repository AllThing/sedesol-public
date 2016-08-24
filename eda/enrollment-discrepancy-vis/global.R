library("data.table")
library("DT")
library("shiny")
library("plyr")
library("dplyr")
library("ggplot2")
library("reshape2")

source("core/get_scores.R")


theme_set(theme_bw())
min_theme <- theme_update(panel.border = element_blank(),
                          panel.grid = element_blank(),
                          axis.ticks = element_blank(),
                          legend.title = element_text(size = 15),
                          legend.text = element_text(size = 10),
                          axis.text = element_text(size = 15),
                          axis.title = element_text(size = 20),
                          strip.background = element_blank(),
                          strip.text = element_text(size = 15),
                          legend.key = element_blank())

discrepancies <- fread("data/final_outputlist_underreport.csv") %>%
    filter(!is.na(income_self_reported),
           !is.na(estimated_income)) %>%
    mutate(income_diff = estimated_income - income_self_reported) %>%
    as.data.table()

discrepancies[, overall_score:=numeric(nrow(discrepancies))]
discrepancies[, lbm_score:=numeric(nrow(discrepancies))]

cols_order <- c("home_id",
                "overall_score",
                "has_underreporting",
                "income_diff",
                "estimated_income",
                "income_self_reported",
                "type_of_locality")

discrepancies <- discrepancies[, cols_order, with = F]

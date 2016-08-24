library("jsonlite")
library("ggplot2")
library("dplyr")
library("reshape2")
library("stringr")

theme_set(theme_bw())
min_theme <- theme_update(panel.border = element_blank(),
                          panel.grid = element_blank(),
                          axis.ticks = element_blank(),
                          axis.title = element_text(size = 14),
                          axis.text.x = element_text(size = 10),
                          axis.text.y = element_text(size = 10),
                          strip.text = element_text(size = 8),
                          legend.title = element_text(size = 13),
                          legend.text = element_text(size = 9),
                          axis.text = element_text(size = 6),
                          axis.title = element_text(size = 8),
                          strip.background = element_blank(),
                          strip.text = element_text(size = 8),
                          legend.key = element_blank())

# get results from the database
conn <- src_postgres(dbname = "",
                     host = "",
                     port = "",
                     user = "",
                     password = "",
                     options = "-c search_path=models")

model_results <- conn %>%
    tbl("pub_imputation_spatial_subsets_json") %>%
    collect()
n_models <- nrow(model_results)

# get jsons from the models
model_jsons <- list()
for (i in seq_len(n_models)) {
    model_jsons[[i]] <- fromJSON(as.character(model_results["metadata"][i, 1]))
}

# parse jsons into a data frame
precisions <- list()
recalls <- list()
for (i in seq_len(n_models)) {
    config <- data.frame(param = model_jsons[[i]]$config$param,
                         model = model_jsons[[i]]$config$model,
                         subset_type = model_jsons[[i]]$config$subset,
                         features_path = model_jsons[[i]]$config$features_path,
                         time_to_run = model_jsons[[i]]$config$time_to_run,
                         string = model_jsons[[i]]$config$string)
    cur_precision <- model_jsons[[i]]$metrics$precision_at_k %>%
        fromJSON() %>%
        as.data.frame()
    cur_recall <- model_jsons[[i]]$metrics$recall_at_k %>%
        fromJSON() %>%
        as.data.frame()
    precisions[[i]] <- cbind(type = "precision", config, cur_precision,
                             ix = 1:nrow(cur_precision))
    recalls[[i]] <- cbind(type = "recall", config, cur_recall,
                          ix = 1:nrow(cur_recall))
}

keep_state_ids <- c("sifode_geo_census_subset_07",
                    "sifode_geo_census_subset_21",
                    "sifode_geo_census_subset_12",
                    "sifode_geo_census_subset_13",
                    "sifode_geo_census_subset_20")

all_metrics <- do.call(rbind, c(precisions, recalls))
all_metrics <- all_metrics %>%
    dplyr::filter(subset_type %in% keep_state_ids)

id_vars <- c("type", "model", "param", "features_path", "time_to_run",
             "string", "subset_type", "ix")
m_metrics <- all_metrics %>%
    melt(id.vars = id_vars) %>%
    dplyr::filter(variable %in% c("indicator_for_health_care_deprivation",
                                  "indicator_food_deprivation",
                                  "indicator_for_basic_housing_services"))

indicators_mapping <- c("indicator_food_deprivation" = "Food",
                        "indicator_for_basic_housing_services" = "Housing Services",
                        "indicator_for_health_care_deprivation" = "Health Care")
keep_state_names <- c("Chiapas", "Puebla", "Guerrero", "Hidalgo", "Oaxaca")

m_metrics$variable <- revalue(m_metrics$variable, indicators_mapping)
m_metrics$subset_type <- mapvalues(m_metrics$subset_type, from = keep_state_ids,
                                   to = keep_state_names)
m_metrics$ix <- m_metrics$ix / 50

p <- ggplot(m_metrics) +
    geom_line(aes(x = rev(ix), y = value, group = interaction(type, string, variable), col = type),
              alpha = 0.6) +
    facet_grid(variable~subset_type) +
    guides(color = guide_legend(override.aes = list(size = 2, alpha = 1))) +
    labs(x = "Proportion of Population", y = "Value", col = "Metric") +
    theme(strip.text = element_text(size = 12),
          axis.text.x = element_text(size = 6),
          axis.title.x = element_text(size = 13),
          axis.title.y = element_text(size = 13),
          axis.text.y = element_text(size = 7)) +
    scale_color_manual(values = c("#7fc97f", "#beaed4"))
p
#ggsave("~/Desktop/pr_curves.png", p, dpi = 400)

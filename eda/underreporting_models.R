###############################################################################
# Visualize the results from an under-reporting model.
###############################################################################

## ---- setup ----
library("ggplot2")
library("data.table")
library("plyr")
library("dplyr")

X_path <- "../raw_data/underreporting_features.csv"
y_path <- "../raw_data/underreporting_responses.csv"
p_path <- "../raw_data/underreporting_probs.csv"
i_path <- "../raw_data/underreporting_importances.csv"


# generated using retrieve_model_fits.py and writing to csv
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

## ---- get-data ----
X <- fread(X_path)
y <- fread(y_path)
p_hat <- fread(p_path)
importances <- fread(i_path, header = TRUE)
importances <- data.table(importances, colnames(X))
setnames(importances, c("importance", "feature"))
importances <- importances %>%
    arrange(desc(importance))

## ---- reshape-data ----
X$id <- seq_len(nrow(X))
is_categorical <- function(x, threshold = 20) {
    length(unique(x[1:2000])) <= 20
}

x_types <- colwise(is_categorical)(X)
x_types <- unlist(x_types)
id_vars <- c("id", "p.has_discrepancy", "p.has_underreporting",
             "y.has_discrepancy", "y.has_underreporting")

convert_y <- function(y, type) {
    y <- as.character(y)
    y[y == "0"] <- sprintf("no_%s", type)
    y[y == "1"] <- type
    y
}

y$has_discrepancy <- convert_y(y$has_discrepancy, "discrepancy")
y$has_underreporting <- convert_y(y$has_underreporting, "underreporting")

mX <-cbind(X, p = p_hat, y = y) %>%
    melt(id.vars = id_vars)
mX$categorical <- x_types[mX$variable]

## ---- figure-utils ----
#' @title Create a histogram of predicted probabilities vs. feature
#'
#' @param mX data.frame A melted version of the features + predicted
#' probabilities data.frame
#' @return ggplot object giving histogram of predicted probabilities split
#' by the feature and whether the respondent was flagged as underreporting.
categorical_plots <- function(mX, type = "has_underreporting") {
    p <- list()

    y_counts <- mX %>%
        group_by_(paste0("y.", type), "value") %>%
        summarise(count = n())
    p[[1]] <- ggplot(y_counts) +
        geom_bar(aes_string(x = "value", y = "count"),
                 stat = "identity") +
        facet_grid(formula(sprintf("y.%s ~ .", type)),
                   scale = "free_y") +
        scale_fill_manual(values = c("#7fc97f", "#beaed4")) +
    ggtitle(paste(mX$variable[1], type))


    p[[2]] <- ggplot(mX) +
        geom_histogram(aes_string(x = paste0("p.", type),
                                  fill = paste0("y.", type)),
                       bins = 30) +
        scale_fill_manual(values = c("#7fc97f", "#beaed4")) +
    facet_grid(formula(sprintf("y.%s ~ value", type)),
               scale = "free_y") +
        ggtitle(paste(mX$variable[1], type))
    p
}

#' @title A scatterplot of predicted probabilities vs. the feature
#'
#' @param mX data.frame A melted version of the features + predicted
#' probabilities data.frame
#' @return ggplot object giving histogram of predicted probabilities split
#' by the feature and whether the respondent was flagged as underreporting.
numeric_plots <- function(mX, type = "has_underreporting") {
    p <- list()
    mX$value <- runif(nrow(mX), min = .95, max = 1.05) * mX$value
    p[[1]] <- ggplot(mX) +
        geom_histogram(aes(x = value), bins = 50) +
        facet_grid(formula(sprintf("y.%s ~ .", type)),
                   scale = "free_y") +
        ggtitle(paste(mX$variable[1], type))

    p[[2]] <- ggplot(mX, aes_string(y = paste0("p.", type),
                                    x = "value",
                                    col = paste0("y.", type),
                                    fill = paste0("y.", type))) +
        stat_density_2d(n = 200, alpha = 0.15, geom = "polygon", col = "white") +
        geom_point(alpha = 0.3, col = "black", size = .35) +
        geom_point(alpha = 0.3, size = .3) +
        scale_fill_manual(values = c("#7fc97f", "#beaed4")) +
    scale_color_manual(values = c("#7fc97f", "#beaed4")) +
    facet_grid(formula(sprintf("y.%s ~ .", type))) +
        ggtitle(paste(mX$variable[1], type))

    p
}

## ---- get-figures ----
p <- list()
for (cur_ix in 1:15) {
    mX_sub <- mX %>%
        filter(variable %in% importances$feature[cur_ix])
    if(mX_sub$categorical[cur_ix]) {
        p <- c(p, categorical_plots(mX_sub),
               categorical_plots(mX_sub, "has_discrepancy"))
    } else {
        p <- c(p, numeric_plots(mX_sub),
               numeric_plots(mX_sub, "has_discrepancy"))
    }
}

p

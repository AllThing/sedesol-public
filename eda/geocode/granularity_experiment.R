options(scipen = 99)
# user specific parameters
shp_dir <- file.path("raw_data", "manzanas")

## ---- setup-packages ----
# Install CRAN packages (if not already installed)
.packages  <-  c("rgdal", "yaml", "data.table", "sp", "geosphere",
                 "rgeos", "plyr", "dplyr", "ggplot2")
.inst <- .packages %in% installed.packages()
if(any(!.inst)) {
  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
}
# Load packages into session 
sapply(.packages, require, character.only = TRUE)
graphics.off() # Close all open plots

## ---- get-areas ----
nom_ef <- "distrito-federal"
ef_ids <- setNames(c(paste0("0", 1:9), 10:32),
                   c("aguascalientes", "baja-california", "baja-california-sur", "campeche", "coahuila-de-zaragoza", "colima", "chiapas", "chihuahua", "distrito-federal", "durango", "guanajuato", "guerrero", "hidalgo", "jalisco", "mexico", "michoacan-de-ocampo", "morelos", "nayarit", "nuevo-leo\314\201n", "oaxaca", "puebla", "queretaro", "quintana-roo", "san-luis-potosi", "sinaloa", "sonora", "tabasco", "tamaulipas", "tlaxcala", "veracruz-de-ignacio-de-la-llave", "yucatan", "zacatecas"))


# read in shape file data for one state
read_state_data <- function(shp_dir, ef_ids, nom_ef) {
    state_data <- readOGR(shp_dir, paste(ef_ids[nom_ef], nom_ef, sep = "-"))
    state_df <-  as.data.table(state_data)
    polys <- state_data@polygons
    state_df$area <- sapply(polys, function(x) x@area)
    return(state_df)
}

# return total area in square meters
areas_sum <- function(areas_df, granularity = "CVE_LOCC") {
  areas_df %>%
    group_by_(granularity) %>%
    summarise(total_area = sum(area))
}

# get area sums at different granularities, and combine them into a
# single data frame.
get_areas_sum <- function(state_df) {  
    granularity_levels <- c("CVE_MUNC", "CVE_LOCC", "CVE_AGEBC", "CVE_MZAC")
    area_sums <- lapply(granularity_levels,
                        function(x) {
                          area_df <- areas_sum(state_df, x)
                          area_df$type <- x
                          colnames(area_df) <- c("id", "total_area", "type")
                          area_df
                        })
    area_sums <- do.call(rbind, area_sums)
    # reorder levels
    area_sums$type <- factor(area_sums$type, levels = granularity_levels)
    return(area_sums)
}

## ---- visualize-areas ----
plot_areas <- function(area_sums) {
    hist <- ggplot(area_sums/1000000) +
      geom_histogram(aes(x = total_area)) +
      scale_x_log10() +
      facet_grid(type ~ ., scale = "free_y") +
      labs(x = "Total Area (square km)")
   
    ecdf <- ggplot(area_sums) +
      stat_ecdf(aes(x = total_area/1000000)) +
      scale_x_log10() +
      facet_grid(type ~ ., scale = "free_y") +
      labs(x = "Total Area (square km)", y = "ECDF")
    ggsave(ecdf, file = "area_by_granularity_aggregate_states_ecdf_test.png")

    hist_by_granularity <- list()
    for (level in granularity_levels) {
      p[[level]] <- ggplot(area_sums %>%
                             filter(type == level)) +
        scale_x_log10() +
        geom_histogram(aes(x = total_area)) +
        ggtitle(level)
    }

    hist_by_granularity
}

areas_sum_all_states <- data.frame() 
for (nom_ef in names(ef_ids)[1:2]){
    state_df <- read_state_data(shp_dir, ef_ids, nom_ef) 
    areas_sum_per_state <- get_areas_sum(state_df)
    areas_sum_all_states <- rbind(areas_sum_all_states, areas_sum_per_state)
}

plot_areas(areas_sum_all_states)

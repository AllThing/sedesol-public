.packages  <-  c("plyr", "dplyr", "yaml", "data.table", "RPostgreSQL")
sapply(.packages, require, character.only = TRUE)

#.inst <- .packages %in% installed.packages()
#if(any(!.inst)) {
#  install.packages(.packages[!.inst], repos = "http://cran.rstudio.com/")
#}

db_info <- yaml.load_file("/Users/dolano/htdocs/dssg/sedesol-proj/scrap/db_profile.yaml")
conn_info <- yaml.load_file("/Users/dolano/htdocs/dssg/sedesol-proj/scrap/connection.yaml")

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

geo <- tbl(db_conn, "geo_cuis")
fields <- colnames(geo)
  
#get total value counts by state
cve_state_to_nom <- geo %>%                                       
                    select_("cve_entidad_federativa","nom_entidad_federativa") %>%   
                    group_by_("cve_entidad_federativa","nom_entidad_federativa") %>% 
                    summarise(countnames=n()) %>%
                    arrange(cve_entidad_federativa)

cv_df = cve_state_to_nom %>% 
        group_by_("cve_entidad_federativa") %>%
        summarise(total=sum(countnames))  %>% 
        as.data.table()

cve_map <- cve_state_to_nom %>%
    group_by_("cve_entidad_federativa") %>%
    filter(!is.na(nom_entidad_federativa)) %>%
    filter(countnames == max(countnames)) %>%
    arrange() %>%
    as.data.table() 
       
total_counts_by_state <- join(cv_df, cve_map, by='cve_entidad_federativa', match='first')



#GET value counts for not missing latitude and longitude , in future start using collect() ( as.data.table truncates to 100,000 rows)
cve_state_to_nom_not_missing <- geo %>%                                       
    select_("cve_entidad_federativa","latitud","longitud") %>%   
    filter(!is.na(latitud) & !is.na(longitud)) %>%
    group_by_("cve_entidad_federativa") %>% 
    summarise(num_valid_lat_lon=n()) %>%
    arrange(cve_entidad_federativa) %>%
    as.data.table()

#JOIN total counts and not missing lat/lon counts by state.   Remove countnames column.
cve_stats <- join(total_counts_by_state, cve_state_to_nom_not_missing, by='cve_entidad_federativa') %>% 
              select(-countnames) 

#GET Urban/Rural numbers by state
urban_rural_by_state <- geo %>% 
                        group_by_("cve_entidad_federativa","s_tipoloc") %>%
                        select(cve_entidad_federativa,s_tipoloc) %>%
                        summarise(count=n()) %>%
                        arrange(cve_entidad_federativa,s_tipoloc) %>%
                        as.data.table()


#ADD total RURAL INFO TO cve_stats
rural <- urban_rural_by_state %>% 
                   filter(grepl("R",s_tipoloc)) %>%
                   select(cve_entidad_federativa,count) %>%
                   rename(total_rural = count)

#cve_stats <- cve_stats %>% select(-urban)
cve_stats <- join(cve_stats, rural, by='cve_entidad_federativa')



#ADD total URBAN INFO
urban <- urban_rural_by_state %>% 
                filter(grepl("U",s_tipoloc)) %>%
                select(cve_entidad_federativa,count) %>%
                rename(total_urban = count)

cve_stats <- join(cve_stats, urban, by='cve_entidad_federativa')

#Get Urban/Rural for valid lat,lon rows only
urban_rural_by_state_with_latlon <- geo %>% 
  filter(!is.na(latitud) & !is.na(longitud)) %>%
  group_by_("cve_entidad_federativa","s_tipoloc") %>%
  select(cve_entidad_federativa,s_tipoloc) %>%
  summarise(count=n()) %>%
  arrange(cve_entidad_federativa,s_tipoloc) %>%
  as.data.table()

#ADD lat/lon RURAL INFO TO cve_stats
rural_latlon <- urban_rural_by_state_with_latlon %>% 
  filter(grepl("R",s_tipoloc)) %>%
  select(cve_entidad_federativa,count) %>%
  rename(latlon_rural = count)

cve_stats <- join(cve_stats, rural_latlon, by='cve_entidad_federativa')


#ADD lat/long URBAN INFO
urban_latlon <- urban_rural_by_state_with_latlon %>% 
  filter(grepl("U",s_tipoloc)) %>%
  select(cve_entidad_federativa,count) %>%
  rename(latlon_urban = count)

cve_stats <- join(cve_stats, urban_latlon, by='cve_entidad_federativa')


#ADD PROPORTION_WITH_LATLON NUMBERS
cve_stats$with_latlon_prop <- cve_stats$num_valid_lat_lon / cve_stats$total

#SORT BY ENTIDAD FEDERATIVA
cve_stats$cve_entidad_federativa <- as.numeric(cve_stats$cve_entidad_federativa) 
cve_stats <- cve_stats %>%
              arrange(cve_entidad_federativa)

#REORDER columns
col_order <- c("cve_entidad_federativa", "nom_entidad_federativa", "total", "total_rural","total_urban","with_latlon_prop","num_valid_lat_lon","latlon_rural","latlon_urban")
cve_stats <- cve_stats[, col_order, with = F]


View(cve_stats)

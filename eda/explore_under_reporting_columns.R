.packages  <-  c("plyr", "dplyr", "yaml", "data.table", "RPostgreSQL","ggplot2")
sapply(.packages, require, character.only = TRUE)

db_info <- yaml.load_file("../conf/db_profile.yaml")
conn_info <- yaml.load_file("../conf/connection.yaml")

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
                        options="-c search_path=semantic_underreport")

#SPECIFY SCHEMA in prior line and TABLE in following to use
p100 <- tbl(db_conn, "prospera_with_important_localities100")
p_homes <- p100 %>% as.data.table() %>% distinct_("home_id", .keep_all = TRUE) 



#SECTION ONE VARIABLES ( variables which are more similar to one another in terms of processing)

sec1 <- data.frame(matrix(
          c('n_rooms','verified_n_rooms', 
             'cook_sleep_same_room','verified_sleep_in_kitchen',
             'dirt_where_sleep_or_cook','verified_dirt_where_sleep_or_cook',
             'n_bedrooms','verified_n_bedrooms',
             'floor_material','verified_floor_material',
             'roof_material','verified_roof_material',
             'wall_material','verified_wall_material',
             'water_source', 'verified_water_source',
             'light_source', 'verified_light_source',
             'toilet','verified_toilet',
             'toilet_exclusively_residents','verified_toilet_exclusively_residents',
             'sewage_type','verified_sewage_type'), 
             ncol=2, nrow=12, byrow=T)
             )


f <- function(x, p_homes){ 
  cuis <- x[1];
  homev <- x[2]; 
  resname <- paste("diff",cuis,sep="_");
  criteria_pre <- paste("!is.na(",cuis,")",sep="");
  
  if( cuis %in% c('n_rooms', 'n_bedrooms', 'floor_material', 'roof_material','wall_material')){     
    #for these higher number is more than lower number '''
    criteria_under <- paste("(",cuis," < ",homev," )",sep="");
    criteria_over <- paste("(",cuis," > ",homev," )",sep="");
  }
  else{
    if( cuis %in% c('toilet','sewage_type','water_source')){      
      #for these lower numbers are better than higher numbers '''
      criteria_under <- paste("(",cuis," > ",homev," )",sep="");
      criteria_over <- paste("(",cuis," < ",homev," )",sep="");
    }
    else{
      if( cuis == 'light_source'){  
        #5 means no electricity, 1 - 4 are similar so group together '''
        criteria_under <- paste("(( ",cuis," == '5' ) & ( ",homev," != '5' ))",sep="");
        criteria_over <- paste("(( ",cuis," %in% c('1','2','3','4') ) & ( ",homev," == '5' ))",sep="");  
      }
      else{
        criteria_under <- paste("(( ",cuis," == F ) & ( ",homev," == T ))",sep="");
        criteria_over <- paste("(( ",cuis," == T ) & ( ",homev," == F ))",sep="");  
      }
    } 
  }
  
  under_num <- p_homes %>% filter_(criteria_pre) %>% filter_(criteria_under) %>% nrow();
  over_num <- p_homes %>% filter_(criteria_pre) %>% filter_(criteria_over) %>% nrow();
  num_not_null <-  p_homes %>% filter_(criteria_pre) %>% nrow();
  diff_num <- under_num + over_num
  true_num <- num_not_null - diff_num
  total <- true_num + diff_num
  total_with_nulls <- p_homes %>% nrow()
  null_nums <- total_with_nulls - total
  
  c(resname,diff_num, under_num, over_num, true_num, total, null_nums, total_with_nulls, 100 * (diff_num / total), 100 * (under_num / total), 100 * (null_nums/total_with_nulls));
}

results_sec1 <- apply(sec1, 1, f, p_homes  );
row.names(results_sec1) <- c("variable","total_num_diff","num_under_reporting","num_over_reporting","num_telling_truth","total_non_null","num_null","total_with_nulls","% diff", "% under report","% null")
colnames(results_sec1) <- results_sec1[1,]
results_sec1 <- results_sec1[-1,]



#SECTION 2 variables
sec2 <- data.frame(matrix(
          c('own_stove', 'verified_own_stove', 
            'own_refridgerator', 'verified_own_refridgerator',
            'own_microwave', 'verified_own_microwave',
            'own_washing_machine', 'verified_own_washing_machine', 
            'own_water_tank', 'verified_own_water_tank',
            'own_boiler', 'verified_own_water_heater',
            'own_tv', 'verified_own_tv',
            'own_tv_service',' verified_own_tv_network',
            'own_vhs', 'verified_own_video_player',
            'own_telephone', 'verified_own_telephone',
            'own_cell_phone', 'verified_own_cell_phone',
            'own_computer', 'verified_own_computer',
            'own_climate_control', 'verified_own_climate_control',
            'own_vehicle', 'verified_own_vehicle',
            'own_internet', 'verified_own_internet'), 
             ncol=2, nrow=15, byrow=T)
        )

f2 <- function(x, p_homes){ 
  cuis <- x[1];
  homev <- x[2]; 
  resname <- paste("diff",cuis,sep="_");
  
  criteria_pre <- paste("!is.na(",cuis,")");
  criteria_under <- paste("(!(",cuis," %in% c('11','12')) & ",homev,"== T )",sep="");
  criteria_over <-  paste("(",cuis," %in% c('11','12') & ",homev,"== F )",sep="");
  
  under_num <- p_homes %>% filter_(criteria_pre) %>% filter_(criteria_under) %>% nrow()
  over_num <- p_homes %>% filter_(criteria_pre) %>% filter_(criteria_over) %>% nrow()
  num_not_null <-  p_homes %>% filter_(criteria_pre) %>% nrow()
  diff_num <- under_num + over_num
  true_num <- num_not_null - diff_num
  total <- true_num + diff_num
  total_with_nulls <- p_homes %>% nrow()
  null_nums <- total_with_nulls - total
  
  c(resname,diff_num, under_num, over_num, true_num, total, null_nums, total_with_nulls, 100 * (diff_num / total), 100 * (under_num / total), 100 * (null_nums/total_with_nulls));
}

results_sec2 <- apply(sec2, 1, f2, p_homes  );
row.names(results_sec2) <- c("variable","total_num_diff","num_under_reporting","num_over_reporting","num_telling_truth","total_non_null","num_null","total_with_nulls","% diff", "% under report","% null")
colnames(results_sec2) <- results_sec2[1,]
results_sec2 <- results_sec2[-1,]

final_results <- cbind(results_sec1,results_sec2)

#FINAL RESULTS FOR EACH COLUMN
#View(final_results)


mres <- final_results %>% melt()
mres$value = as.numeric(as.character(mres$value))


#DIFFERENT GRAPHS YOU CAN RUN
#ggplot(mres,aes(x=Var2, y=value)) + geom_bar(aes(fill = Var1),stat='identity',position='dodge') 

#ggplot(mres,aes(x=Var1, y=value)) + geom_bar(stat='identity') + facet_wrap(~ Var2) + theme(axis.text.x = element_text(angle=90))

ggplot(mres,aes(x=Var2, y=value)) + geom_bar(stat='identity') + facet_wrap(~ Var1, scale="free_y") + theme(axis.text.x = element_text(angle=90)) 

ggplot(mres %>% filter(Var1 == 'num_telling_truth'), aes(x=Var2, y=value)) + geom_bar(stat='identity')  + theme(axis.text.x = element_text(angle=90))



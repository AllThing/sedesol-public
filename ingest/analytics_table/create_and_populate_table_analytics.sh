#./sh

#THIS IS THE SCRIPT TO RUN IN ORDER TO CREATE A HIGH LEVEL ANALYTICS TABLE
#OF THE TABLES AVAILABLE IN THE RAW SCHEMA.  IT IS FOR EDA PURPOSES.

#THERE ARE TWO POSSIBLE APPROACHES HERE
#A) IF YOU WANT TO GENERATE A NEW TABLE AND RE-RUN THE ANALYTICS
#1. CREATE table_analytics TABLE on RAW Schema
psql -U sedesol -W -f analytics_table_schema.sql

#2. GENERATE SQL TO POPULATE THE ANALYTIC QUERIES FOR THE TABLE
psql -U sedesol -W -f generate_table_inserts.sql > generated_table_outputs/all_inserts.sql

#3. PUT DATA IN DB
psql -U sedesol -W -f generated_table_outputs/all_inserts.sql

#OR
#B) USE THE EXISTING OUTPUT WHICH WAS GENERATED PRIOR
psql -U sedesol -W -f generated_table_outputs/all_inserts.sql


#AFTER ADD INDEXES TO TABLE FOR SPEED
psql -U sedesol -W -f add_indexes_to_table_after_data_inserted.sql

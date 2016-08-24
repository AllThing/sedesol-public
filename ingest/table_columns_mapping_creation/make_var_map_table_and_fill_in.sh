#./sh

#THIS SCRIPT ASSUMES YOU HAVE DEFINED YOUR POSTGRES CREDENTIALS AS AN ENVIRONMENT VARIABLE ALREADY
#CREATE SCHEMA FOR table_columns_dictionary
psql -U sedesol -W -f table_columns_dictionary.sql


#FILL IN WITH MAPPINGS FOR EACH TABLE/COLUMN WE WANT
cat LAYOUT_SIFODE_19.2_int_sorted.csv | psql -c "\copy raw.table_columns_dictionary from stdin with csv header;"
cat LAYOUT_SIFODE_19.2_univ_sorted.csv | psql -c "\copy raw.table_columns_dictionary from stdin with csv header;"

#TODO fill in info for other tables

#This file assumes 
#   1) all the csv files needed (one per table) are in the folder "csv"  
#   2) there exists a "schema" folder 
#
# It goes through each csv, creates the appropriate table in the raw schema, and then ingests the data for that table.

head csvs/ncv_agropecuario_2014_concil_2010_csv.csv | tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_agropecuario_2014 --no-constraints --no-inference > schema/ncv_agropecuario_2014_concil_2010_csv.sql &&
head csvs/ncv_concentrado_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_concentrado_2014 --no-constraints --no-inference > schema/ncv_concentrado_2014_concil_2010_csv.sql &&
head csvs/ncv_erogaciones_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_erogaciones_2014 --no-constraints --no-inference > schema/ncv_erogaciones_2014_concil_2010_csv.sql &&
head csvs/ncv_gastohogar_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_gastohogar_2014 --no-constraints --no-inference > schema/ncv_gastohogar_2014_concil_2010_csv.sql &&
head csvs/ncv_gastopersona_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_gastopersona_2014 --no-constraints --no-inference > schema/ncv_gastopersona_2014_concil_2010_csv.sql &&
head csvs/ncv_gastotarjetas_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_gastotarjetas_2014 --no-constraints --no-inference > schema/ncv_gastotarjetas_2014_concil_2010_csv.sql &&
head csvs/ncv_hogares_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_hogares_2014 --no-constraints --no-inference > schema/ncv_hogares_2014_concil_2010_csv.sql &&
head csvs/ncv_ingresos_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_ingresos_2014 --no-constraints --no-inference > schema/ncv_ingresos_2014_concil_2010_csv.sql &&
head csvs/ncv_noagropecuario_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_noagropecuario_2014 --no-constraints --no-inference > schema/ncv_noagropecuario_2014_concil_2010_csv.sql &&
head csvs/ncv_poblacion_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_poblacion_2014 --no-constraints --no-inference > schema/ncv_poblacion_2014_concil_2010_csv.sql &&
head csvs/ncv_trabajos_2014_concil_2010_csv.csv| tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_trabajos_2014 --no-constraints --no-inference > schema/ncv_trabajos_2014_concil_2010_csv.sql &&
head csvs/ncv_vivi_2014_concil_2010_csv.csv | tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables raw.inegi_vivi_2014 --no-constraints --no-inference > schema/ncv_vivi_2014_concil_2010_csv.sql;

psql -W -f schema/ncv_agropecuario_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_concentrado_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_erogaciones_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_gastohogar_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_gastopersona_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_gastotarjetas_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_hogares_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_ingresos_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_noagropecuario_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_poblacion_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_trabajos_2014_concil_2010_csv.sql &&
psql -W -f schema/ncv_vivi_2014_concil_2010_csv.sql;

cat csvs/ncv_agropecuario_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_agropecuario_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_concentrado_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_concentrado_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_erogaciones_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_erogaciones_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_gastohogar_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_gastohogar_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_gastopersona_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_gastopersona_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_gastotarjetas_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_gastotarjetas_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_hogares_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_hogares_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_ingresos_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_ingresos_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_noagropecuario_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_noagropecuario_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_poblacion_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_poblacion_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_trabajos_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_trabajos_2014 from stdin with csv header delimiter ';' ;" &&
cat csvs/ncv_vivi_2014_concil_2010_csv.csv | psql  -c "copy raw.inegi_vivi_2014 from stdin with csv header delimiter ';' ;";

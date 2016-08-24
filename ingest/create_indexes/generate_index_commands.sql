--THESE SQL COMMANDS CREATE THE SQL STATEMENTS WE NEED TO CREATE THE INDEXES
--1)The following command generates the file "create_indexes_on_raw_schema.sql" 
--which we can then run in the background against our db so that it concurrently creates the indexes

--  psql -U sedesol -W -f generate_index_commands.sql > create_indexes_on_raw_schema.sql

--2) After the INDEXES have been created we should VACUUM the tables

select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.coneval_pobreza(',column_name,');') as indexes from information_schema.columns where table_name='coneval_pobreza' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_domicilio(',column_name,');') as indexes from information_schema.columns where table_name='cuis_domicilio' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_encuesta(',column_name,');') as indexes from information_schema.columns where table_name='cuis_encuesta' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_integrante(',column_name,');') as indexes from information_schema.columns where table_name='cuis_integrante' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_persona(',column_name,');') as indexes from information_schema.columns where table_name='cuis_persona' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_se_integrante(',column_name,');') as indexes from information_schema.columns where table_name='cuis_se_integrante' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_se_vivienda(',column_name,');') as indexes from information_schema.columns where table_name='cuis_se_vivienda' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_visita_encuestador(',column_name,');') as indexes from information_schema.columns where table_name='cuis_visita_encuestador' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.cuis_vivienda(',column_name,');') as indexes from information_schema.columns where table_name='cuis_vivienda' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.geo_cuis_2(',column_name,');') as indexes from information_schema.columns where table_name='geo_cuis_2' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.geo_pub(',column_name,');') as indexes from information_schema.columns where table_name='geo_pub' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.imss_salario(',column_name,');') as indexes from information_schema.columns where table_name='imss_salario' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.manzanas(',column_name,');') as indexes from information_schema.columns where table_name='manzanas' order by column_name;
--select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.pub_sub(',column_name,');') as indexes from information_schema.columns where table_name='pub_sub' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.sifode_int(',column_name,');') as indexes from information_schema.columns where table_name='sifode_int' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.sifode_univ(',column_name,');') as indexes from information_schema.columns where table_name='sifode_univ' order by column_name;

select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.p160610_pub(',column_name,');') as indexes from information_schema.columns where table_name='p160610_pub' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.p160610_s072(',column_name,');') as indexes from information_schema.columns where table_name='p160610_s072' order by column_name;
select concat('CREATE INDEX CONCURRENTLY ',table_name,'_',column_name,'_ind ON raw.p160610_u005(',column_name,');') as indexes from information_schema.columns where table_name='p160610_u005' order by column_name;

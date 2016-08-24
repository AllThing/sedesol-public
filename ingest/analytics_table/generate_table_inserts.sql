--THIS CREATES ALL FROM THE raw schema, but also treats ids as categorical variables and unnecessarily creates too many.  SO GO BY EACH INDIVIDUAL TABLE 
--Select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name in ( SELECT table_name FROM information_schema.tables WHERE table_schema='raw' AND table_type='BASE TABLE' AND table_name <> 'table_analytics' order by table_name) order by table_name,column_name;

--CONEVAL_POBREZA table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='coneval_pobreza' AND column_name NOT IN ('folioviv','folio_prog_proy2','factor_hog','tamhogesc','ict','ictpc','prof_b1','prof_bm1','profun','int_pob','int_pobe','int_vulcar','int_caren') order by column_name;

--CUIS_DOMICILIO table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_domicilio' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','km_vial','m_vial','s_cve_localidad','s_nom_localidad','cve_localidad','fch_creacion','c_localidad','nom_localidad') order by column_name;


--CUIS_ENCUESTA table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_encuesta' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','folio_cuis','folio_prog_proy','folio_prog_proy2','folio_sedesol','fh_levanta','fch_creacion') order by column_name;


--CUIS_INTEGRANTE table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_integrante' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','id_mdm_p','fch_creacion') order by column_name;

--CUIS_PERSONA table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_persona' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','id_unico','id_mdm_p','s_fch_nacimiento','folio_sgei','fch_creacion') order by column_name;

--CUIS_SE_INTEGRANTE table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_se_integrante' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','id_mdm_p','fch_creacion','tipo_proy_esp','monto') order by column_name;


--CUIS_SE_VIVIENDA table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_se_vivienda' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','fch_creacion') order by column_name;

--CUIS_VISITA_ENCUESTADOR table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_visita_encuestador' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','cve_encuestador','fch_creacion','folio_ident','folio_edad','observaciones','nom_encuestador') order by column_name;


--CUIS_VIVIENDA table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='cuis_vivienda' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','fch_creacion','csc_hogar') order by column_name;

--SIFODE_INT table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='sifode_int' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','id_mdm_p','id_unico','ingreso_pc','p_total','s_cve_localidad') order by column_name;

--SIFODE_UNIV table analytics
select concat('INSERT INTO raw.table_analytics SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', column_name, ' as value, count(',column_name,') as value_count,(0.0+count(',column_name,'))/(Select count(*) from raw.',table_name,') as value_prop from raw.',table_name,' GROUP BY ',column_name,';') from information_schema.columns where table_name='sifode_univ' AND column_name NOT IN ('llave_hogar_h','id_mdm_h','id_mdm_p','id_unico','id_index','ingreso_pc','s_cve_localidad','s_nom_localidad','s_fch_nacimiento') order by column_name;

/*
--TODO: HANDLE INTEGER/DATES
--TODO: INCORPORATE DATASETS NOT USED INCLUDING..
--HOME VERIFICATION INCOMPLETE..
--GEO_CUIS SKIP.. OLD
--GEO_CUIS_2  .. SHAPEFILES
--IMSS SALARIO SKIP..
--MANZANAS SKIP .. SHAPEFILES
--PARTIAL CENSUS SKIP
--PUB_SUB SKIP .. INCOMPLETE
--SEPOMEX SKIP .. NOT USING
*/

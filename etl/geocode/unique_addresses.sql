-- This is a script to create a table that has unique addresses
-- from both SIFODE (geo_cuis) and PUB (geo_pub) tables
-- to avoid replicating queries
-- It is aggregated to consider possible duplicates even within the tables
-- For example, for a lot of people we only have info up to locality
-- Also matches cve's with cve_mapping table to ensure that 
-- we are using the most recent names for places.
--
-- The result table should look like this:
--  address_id 	| pub_id | cuis_id 	| ent 		| mun | loc | asen | nom_vial | nomref1 | nomref2 	| address_id	| num_ext 	| postal_code
-- 	1 			| null 	|  11,24	| queretaro | ... 										... 	| 1				| 178		| 01010	
-- 	1 			| 1114 	| 476 		| oaxaca	| ... 										... 	| 2 			| 123		| 99999	

-- for when we run the google query: change the id column type to numeric so we can constrain on it

-- ALTER TABLE raw.geo_cuis_2 ALTER COLUMN id_dom_uni TYPE numeric using (id_dom_uni::numeric);
-- ALTER TABLE raw.geo_pub ALTER COLUMN id_registr TYPE numeric using (id_registr::numeric);

-- The script normalizes to uppercase, cleans "ninguno", "sn", "sin numero" (etc)
-- erases accents
	-- for this it needs the next extension: CREATE EXTENSION unaccent;
-- and takes only the first four words for street names via regex


-------------------------------TABLE CREATION---------------------------------------
drop table if exists raw.addresses;

create table raw.addresses (
	address serial,	-- serial
	pub_id int[],
	cuis_id int[],
	ent varchar,
	mun varchar,
	loc varchar,
	asen varchar,
	vial varchar,
	vial_1 varchar,
	vial_2 varchar,
	num_ext varchar,
	postal_code varchar
);
------------------------------------------------------------------------------------

insert into raw.addresses (pub_id,cuis_id,ent,mun,loc,asen,vial,vial_1,vial_2,num_ext,postal_code)
-- explain analyze
select  array_agg(pub_id) as pub_id,array_agg(cuis_id) as cuis_id,
		ent,
		mun,
		loc,
		asen,
		vial,
		vial_1,
		vial_2,
		num_ext,
		postal_code
from
(
		(
		select	pub_id,cuis_id,
			-- all these cases are to merge the columns - sort of what a union would do
			-- but we're not using a union because we want both pub and cuis ids
			-- and non replicated addresses
				case when pub_cve_entidad_federativa is not null then pub_cve_entidad_federativa
					else cuis_cve_entidad_federativa
					end as cve_entidad_federativa,
				case when pub_cve_municipio is not null then pub_cve_municipio
					else cuis_cve_municipio
					end as cve_municipio,
				case when pub_cve_localidad is not null then pub_cve_localidad
					else cuis_cve_localidad
					end as cve_localidad,
				case when pub_asen is not null then pub_asen
					else cuis_asen
					end as asen,
				case when pub_vial is not null then pub_vial
					else cuis_vial
					end as vial,
				case when pub_vial_1 is not null then pub_vial_1
					else cuis_vial_1
					end as vial_1,
				case when pub_vial_2 is not null then pub_vial_2
					else cuis_vial_2
					end as vial_2,
				case when pub_postal_code is not null then pub_postal_code
					else cuis_postal_code
					end as postal_code,
				case when pub_num_ext is not null then pub_num_ext
					else cuis_num_ext
					end as num_ext
		from (
				(-- 
				SELECT	id_registr::numeric::int as pub_id, -- the ::num::int is because it has some .000000 at the end (?)
						unaccent(cve_ent) as pub_cve_entidad_federativa, -- everything is unaccented
				 		unaccent(cve_munici) as pub_cve_municipio,
				 		unaccent(cve_locali) as pub_cve_localidad,
				 		unaccent(upper(nomasen)) as pub_asen,
				 		-- for streets and intersections, sometimes there is no data but not left null
				 		case when (nomvial not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO')
				 				and nomvial not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nomvial, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as pub_vial,
				 		case when (nomref1 not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO')
				 				and nomref1 not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nomref1, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as pub_vial_1,
				 		case when (nomref2 not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO') 
				 				and nomref2 not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nomref2, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as pub_vial_2,	
				 		cp as pub_postal_code,	
				 		-- we take the best possible information with both numeric and alphanumeric
				 		case when (numextnum1 is not null and numextnum1 != '0') then numextnum1
				 		 	 when (numextalf1 not in ('SN','NINGUNO','DOMICILIO CONOCIDO','SIN NUMERO','DOMICILIO CONOCIDO')
				 					and numextalf1 not LIKE '%MANZ%' and numextalf1 not LIKE '%MZ%'
				 					and numextalf1 not like '%LOT%') then numextalf1
				 			else NULL
				 			end pub_num_ext
				 	FROM raw.geo_pub
				 	where id_registr != 'ID_REGISTR' -- there was a problem with ingesting csvs and there are rows which are the header
				 	-- and nomasen is null and nomvial is null and nomref1 is null and nomref2 is null -- erase, constraint for testing
				 	-- limit 500 -- erase, constraint for testing: number of rows from pub
				) pub
			full join 	
			 	(
			--- this is an example query for pub
				select	id_dom_uni::numeric::int as cuis_id,
						unaccent(cve_ent) as cuis_cve_entidad_federativa,
						unaccent(cve_munc) as cuis_cve_municipio,
						unaccent(cve_locc) as cuis_cve_localidad,
						unaccent(upper(nomasen)) as cuis_asen,
						-- for streets and intersections, sometimes there is no data but not left null
				 		case when (nom_vial not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO')
				 				and nom_vial not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nom_vial, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as cuis_vial,
				 		case when (nomref1 not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO')
				 				and nomref1 not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nomref1, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as cuis_vial_1,
				 		case when (nomref2 not in ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO') 
				 				and nomref2 not like '% SABE%') then unaccent(upper(array_to_string((regexp_split_to_array(nomref2, E'\\s+'))[1:4],' ')))
				 			else null
				 			end as cuis_vial_2,	
				 		cp as cuis_postal_code,
				 		-- we take the best possible information with both numeric and alphanumeric
				 		case when (numextnum is not null and numextnum != '0') then numextnum
				 		 	 when (numextalf not in ('SN','NINGUNO','DOMICILIO CONOCIDO','SIN NUMERO')
				 					and numextalf not LIKE '%MANZ%' and numextalf not LIKE '%MZ%'
				 					and numextalf not like '%LOT%') then numextalf
				 			else NULL
				 			end cuis_num_ext
				 	from raw.geo_cuis_2
				 	-- where nomasen is null and nom_vial is null and nomref1 is null and nomref2 is null --erase, constraint for testing
				 	-- limit 500 -- erase, constraint for testing: number of rows from cuis
				) cuis
			on pub.pub_cve_entidad_federativa = cuis.cuis_cve_entidad_federativa
			and pub.pub_cve_municipio = cuis.cuis_cve_municipio
			and pub.pub_cve_localidad = cuis.cuis_cve_localidad
			and pub.pub_asen = cuis.cuis_asen
			and pub.pub_vial = cuis.cuis_vial
			and pub.pub_vial_1 = cuis.cuis_vial_1
			and pub.pub_vial_2 = cuis.cuis_vial_2
			and pub.pub_postal_code = cuis.cuis_postal_code
			and pub.pub_num_ext = cuis.cuis_num_ext) joined_table
		-- where pub_id is not null and cuis_id is not null -- erase, constraint to look for addresses on both tables
		-- limit 5 -- erase, constraint for testing
		) unique_addresses
left join 	
		(
		-- this creates a catalogue from cve -> names 
		select 	municipalities.cve_ent as cve_entidad_federativa,
			unaccent(upper(municipalities.nom_ent)) as ent,
			concat(municipalities.cve_ent,municipalities.cve_mun) as cve_municipio,
			unaccent(upper(municipalities.nom_mun)) as mun,
			concat(localities.cve_ent,localities.cve_mun,localities.cve_loc) as cve_localidad,
			unaccent(upper(localities.nom_loc)) as loc
		from 		raw.cve_mapping_localidads localities
		inner join 	raw.cve_mapping_municipalities municipalities
			on localities.cve_ent = municipalities.cve_ent
			and localities.cve_mun = municipalities.cve_mun
		) cve_nom_calatogue	
on 	unique_addresses.cve_entidad_federativa = cve_nom_calatogue.cve_entidad_federativa
and unique_addresses.cve_municipio = cve_nom_calatogue.cve_municipio
and unique_addresses.cve_localidad = cve_nom_calatogue.cve_localidad
)
group by ent, mun,loc,asen,vial,vial_1,vial_2,num_ext,postal_code
-- ) -- if we want to run any of the previous commands
;
	

------------------ testing playground -------------------
--- anything below this line is not part of the code ----

-- for getting the first four words of the street string
-- select array_to_string((regexp_split_to_array(nomvial, E'\\s+'))[1:4],' ') FROM raw.geo_pub limit 25;






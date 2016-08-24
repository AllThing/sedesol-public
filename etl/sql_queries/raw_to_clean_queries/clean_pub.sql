/*This script takes the three PUB tables: p160610_pub p160610_s072 p160610_u005

Notable cleaning transformations:
	Table `pub` inherits to `pub_prospera`, `pub_seguro_popular` and `pub_others `
	Trims spaces around the values of many variables
	Quantitative to numeric
	Binary to boolean
	Eliminate replicated column
	Give readable column names*/

/*------------------------------Parent Table------------------------------*/
/*	Created as an empty table but queries to this table should spread
 	to all three of its children tables */

CREATE TABLE clean.pub (
/* cve_pad  same variable as cd_programa in all three tables*/
sifode_pub_id varchar
,registry_id varchar /* do we ever use this? */
,home_id varchar /* do we ever use this? */
,cuis_ps_id varchar /* do we ever use this? */
,cuis_sedesol_id varchar /* do we ever use this? */
,benefit_period varchar
,date_of_survey timestamp
,gender varchar
,state_of_birth varchar
,marital_status varchar
,institution_id varchar /* we don't have a dictionary for this
							it might be the physical place in which they get the benefit
							eg which Liconsa store, or where they pick up their money */
,program_id varchar
,intra_program_id varchar /* we don't have a dictionary for this, appears non useful */
,subprogram_id varchar /* we don't have a dictionary for this, appears non useful
							it is also a very messy column, has stuff like 
							'SD 02' 'TRANSICION ESCUELA-TRABAJO' 'CONCURSOS DE ARTE POPULAR' '158'*/
,payment_entity varchar
,payment_municipality varchar
,payment_locality varchar
,benefit_type varchar
,registry_holder boolean
,family_relation varchar
,beneficiary_type varchar
,number_of_benefits numeric
,benefit_description varchar
,payment_amount numeric
,payment_month varchar
,locality_id varchar
,municipality_id varchar
,state_id varchar
);

/*--------------------------Creating Children Tables--------------------------*/
/*      Note the INHERITS (pub) at the end of every one*/

CREATE TABLE clean.pub_prospera () INHERITS (clean.pub);
CREATE TABLE clean.pub_seguro_popular () INHERITS (clean.pub);
CREATE TABLE clean.pub_others () INHERITS (clean.pub);

/*-------------------------Populating Children Tables-------------------------*/

insert into clean.pub_prospera ( SELECT
	btrim(id_unico) 								as sifode_pb_id
	,btrim(id_registro)								as registry_id
	,btrim(id_hogar)								as home_id
	,btrim(id_cuis_ps)								as cuis_ps_id
	,btrim(id_cuis_sedesol) 						as cuis_sedesol_id
	,periodo 										as benefit_period
	,TO_TIMESTAMP(fh_levantamiento, 'DD-Mon-YY')	as date_of_survey	
	,(case cd_sexo 	when 'H' then 'male'
					when 'M' then 'female'
					else null
					end)							as gender  /*there are ' ' responses*/
	,cd_edo_nac										as state_of_birth
	,cast(case when cd_edo_civil IN ('-1','0') then null
				else cd_edo_civil
				end as numeric)						as marital_status /*there are "-1" and "0" values that don't map to anything*/
	,cd_institucion									as institution_id
	,btrim(cd_programa)								as program_id
	,btrim(cd_intraprograma)						as intra_program_id
	,btrim(nb_subprograma)							as subprogram_id
	,cd_ent_pago									as payment_entity
	,cd_mun_pago									as payment_municipality
	,cd_loc_pago									as payment_locality
	,cd_tp_beneficio								as benefit_type
	,cast(case when btrim(in_titular) ='1' then true 
				else false
				end as boolean)						as registry_holder
	,cast(case when cd_parentesco = '-1' then null
				else cd_parentesco
				end  as numeric)					as family_relation /*there is a "-1" value that doesn't map to anything*/
	,btrim(cd_tp_ben_det)							as beneficiary_type
	,cast(case when btrim(nu_beneficios)  = '' then null
			else btrim(nu_imp_monetario) 
			end as numeric)					as number_of_benefits
	,btrim(cd_beneficio)							as benefit_description
	,cast(case when btrim(nu_imp_monetario) = '' then null
			else btrim(nu_imp_monetario) 
			end as numeric)		as payment_amount
	,btrim(nu_mes_pago)								as payment_month
	,cve_loc										as locality_id
	,cve_mun										as municipality_id
	,cve_ent										as state_id
FROM raw.p160610_s072
);


insert INTO clean.pub_seguro_popular ( SELECT
	btrim(id_unico) 								as sifode_pb_id
	,btrim(id_registro)								as registry_id
	,btrim(id_hogar)								as home_id
	,btrim(id_cuis_ps)								as cuis_ps_id
	,btrim(id_cuis_sedesol) 						as cuis_sedesol_id
	,periodo 										as benefit_period
	,TO_TIMESTAMP(fh_levantamiento, 'DD-Mon-YY')	as date_of_survey	
	,(case cd_sexo 	when 'H' then 'male'
					when 'M' then 'female'
					else null
					end)							as gender  /*there are ' ' responses*/
	,cd_edo_nac										as state_of_birth
	,cast(case when cd_edo_civil IN ('-1','0') then null
				else cd_edo_civil
				end as numeric)						as marital_status /*there are "-1" and "0" values that don't map to anything*/
	,cd_institucion									as institution_id
	,btrim(cd_programa)								as program_id
	,btrim(cd_intraprograma)						as intra_program_id
	,btrim(nb_subprograma)							as subprogram_id
	,cd_ent_pago									as payment_entity
	,cd_mun_pago									as payment_municipality
	,cd_loc_pago									as payment_locality
	,cd_tp_beneficio								as benefit_type
	,cast(case when btrim(in_titular) ='1' then true 
				else false
				end as boolean)						as registry_holder
	,cast(case when cd_parentesco = '-1' then null
				else cd_parentesco
				end  as numeric)					as family_relation /*there is a "-1" value that doesn't map to anything*/
	,btrim(cd_tp_ben_det)							as beneficiary_type
	,cast(case when btrim(nu_beneficios)  = '' then null
			else btrim(nu_imp_monetario) 
			end as numeric)					as number_of_benefits
	,btrim(cd_beneficio)							as benefit_description
	,cast(case when btrim(nu_imp_monetario) = '' then null
			else btrim(nu_imp_monetario) 
			end as numeric)		as payment_amount
	,btrim(nu_mes_pago)								as payment_month
	,cve_loc										as locality_id
	,cve_mun										as municipality_id
	,cve_ent										as state_id
FROM raw.p160610_u005
);



insert into  clean.pub_others ( SELECT
	btrim(id_unico) 								as sifode_pb_id
	,btrim(id_registro)								as registry_id
	,btrim(id_hogar)								as home_id
	,btrim(id_cuis_ps)								as cuis_ps_id
	,btrim(id_cuis_sedesol) 						as cuis_sedesol_id
	,periodo 										as benefit_period
	,TO_TIMESTAMP(fh_levantamiento, 'DD-Mon-YY')	as date_of_survey	
	,(case cd_sexo 	when 'H' then 'male'
					when 'M' then 'female'
					else null
					end)							as gender  /*there are ' ' responses*/
	,cd_edo_nac										as state_of_birth
	,cast(case when cd_edo_civil IN ('-1','0') then null
				else cd_edo_civil
				end as numeric)						as marital_status /*there are "-1" and "0" values that don't map to anything*/
	,cd_institucion									as institution_id
	,btrim(cd_programa)								as program_id
	,btrim(cd_intraprograma)						as intra_program_id
	,btrim(nb_subprograma)							as subprogram_id
	,cd_ent_pago									as payment_entity
	,cd_mun_pago									as payment_municipality
	,cd_loc_pago									as payment_locality
	,cd_tp_beneficio								as benefit_type
	,cast(case when btrim(in_titular) ='1' then true 
				else false
				end as boolean)						as registry_holder
	,cast(case when cd_parentesco = '-1' then null
				else cd_parentesco
				end  as numeric)					as family_relation /*there is a "-1" value that doesn't map to anything*/
	,btrim(cd_tp_ben_det)							as beneficiary_type
	,cast(case when btrim(nu_beneficios)  = '' then null
			else btrim(nu_beneficios) 
			end as numeric)					as number_of_benefits
	,btrim(cd_beneficio)							as benefit_description
	,cast(case when btrim(nu_imp_monetario) = '' then null
			else btrim(nu_imp_monetario) 
			end as numeric)		as payment_amount
	,btrim(nu_mes_pago)								as payment_month
	,cve_loc										as locality_id
	,cve_mun										as municipality_id
	,cve_ent										as state_id
FROM raw.p160610_pub
);


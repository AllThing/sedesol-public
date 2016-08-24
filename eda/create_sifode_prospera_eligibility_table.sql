-- This file takes all the persons from the table raw.sifode_int, joins them with information from raw.cuis_encuesta and raw.cuis_se_integrante,
-- and then determines if the individual was eligible for inclusion in PROSPERA at the time of the survey based on their age and other criteria 
-- and then finally checks that eligibility against whether they are currently in prospera ( or not ) for possible inclusion/exclusion errors.
--
with t3 as 
(select si.s_cve_entidad_federativa, si.s_cve_municipio, si.s_cve_localidad, si.llave_hogar_h,si.id_mdm_h, si.id_mdm_p, si.s_c_cd_sexo, si.pob_lbm, si.edad_actual, si.s_fch_nacimiento, ce.fh_levanta, si.in_prosp,
((to_date(ce.fh_levanta,'DD/MM/YY') - to_date(si.s_fch_nacimiento,'DD/MM/YY')) / 365) as age_at_survey 
from 
(select s_cve_entidad_federativa, s_cve_municipio, s_cve_localidad, llave_hogar_h, id_mdm_h, id_mdm_p, s_c_cd_sexo, pob_lbm,in_prosp, edad_actual, s_fch_nacimiento 
from raw.sifode_int order by llave_hogar_h) si
join (select llave_hogar_h, id_mdm_h, fh_levanta from raw.cuis_encuesta order by llave_hogar_h ) ce 
on si.llave_hogar_h = ce.llave_hogar_h and si.id_mdm_h = ce.id_mdm_h),
t4 as 
(select t3.s_cve_entidad_federativa, t3.s_cve_municipio, t3.s_cve_localidad, t3.llave_hogar_h, t3.id_mdm_h, t3.id_mdm_p, to_date(t3.fh_levanta,'DD/MM/YY') as survey_date, t3.age_at_survey, t3.s_c_cd_sexo, t3.pob_lbm, csi.asis_esc , 
case when age_at_survey < 5 and t3.pob_lbm = '1' then true 
     when age_at_survey < 22 and csi.asis_esc = '1' and t3.pob_lbm = '1' then true
     when age_at_survey > 14 and age_at_survey < 50 and t3.s_c_cd_sexo = '2' and t3.pob_lbm = '1' then true
     when age_at_survey > 64 and t3.pob_lbm = '1' then true
     else false
end as was_eligible_for_prospera_at_time_of_survey,
case when age_at_survey < 5 and t3.pob_lbm = '1' then 'child' 
     when age_at_survey < 22 and csi.asis_esc = '1' and t3.pob_lbm = '1' then 'student'
     when age_at_survey > 14 and age_at_survey < 50 and t3.s_c_cd_sexo = '2' and t3.pob_lbm = '1' then 'reproductive'
     when age_at_survey > 64 and t3.pob_lbm = '1' then 'older'
     else 'not eligible'
end as reason_for_eligibility,
t3.in_prosp
from 
t3 join (select llave_hogar_h, id_mdm_h, id_mdm_p, asis_esc  from raw.cuis_se_integrante order by llave_hogar_h ) csi 
on t3.llave_hogar_h = csi.llave_hogar_h and t3.id_mdm_h = csi.id_mdm_h and t3.id_mdm_p = csi.id_mdm_p),
t5 as
(select *, 
case when was_eligible_for_prospera_at_time_of_survey = true and in_prosp = '0' then '1' else '0' end as target_for_inclusion_in_prospera,
case when was_eligible_for_prospera_at_time_of_survey = false and in_prosp = '1' then '1' else '0' end as possible_inclusion_error_person_level 
from t4),
t6 as (
select *, min(possible_inclusion_error_person_level) over (PARTITION BY llave_hogar_h) as possible_inclusion_error_household_level from t5)
select * into raw.prospera_sifode_eligibility from t6 ;

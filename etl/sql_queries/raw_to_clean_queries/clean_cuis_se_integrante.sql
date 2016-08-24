/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_se_integrante is present, and
 * defines a new table with the following cleaning transformations,
 *  - Removes columns already present in the SIFODE universal table
 *  - Transforms binary fields to BOOLEAN
 *  - Converts quantitative fields to NUMERIC
 *  - Gives more descriptive column names
 */

CREATE TABLE clean.cuis_se_integrante AS
SELECT llave_hogar_h AS home_id,
       id_mdm_h AS family_id,
       id_mdm_p AS person_id,

       /* basic work info */
       c_instsal_a AS medical_provider,
       c_afilsal_a AS work_affilation,

       /* education related data */
       c_ult_nivel AS education_level,
       c_ult_gra AS education_grade,

       CAST(
       CASE WHEN asis_esc = '1' THEN TRUE
       WHEN asis_esc = '2' THEN FALSE
       WHEN asis_esc = ' ' THEN NULL
       END AS BOOLEAN) AS attends_school,

       c_aban_escu AS reason_left_school,
       c_con_tra AS work_situation,
       c_ver_con_trab AS  job_search,
       c_peri_tra AS work_period,
       c_mot_notr AS reason_not_work_all_year,

       /* work related data */
       CAST(
       CASE WHEN trab_subor = '1' THEN TRUE
       WHEN trab_subor = '2' THEN FALSE
       WHEN trab_subor = ' ' THEN NULL
       END AS BOOLEAN) AS has_a_work_supervisor,

       CAST(
       CASE WHEN trab_indep = '1' THEN TRUE
       WHEN trab_indep = '2' THEN FALSE
       WHEN trab_indep = ' ' THEN NULL
       END AS BOOLEAN) AS independent_work,

       /* work benefits */
       CAST(
       CASE WHEN trab_presta_a = '0' THEN FALSE
       WHEN trab_presta_a = '1' THEN TRUE
       WHEN trab_presta_a = ' ' THEN NULL
       END AS BOOLEAN) AS have_sick_leave,

       CAST(
       CASE WHEN trab_presta_b = '0' THEN FALSE
       WHEN trab_presta_b = '1' THEN TRUE
       WHEN trab_presta_b = ' ' THEN NULL
       END AS BOOLEAN) AS have_sar,

       CAST(
       CASE WHEN trab_presta_c = '0' THEN FALSE
       WHEN trab_presta_c = '1' THEN TRUE
       WHEN trab_presta_c = ' ' THEN NULL
       END AS BOOLEAN) AS have_housing_credit,

       CAST(
       CASE WHEN trab_presta_d = '0' THEN FALSE
       WHEN trab_presta_d = '1' THEN TRUE
       WHEN trab_presta_d = ' ' THEN NULL
       END AS BOOLEAN) AS have_daycare_support,

       CAST(
       CASE WHEN trab_presta_e = '0' THEN FALSE
       WHEN trab_presta_e = '1' THEN TRUE
       WHEN trab_presta_e = ' ' THEN NULL
       END AS BOOLEAN) AS have_bonus,

       CAST(
       CASE WHEN trab_presta_f = '0' THEN FALSE
       WHEN trab_presta_f = '1' THEN TRUE
       WHEN trab_presta_f = ' ' THEN NULL
       END AS BOOLEAN) AS have_life_insurance,

       CAST(
       CASE WHEN trab_presta_g = '0' THEN FALSE
       WHEN trab_presta_g = '1' THEN TRUE
       WHEN trab_presta_g = ' ' THEN NULL
       END AS BOOLEAN) AS have_no_benefits,

       CAST(
       CASE WHEN trab_presta_h = '0' THEN FALSE
       WHEN trab_presta_h = '1' THEN TRUE
       WHEN trab_presta_h = ' ' THEN NULL
       END AS BOOLEAN) AS benefits_no_response,

       CAST(
       CASE WHEN trab_no_re = '0' THEN FALSE
       WHEN trab_no_re = '1' THEN TRUE
       END AS BOOLEAN) AS received_work_pay,

       /* self-reported income info */
       CAST(
       CASE WHEN monto = ' ' THEN NULL
       WHEN monto = '99999' THEN NULL
       ELSE monto
       END AS NUMERIC) AS income_self_reported,

       CAST(
       CASE WHEN c_periodo = ' ' THEN NULL
       ELSE c_periodo
       END AS NUMERIC) AS income_period,

       /* insurance related fields */
       CAST(
       CASE WHEN seg_volunt_a = '1' THEN TRUE
       WHEN seg_volunt_a = '0' THEN FALSE
       WHEN seg_volunt_a = ' ' THEN NULL
       END AS BOOLEAN) AS have_sar_afore_insurance,

       CAST(
       CASE WHEN seg_volunt_b = '1' THEN TRUE
       WHEN seg_volunt_b = '0' THEN FALSE
       WHEN seg_volunt_b = ' ' THEN NULL
       END AS BOOLEAN) AS have_private_insurance,

       CAST(
       CASE WHEN seg_volunt_c = '1' THEN TRUE
       WHEN seg_volunt_c = '0' THEN FALSE
       WHEN seg_volunt_c = ' ' THEN NULL
       END AS BOOLEAN) AS have_life_insurance_voluntarios,

       CAST(
       CASE WHEN seg_volunt_d = '1' THEN TRUE
       WHEN seg_volunt_d = '0' THEN FALSE
       WHEN seg_volunt_d = ' ' THEN NULL
       END AS BOOLEAN) AS have_disability_insurance,

       CAST(
       CASE WHEN seg_volunt_e = '1' THEN TRUE
       WHEN seg_volunt_e = '0' THEN FALSE
       WHEN seg_volunt_e = ' ' THEN NULL
       END AS BOOLEAN) AS have_other_insurance,

       CAST(
       CASE WHEN seg_volunt_f = '1' THEN TRUE
       WHEN seg_volunt_f = '0' THEN FALSE
       WHEN seg_volunt_f = ' ' THEN NULL
       END AS BOOLEAN) AS no_aforementioned_insurance,

       CAST(
       CASE WHEN seg_volunt_g = '1' THEN TRUE
       WHEN seg_volunt_g = '0' THEN FALSE
       WHEN seg_volunt_g = ' ' THEN NULL
       END AS BOOLEAN) AS no_response_insurance,

       /* retirement / senior related fields */
       jubilado AS retired,

       CAST(
       CASE WHEN jubilado_1 = '1' THEN TRUE
       WHEN jubilado_1 = '0' THEN FALSE
       WHEN jubilado_1 = ' ' THEN NULL
       END AS BOOLEAN) AS pension_from_abroad,

       inapam AS has_inapam_card,

       CAST(
       CASE WHEN am_a = '1' THEN TRUE
       WHEN am_a = '0' THEN FALSE
       WHEN am_a = ' ' THEN NULL
       END AS BOOLEAN) AS receives_pension_adultos_mayores,

       CAST(
       CASE WHEN am_b = '1' THEN TRUE
       WHEN am_b = '0' THEN FALSE
       WHEN am_b = ' ' THEN NULL
       END AS BOOLEAN) AS receives_prospera_adultos_mayores,

       CAST(
       CASE WHEN am_c = '1' THEN TRUE
       WHEN am_c = '0' THEN FALSE
       WHEN am_c = ' ' THEN NULL
       END AS BOOLEAN) AS receives_other_adultos_mayores,

       CAST(
       CASE WHEN am_d = '1' THEN TRUE
       WHEN am_d = '0' THEN FALSE
       WHEN am_d = ' ' THEN NULL
       END AS BOOLEAN) AS receives_no_adultos_mayores,

       CAST(
       CASE WHEN am_e = '1' THEN TRUE
       WHEN am_e = '0' THEN FALSE
       WHEN am_e = ' ' THEN NULL
       END AS BOOLEAN) AS no_response_adultos_mayores,

       /* diseases related fields */
       CAST(
       CASE WHEN enf_art = '1' THEN TRUE
       WHEN enf_art = '2' THEN FALSE
       WHEN enf_art = ' ' THEN NULL
       END AS BOOLEAN) AS has_arthritis,

       CAST(
       CASE WHEN enf_can = '1' THEN TRUE
       WHEN enf_can = '2' THEN FALSE
       WHEN enf_can = ' ' THEN NULL
       END AS BOOLEAN) AS has_cancer,

       CAST(
       CASE WHEN enf_cir = '1' THEN TRUE
       WHEN enf_cir = '2' THEN FALSE
       WHEN enf_cir = ' ' THEN NULL
       END AS BOOLEAN) AS has_cirrhosis,

       CAST(
       CASE WHEN enf_ren = '1' THEN TRUE
       WHEN enf_ren = '2' THEN FALSE
       WHEN enf_ren = ' ' THEN NULL
       END AS BOOLEAN) AS has_renal_disease,

       CAST(
       CASE WHEN enf_dia = '1' THEN TRUE
       WHEN enf_dia = '2' THEN FALSE
       WHEN enf_dia = ' ' THEN NULL
       END AS BOOLEAN) AS has_diabetes,

       CAST(
       CASE WHEN enf_cor = '1' THEN TRUE
       WHEN enf_cor = '2' THEN FALSE
       WHEN enf_cor = ' ' THEN NULL
       END AS BOOLEAN) AS has_heart_disease,

       CAST(
       CASE WHEN enf_pul = '1' THEN TRUE
       WHEN enf_pul = '2' THEN FALSE
       WHEN enf_pul = ' ' THEN NULL
       END AS BOOLEAN) AS has_lung_disease,

       CAST(
       CASE WHEN enf_vih = '1' THEN TRUE
       WHEN enf_vih = '2' THEN FALSE
       WHEN enf_vih = ' ' THEN NULL
       END AS BOOLEAN) AS has_hiv,

       CAST(
       CASE WHEN enf_def = '1' THEN TRUE
       WHEN enf_def = '2' THEN FALSE
       WHEN enf_def = ' ' THEN NULL
       END AS BOOLEAN) AS has_anemia,

       CAST(
       CASE WHEN enf_hip = '1' THEN TRUE
       WHEN enf_hip = '2' THEN FALSE
       WHEN enf_hip = ' ' THEN NULL
       END AS BOOLEAN) AS has_hypertension,

       CAST(
       CASE WHEN enf_obe = '1' THEN TRUE
       WHEN enf_obe = '2' THEN FALSE
       WHEN enf_obe = ' ' THEN NULL
       END AS BOOLEAN) AS has_obesity

       /* intentionally omit disability columns present in SIFODE */

FROM raw.cuis_se_integrante;

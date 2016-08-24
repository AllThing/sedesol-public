/* Script to create a semantic database from a set of cleaned tables. The
 * semantic database is the single "merged" database combining all data used
 * for the various machine learning tasks in the SEDESOL project. We join the
 * data to the "transaction" level -- individual CUIS surveys or PUB payments.
 * That is, the same individual can appear multiple times.
 *
 * The input of this script is a collection of databases, specifically,
 *
 *	- clean.cuis_persona
 * 	- clean.cuis_encuesta
 * 	- clean.cuis_se_vivienda
 * 	- clean.cuis_se_integrante
 * 	- clean.sifode_univ
 * 	- clean.home_verification
 * 	- raw.encaseh
 * 	- raw.imss_salario
 */

CREATE TABLE semantic_schema.semantic_table AS
(
WITH 
pub AS 
(SELECT sifode_pub_id,
        registry_id,
        cuis_ps_id,
        cuis_sedesol_id,
        benefit_period,
        date_of_survey,
        institution_id,
        program_id,
        intra_program_id,
        subprogram_id,
        payment_entity,
        payment_municipality,
        payment_locality,
        benefit_type,
        registry_holder,
        beneficiary_type,
        number_of_benefits,
        benefit_description,
        payment_amount,
        payment_month
FROM clean.pub),

cuis_encuesta AS 
(SELECT home_id,
        family_id,
        process_id,
        process_type,
        survey_origin,
        survey_start_time,
        survey_end_time
FROM clean.cuis_encuesta),

imss_renamed AS 
(SELECT llave_hogar_h AS home_id,
     	id_mdm_h AS family_id,
	    id_mdm_p AS person_id,
	    salario_imss AS imss_salary
FROM raw.imss_salario
),

all_cuis_joined AS 
(
/* persona table (19232771 people) */
SELECT * FROM clean.cuis_persona
LEFT JOIN
/* join with the viviendas table (6184704 families) */
clean.cuis_se_vivienda
USING (home_id, family_id)
LEFT JOIN
/* join with the cuis encuesta table (6184704 families) */
cuis_encuesta
USING (home_id, family_id)
LEFT JOIN
/* join with the integrante table (6184704 families) */
clean.cuis_se_integrante
USING (home_id, family_id, person_id)
),

/* join PUB with SIFODE integrante (18030723 individuals) */
pub_sifode_int_join AS
(
SELECT * FROM pub
FULL OUTER JOIN
clean.sifode_int
USING (sifode_pub_id)
),

/* join with SIFODE universal (19232771 individuals) */
pub_sifode_int_sifode_univ_join AS
(
SELECT * FROM pub_sifode_int_join
FULL OUTER JOIN
clean.sifode_univ
USING (home_id, family_id, person_id)
),

/* join with all CUIS (19232771 individuals) */
pub_sifode_int_sifode_univ_cuis_join AS
(
SELECT * FROM pub_sifode_int_sifode_univ_join
FULL OUTER JOIN
all_cuis_joined
USING (home_id, family_id, person_id, sifode_pub_id)
),

/* join homeverification with encash */
home_verification_and_encaseh AS
(
SELECT 
en.llave_hogar_h as home_id,
hv.*
FROM raw.encaseh AS en
INNER JOIN clean.home_verification AS hv 
USING (folio, folio_enca)
),

/* join with home verification_encaseh_joined */
pub_sifode_int_sifode_univ_cuis_hv_join AS
(
SELECT * FROM
pub_sifode_int_sifode_univ_cuis_join
LEFT JOIN 
home_verification_and_encaseh
USING (home_id)
),

/* join with indice de pobreza */
pub_sifode_int_sifode_univ_cuis_hv_idrs_join AS
(
SELECT * FROM
pub_sifode_int_sifode_univ_cuis_hv_join
LEFT JOIN 
ingestion_result.indice_de_rezago_social
USING (municipality_id)
),

/* join with IMSS (154417 individuals, 148300 families) after renaming their fields */
pub_sifode_int_sifode_univ_cuis_hv_idrs_imss_join AS
(
SELECT * FROM
(
pub_sifode_int_sifode_univ_cuis_hv_idrs_join
LEFT JOIN 
imss_renamed    
USING (home_id, family_id, person_id)
)
)

/* join with manzana IDs */
SELECT * FROM
pub_sifode_int_sifode_univ_cuis_hv_idrs_imss_join
LEFT JOIN
clean.manzana_link
USING (sifode_pub_id));

/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_visita_encuestador is present, and
 * defines a new table with just the fields that are relevant for
 * subsequent analysis.
 */

CREATE TABLE clean.cuis_visita_encuestador AS
SELECT llave_hogar_h AS home_id,
       id_mdm_h AS family_id,
       cve_encuestador AS surveyor_id,
       info_adec AS qualified_respondent,
       observaciones AS surveyor_notes
FROM raw.cuis_visita_encuestador

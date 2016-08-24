/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_persona is present, and
 * defines a new table, which just extracts IDs to link across the
 * other CUIS and PUB.
 */

CREATE TABLE clean.cuis_persona AS
SELECT llave_hogar_h as home_id,
       id_mdm_h as family_id,
       id_mdm_p as person_id,
       id_unico as sifode_pub_id

       /* omit basic demographics, since they are in sifode */

FROM raw.cuis_persona;

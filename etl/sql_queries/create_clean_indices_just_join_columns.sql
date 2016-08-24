/* This script creates indices on the columns used to join the tables in
 * the clean.* schema. This join is defined in join in create_semantic_table.sql
 * Each block of code corresponds to one table in the clean schema.
 */

DROP INDEX IF EXISTS clean.clean_pub_sifode_pub_id_idx;
DROP INDEX IF EXISTS clean.clean_pub_home_id_idx;
DROP INDEX IF EXISTS clean.clean_pub_locality_id_idx;

DROP INDEX IF EXISTS clean.clean_sifode_int_sifode_pub_id_idx;
DROP INDEX IF EXISTS clean.clean_sifode_int_home_id_idx;
DROP INDEX IF EXISTS clean.clean_sifode_int_locality_id_idx;

DROP INDEX IF EXISTS clean.clean_sifode_univ_home_id_idx;
DROP INDEX IF EXISTS clean.clean_sifode_univ_family_id_idx;
DROP INDEX IF EXISTS clean.clean_sifode_univ_person_id_idx;

DROP INDEX IF EXISTS clean.clean_cuis_encuesta_home_id_idx;
DROP INDEX IF EXISTS clean.clean_cuis_encuesta_family_id_idx;

DROP INDEX IF EXISTS clean.clean_cuis_se_vivienda_home_id_idx;
DROP INDEX IF EXISTS clean.clean_cuis_se_vivienda_family_id_idx;

DROP INDEX IF EXISTS clean.clean_cuis_se_integrante_home_id_idx;
DROP INDEX IF EXISTS clean.clean_cuis_se_integrante_family_id_idx;
DROP INDEX IF EXISTS clean.clean_cuis_se_integrante_person_id_idx;

/* assuming encaseh, imss, and manzana_link are indexed on raw.raw! */
DROP INDEX IF EXISTS raw.raw_encaseh_folio_enca_idx;
DROP INDEX IF EXISTS clean.clean_home_verification_folio_enca_idx;

DROP INDEX IF EXISTS raw.raw_imss_salario_home_id_idx;
DROP INDEX IF EXISTS raw.raw_imss_salario_family_id_idx;
DROP INDEX IF EXISTS raw.raw_imss_salario_person_id_idx;

DROP INDEX IF EXISTS clean.clean_manzana_link_pub_id_idx;


CREATE INDEX clean_pub_sifode_pub_id_idx ON clean.pub(sifode_pub_id);
CREATE INDEX clean_pub_home_id_idx ON clean.pub(home_id);
CREATE INDEX clean_pub_locality_id_idx ON clean.pub(locality_id);

CREATE INDEX clean_sifode_int_sifode_pub_id_idx ON clean.sifode_int(sifode_pub_id);
CREATE INDEX clean_sifode_int_home_id_idx ON clean.sifode_int(home_id);
CREATE INDEX clean_sifode_int_locality_id_idx ON clean.sifode_int(locality_id);

CREATE INDEX clean_sifode_univ_home_id_idx ON clean.sifode_univ(home_id);
CREATE INDEX clean_sifode_univ_family_id_idx ON clean.sifode_univ(family_id);
CREATE INDEX clean_sifode_univ_person_id_idx ON clean.sifode_univ(person_id);

CREATE INDEX clean_cuis_encuesta_home_id_idx ON clean.cuis_encuesta(home_id);
CREATE INDEX clean_cuis_encuesta_family_id_idx ON clean.cuis_encuesta(family_id);

CREATE INDEX clean_cuis_se_vivienda_home_id_idx ON clean.cuis_se_vivienda(home_id);
CREATE INDEX clean_cuis_se_vivienda_family_id_idx ON clean.cuis_se_vivienda(family_id);

CREATE INDEX clean_cuis_se_integrante_home_id_idx ON clean.cuis_se_integrante(home_id);
CREATE INDEX clean_cuis_se_integrante_family_id_idx ON clean.cuis_se_integrante(family_id);
CREATE INDEX clean_cuis_se_integrante_person_id_idx ON clean.cuis_se_integrante(person_id);

/* assuming encaseh, imss, and manzana_link are indexed on raw! */
CREATE INDEX raw_encaseh_folio_enca_idx ON raw.encaseh(folio_enca);
CREATE INDEX clean_home_verification_folio_enca_idx ON clean.home_verification(folio_enca);

CREATE INDEX raw_imss_salario_home_id_idx ON raw.imss_salario(llave_hogar_h);
CREATE INDEX raw_imss_salario_family_id_idx ON raw.imss_salario(id_mdm_h);
CREATE INDEX raw_imss_salario_person_id_idx ON raw.imss_salario(id_mdm_p);

CREATE INDEX clean_manzana_link_pub_id_idx ON clean.manzana_link(sifode_pub_id);

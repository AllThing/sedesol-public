/* These queries create indices for the ENIGH data to facilitate the
 * subsequent join to the semantic level. We first drop all the indices
 * to ensure idempotency in the pipeline (otherwise, running the pipeline
 * twice would cause
 * an error. */

DROP INDEX IF EXISTS clean.gastohogar_folioviv_idx;
DROP INDEX IF EXISTS clean.gastohogar_foliohog_idx;
DROP INDEX IF EXISTS clean.gastopersona_folioviv_idx;
DROP INDEX IF EXISTS clean.gastopersona_foliohog_idx;
DROP INDEX IF EXISTS clean.gastopersona_numren_idx;
DROP INDEX IF EXISTS clean.ingresos_folioviv_idx;
DROP INDEX IF EXISTS clean.ingresos_foliohog_idx;
DROP INDEX IF EXISTS clean.ingresos_numren_idx;
DROP INDEX IF EXISTS clean.poblacion_folioviv_idx;
DROP INDEX IF EXISTS clean.poblacion_foliohog_idx;
DROP INDEX IF EXISTS clean.poblacion_numren_idx;
DROP INDEX IF EXISTS clean.trabajos_folioviv_idx;
DROP INDEX IF EXISTS clean.trabajos_foliohog_idx;
DROP INDEX IF EXISTS clean.trabajos_numren_idx;
DROP INDEX IF EXISTS clean.vivi_folioviv_idx;
DROP INDEX IF EXISTS clean.vivi_locality_id_idx;
DROP INDEX IF EXISTS clean.localidades_locality_id_idx;

CREATE INDEX gastohogar_folioviv_idx ON clean.enigh_gastohogar(folioviv);
CREATE INDEX gastohogar_foliohog_idx ON clean.enigh_gastohogar(foliohog);
CREATE INDEX gastopersona_folioviv_idx ON clean.enigh_gastopersona(folioviv);
CREATE INDEX gastopersona_foliohog_idx ON clean.enigh_gastopersona(foliohog);
CREATE INDEX gastopersona_numren_idx ON clean.enigh_gastopersona(numren);
CREATE INDEX ingresos_folioviv_idx ON clean.enigh_ingresos(folioviv);
CREATE INDEX ingresos_foliohog_idx ON clean.enigh_ingresos(foliohog);
CREATE INDEX ingresos_numren_idx ON clean.enigh_ingresos(numren);
CREATE INDEX poblacion_folioviv_idx ON clean.enigh_poblacion(folioviv);
CREATE INDEX poblacion_foliohog_idx ON clean.enigh_poblacion(foliohog);
CREATE INDEX poblacion_numren_idx ON clean.enigh_poblacion(numren);
CREATE INDEX trabajos_folioviv_idx ON clean.enigh_trabajos(folioviv);
CREATE INDEX trabajos_foliohog_idx ON clean.enigh_trabajos(foliohog);
CREATE INDEX trabajos_numren_idx ON clean.enigh_trabajos(numren);
CREATE INDEX vivi_folioviv_idx ON clean.enigh_vivi(folioviv);
CREATE INDEX vivi_locality_id_idx ON clean.enigh_vivi(locality_id);
CREATE INDEX localidades_locality_id_idx ON clean.localidades(locality_id);

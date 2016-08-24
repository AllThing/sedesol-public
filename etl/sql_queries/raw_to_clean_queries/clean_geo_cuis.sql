/* This script cleans the table raw.geo_cuis_2 as a part of the SEDESOL
 * pipeline. The main simplifications are,
 *     - Only keep locality ids, rather than names (and names of
 *       municipalities, states, etc., since these can be recovered
 * 	 from the INEGI shapefiles.
 *     - Street names or addresses that are essentially marked as NULL
 *     	 should in fact be converted to NULL.
 */

CREATE TABLE clean.geo_cuis AS
SELECT REPLACE(CAST(id_dom_uni AS VARCHAR), '.0000000000', '') AS geo_id,
       cve_locc AS locality_id,
       CASE WHEN cp NOT IN ('0', '00000', '0000000') THEN cp
       	    ELSE NULL
	    END AS postal_code,
       nomasen AS asentimiento_name,

	/* house number information */
       CASE WHEN numextnum NOT IN ('0', ' ') THEN numextnum
	    ELSE NULL
	    END AS address_number,
       CASE WHEN numextalf NOT IN ('0', 'SN', 'SIN NUMERO', ' ', 'MANZANA', 'DOMICLIO CONOCIDO', 'NINGUNO', 'MZ', 'NUMERO', 'LOTE', 'MZA') THEN numextalf
       	    ELSE NULL
	    END AS address_name,
       CASE WHEN numintalf NOT IN ('0', 'SN', ' ', 'DOMICILIO CONOCIDO', 'LOTE', 'NINGUNO', 'LT', 'NA', '00', 'INTERIOR', 'CASA', 'SIN NUMERO', 'DEPAERTMENTO', 'NO', 'CERO', 'NINGUNA', 'NIN', 'NT', 'MANZANA', 'DOMIC', 'DEPTO', 'LTE', 'NUMERO', 'CASA HABITACION', 'SIN', 'MZ', 'SIN DATO', 'DEP') THEN numintalf
       	    ELSE NULL
	    END AS interior_address_name,

       /* side street information */
       CASE WHEN
       	    (nom_vial NOT IN ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO', ' ', 'SIN NOMBRE', 'CONOCIDO') AND
	     nom_vial NOT LIKE '% SABE%')
	THEN nom_vial
	ELSE NULL
	END AS street_name,

       CASE WHEN
       	    (nomref1 NOT IN ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO', ' ', 'SIN NOMBRE', 'CONOCIDO') AND
	     nomref1 NOT LIKE '% SABE%')
	THEN nomref1
	ELSE NULL
	END AS side_street_1,

       CASE WHEN
       	    (nomref2 NOT IN ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO', ' ', 'SIN NOMBRE', 'CONOCIDO') AND
	     nomref2 NOT LIKE '% SABE%')
	THEN nomref2
	ELSE NULL
	END AS side_street_2,

       CASE WHEN
       	    (nomref3 NOT IN ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO', ' ', 'SIN NOMBRE', 'CONOCIDO') AND
	     nomref3 NOT LIKE '% SABE%')
	THEN nomref3
	ELSE NULL
	END AS back_street

FROM (SELECT * FROM raw.geo_cuis_2 LIMIT 100) test;

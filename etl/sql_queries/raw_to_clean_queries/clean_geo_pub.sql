/* This script cleans the table raw.geo_pub as a part of the SEDESOL
 * pipeline. The main simplifications are,
 *     - Only keep locality ids, rather than names (and names of
 *       municipalities, states, etc., since these can be recovered
 * 	 from the INEGI shapefiles.
 *     - Street names or addresses that are essentially marked as NULL
 *     	 should in fact be converted to NULL.
 */

/* convert to numeric, round, and convert back */

CREATE TABLE clean.geo_pub AS
SELECT REPLACE(id_registr, '.0000000000', '') AS geo_id,
       cve_locali AS locality_id,
       CASE WHEN cp NOT IN ('0', '00000', '0000000') THEN cp
       	    ELSE NULL
	    END AS postal_code,
       nomasen AS asentimiento_name,

	/* house number information */
       CASE WHEN numextnum1 NOT IN ('0', ' ') THEN numextnum1
       	    WHEN numextnum2 NOT IN ('0', ' ') THEN numextnum2
	    ELSE NULL
	    END AS address_number,
       CASE WHEN numextalf1 NOT IN ('0', 'SN', 'SIN NUMERO', ' ', 'MANZANA', 'DOMICLIO CONOCIDO', 'NINGUNO', 'MZ', 'NUMERO', 'LOTE', 'MZA') THEN numextalf1
       	    ELSE NULL
	    END AS address_name,
       CASE WHEN numintalf NOT IN ('0', 'SN', ' ', 'DOMICILIO CONOCIDO', 'LOTE', 'NINGUNO', 'LT', 'NA', '00', 'INTERIOR', 'CASA', 'SIN NUMERO', 'DEPAERTMENTO', 'NO', 'CERO', 'NINGUNA', 'NIN', 'NT', 'MANZANA', 'DOMIC', 'DEPTO', 'LTE', 'NUMERO', 'CASA HABITACION', 'SIN', 'MZ', 'SIN DATO', 'DEP') THEN numintalf
       	    ELSE NULL
	    END AS interior_address_name,

       /* side street information */
       CASE WHEN
       	    (nomvial NOT IN ('SIN INFORMACION','NA','SIN REFERENCIA','NINGUNO','DOMICILIO CONOCIDO', ' ', 'SIN NOMBRE', 'CONOCIDO') AND
	     nomvial NOT LIKE '% SABE%')
	THEN nomvial
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

FROM raw.geo_pub;

/* This script creates a new manzanas database containing both the
 * rural and urban manzanas data. Specifically, it assumes tables
 * raw.manzanas_rurales and raw.manzanas_urbanas containing the
 * original INEGI manzanas level data, and then merges them into a 
 * raw.manzanas_transform table.
 *
 * Further, it converts the coordinate system from the original INEGI 
 * system (which we can see by looking at the original INEGI .prj
 * files) into the WGS standard, 4326.
 */ 

/* Combine the rural and urban manzanas shapefiles */
SELECT *
INTO raw.manzanas_transform
FROM (
     SELECT cve_mzac, geom
     FROM raw.manzanas_rurales
     UNION
     SELECT cve_mzac, geom
     FROM raw.manzanas_urbanas
) manz;

/* Convert the coordinate reference system for this table to WGS */
ALTER TABLE raw.manzanas_transform
      ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326)
      	    USING ST_Transform(
	    	  ST_SetSRID(geom, 8624),
		  4326);

/* Create indices to speed up future spatial computations */
CREATE INDEX manzana_idx
ON raw.manzanas_transform (cve_mzac);

CREATE INDEX manzana_geom_idx
ON raw.manzanas_transform
USING GIST (geom);

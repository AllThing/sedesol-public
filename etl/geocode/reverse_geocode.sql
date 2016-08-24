/* This script takes the lat / lons resulting from a google geocoding
 * experiment and reverse geocodes these lat / lons to manzana level using
 * shapefiles available from INEGI and prepared using spatial_transform.sql.
 * The three main steps are: (1) parse the JSON strings encoding the geocoding
 * results, (2) expand the rows in raw.addresses such that each cuis/pub ID has
 * a row in the resulting table and join it with geocoding results, and (3)
 * join the results of step (2) with known shapefile information.
 *
 * We assume the presence of the following tables,
 *    (1) raw.geocoding_results: A table whose rows are JSON strings for
 *        each address.
 *    (2) clean.manzanas: A table containing manzana geom
 *        objects, transformed to the WGS coordinate system.
 *    (3) raw.addresses: This is needed to associate the address_id field in
 *        raw.geocoding_results with the corresponding cuis/pub geo_id
 */
 /*
 * Export the resulting geocode_manzanas table to a csv using the following
 * command:
 * psql -A -F ',' -h localhost -p 8427 -U sedesol -d sedesol -W -F',' -c 'select * from raw.geocode_manzanas;' > 'geocode_manzanas.csv'
 * 
 */

DROP TABLE IF EXISTS raw.geocode_table;

CREATE TABLE raw.geocode_table AS
(
WITH 
geocode_results AS
(
    SELECT 
		   address_id AS results_search_id,
		   google_query_result#>'{query_results,0}'->'pos'->>'lat' AS lat,
	       google_query_result#>'{query_results,0}'->'pos'->>'lng' AS long,
	       google_query_result#>'{query_results,0}'->'geo_names'->>'administrative_area_level_1' AS state,
	       google_query_result#>'{query_results,0}'->'geo_names'->>'administrative_area_level_2' AS municipality,
	       google_query_result#>'{query_results,0}'->'geo_names'->>'locality' AS locality,
	       google_query_result#>'{query_results,0}'->'geo_names'->>'postal_code' AS postal_code,
	       google_query_result#>'{query_results,0}'->'geo_names'->>'political' AS sublocality,
	       google_query_result->'query_input'#>>'{search,0}' AS search_string,
	       google_query_result#>'{query_results,0}'->'quality'#>'{types,0}' AS result_type,
	       google_query_result#>'{query_results,0}'->'quality'->>'geocoding_failed' AS failed
	FROM raw.geocoding_results),
	
/* transform lat/long and drop the rows where geocoding failed */
geocode_results_transformed AS
(
SELECT ST_SetSRID(ST_MakePoint(CAST (geocode_results.long AS REAL), CAST (geocode_results.lat AS REAL)), 4326) AS geocode_geom,
geocode_results.*
FROM
geocode_results
WHERE 
failed IS NULL AND lat IS NOT NULL 
),

cuis_ids_expanded_addresses AS
(
SELECT *,
address AS search_id,
unnest(cuis_id) AS geo_id,
'cuis'::text AS geo_source
FROM raw.addresses 
),

pub_ids_expanded_addresses AS
(
SELECT *,
address AS search_id,
unnest(pub_id) AS geo_id,
'pub'::text AS geo_source
FROM raw.addresses 
),

all_addresses AS 
(
(SELECT 
* 
FROM cuis_ids_expanded_addresses
WHERE geo_id IS NOT NULL) 
UNION ALL
(SELECT
* 
FROM pub_ids_expanded_addresses
WHERE geo_id IS NOT NULL)
)

SELECT 
all_addresses.*,
lat,
long,
search_string,
result_type,
geocode_geom
FROM
geocode_results_transformed
LEFT JOIN 
all_addresses
ON geocode_results_transformed.results_search_id = all_addresses.search_id

);

DROP TABLE IF EXISTS raw.geocode_result;

CREATE INDEX clean_manzanas_geom_idx ON clean.manzanas USING GIST(geom);
CREATE INDEX geocode_manzanas_geom_idx ON raw.geocode_table USING GIST(geocode_geom);

CREATE TABLE raw.geocode_manzanas AS
(
/* get all manzana IDs within a certain distance of a geocode geocoded lat / lon */
WITH closest_manzanas AS 
(
SELECT
search_id,
geo_id,
geo_source,
manzana_id,
result_type, 
ST_Distance(manzana.geom, geocode.geocode_geom) AS manzana_dist
FROM 
raw.geocode_table AS geocode
LEFT JOIN 
clean.manzanas AS manzana
ON ST_DWithin(manzana.geom, geocode.geocode_geom, 0.001)
),

ranked_closest_manzanas AS
(
SELECT 
*,
rank() OVER (PARTITION BY search_id ORDER BY manzana_dist) AS manzana_rank
FROM closest_manzanas
)

SELECT 
geo_id,
geo_source,
manzana_id,
result_type,
manzana_dist
FROM ranked_closest_manzanas 
WHERE manzana_rank = 1 
);

/* Drop the temporary tables */
DROP TABLE raw.geocode_table;

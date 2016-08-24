/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_vivienda is present, and
 * defines a new table with the following cleaning transformations,
 *  - Transforms binary fields to BOOLEAN
 *  - Converts quantitative fields to NUMERIC
 *  - Gives more descriptive column names
 */

CREATE TABLE clean.cuis_vivienda AS
SELECT llave_hogar_h AS home_id,
       id_mdm_h AS family_id,

       CAST(
	CASE WHEN tot_per_viv = ' ' THEN NULL
	ELSE tot_per_viv
	END AS NUMERIC) AS n_people_in_household,

	CAST(
	CASE WHEN tot_hog = ' ' THEN NULL
	ELSE tot_hog
	END AS NUMERIC) AS n_families_in_household,

	CAST(
	CASE WHEN tot_per = ' ' THEN NULL
	ELSE tot_per
	END AS NUMERIC) AS n_people_in_family,

	CAST(
	CASE WHEN per_gasto = ' ' THEN NULL
	WHEN per_gasto = '1' THEN TRUE
	ELSE FALSE
	END AS BOOLEAN) AS household_shares_expenses,

	CAST(
	CASE WHEN per_alim = ' ' THEN NULL
	WHEN per_alim = '1' THEN TRUE
	ELSE FALSE
	END AS BOOLEAN) AS household_shares_food

       FROM raw.cuis_vivienda;

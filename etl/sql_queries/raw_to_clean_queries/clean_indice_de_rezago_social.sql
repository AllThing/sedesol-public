/* This query belongs to the raw --> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.indice_de_rezago_social is present, and
 * defines a new table, which just extracts converts the second column to numeric
 */

CREATE TABLE clean.indice_de_rezago_social AS
SELECT municipality_id,indice_de_rezago_social::numeric FROM raw.indice_de_rezago_social;

/* This subsets the large semantic table to just those individuals
 * for whom the IMSS income is available, and saves the results in
 * a new table in the semantic schema, called imss. */

CREATE TABLE semantic_schema.semantic_subset AS
SELECT * FROM
semantic_schema.semantic_table
WHERE imss_salary IS NOT NULL;

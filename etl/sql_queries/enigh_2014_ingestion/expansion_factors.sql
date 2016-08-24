/* Duplicate rows of the ENIGH table according to the expansion factors
 *
 * The way the ENIGH census data is available, we don't actually have
 * measurements for every single household that they surveyed. Instead, they
 * give data for representative households, along with an "expansion factor"
 * for how many households that row represents. While this compresses the data
 * representation, it makes it hard to perform statistical analysis. While many
 * methods can directly take weights for each sample, an easier approach is to
 * duplicate each row for the number of households it represents. This is what
 * this script accomplishes.
 *
 * Naively expanding would result in a huge table, because some rows would be
 * expanded thousands of times. Recognizing that the real importance of the
 * expansion factors is in reweighting rows, we round and divide the factors so
 * they are small numbers with similar relative sizes to one another. These
 * rounded expansion factors are what we use in the final row duplication.
 *
 * We heavily rely on a trick from: http://stackoverflow.com/questions/10423767/sql-repeat-a-result-row-multiple-times-and-number-the-rows
 */

CREATE TABLE semantic_schema.semantic_subset AS
WITH expansion_ix AS (
     /* array must go up to at least the maximum rounded expansion factor */
     SELECT generate_series(1, 128) AS expansion_ix
     )
SELECT *
FROM
semantic_schema.semantic_table AS semantic_table
LEFT JOIN
expansion_ix
ON CEIL(semantic_table.vivi_factor_viv / 1000) >= expansion_ix.expansion_ix
ORDER BY folioviv;

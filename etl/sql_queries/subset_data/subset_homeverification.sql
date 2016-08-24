/* This subsets the large semantic table to just those individuals
 * for whom homeverification is available, and saves the results in
 * a new table in the semantic schema */
*/
CREATE TABLE semantic_schema.semantic_subset AS
WITH under_prospera AS
(
  SELECT * 
  FROM semantic_schema.semantic_table
  WHERE folio IS NOT NULL AND folio_enca IS NOT NULL
),
t1 AS 
( 
  SELECT locality_id, count(*) AS locnum 
  FROM under_prospera
  GROUP BY locality_id ORDER BY count(*) desc 
)
SELECT 
       p.*, 
       CASE WHEN t1.locnum >= 100 then t1.locnum else 0 end as important_locality_id 
       FROM under_prospera p JOIN t1 USING (locality_id);

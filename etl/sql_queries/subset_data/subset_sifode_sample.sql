CREATE TABLE semantic.sifode_sample_100000 AS
SELECT * 
FROM semantic.semantic_full
WHERE person_id::int between 25461047 and (25461047 + 100000);

/* Ideally we should not have to hard code the person_id range. We should have
 * something along the lines of:

CREATE TABLE semantic.sifode_sample_100000 AS
SELECT * 
FROM semantic.semantic_full
WHERE person_id::int between min(person_id::int) and min(person_id::int) + 100000;

But the min() function doesn't work in a WHERE clause
*/

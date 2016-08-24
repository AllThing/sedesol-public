CREATE TABLE semantic.sifode AS
SELECT * 
FROM semantic.semantic_full
WHERE person_id is NOT NULL;

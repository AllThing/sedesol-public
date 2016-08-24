/* initialize schema for updated table */
DROP TABLE IF EXISTS semantic_schema.subset_table_indexed;
CREATE TABLE semantic_schema.subset_table_indexed AS
SELECT *
FROM semantic_schema.subset_table
LIMIT 0;

/* drop column if they already exist */
ALTER TABLE semantic_schema.subset_table_indexed
DROP COLUMN IF EXISTS row_number;

ALTER TABLE semantic_schema.subset_table_indexed
DROP COLUMN IF EXISTS cv_index;

/* Create columns row number and cv index columns */
ALTER TABLE semantic_schema.subset_table_indexed
ADD COLUMN row_number bigserial;

ALTER TABLE semantic_schema.subset_table_indexed
ADD COLUMN cv_index INT;

/* rename back to original table */
INSERT INTO semantic_schema.subset_table_indexed
SELECT *
FROM semantic_schema.subset_table
cv_order_condition;

DROP TABLE semantic_schema.subset_table;
ALTER TABLE semantic_schema.subset_table_indexed RENAME TO subset_table;

/* Create indices on these columns, for faster retrieval later */
CREATE INDEX subset_table_row_number_idx ON semantic_schema.subset_table(row_number);


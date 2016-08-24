 /* First, load some experimental text data to the raw database */
DROP TABLE IF EXISTS raw.geocode_json;
CREATE TABLE raw.geocode_json (
       geocode_text VARCHAR
);

-- psql (not SQL) command --
-- \copy raw.geocode_json FROM '/home/ksankara/google_experiment_jul4.txt'

/* transform characters to valid JSON */
UPDATE raw.geocode_json
SET geocode_text = REPLACE(geocode_text, 'NaN', '"NaN"');

/* Convert this experimental data to type json */
ALTER TABLE raw.geocode_json
ALTER COLUMN geocode_text
TYPE JSON
USING geocode_text::JSON;

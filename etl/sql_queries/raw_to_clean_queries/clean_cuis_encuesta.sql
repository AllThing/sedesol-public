/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_encuesta is present, and
 * defines a new table with the following cleaning transformations,
 *  - Removes columns already present in the SIFODE universal table
 *  - Transforms binary fields to BOOLEAN
 *  - Converts quantitative fields to NUMERIC
 *  - Gives more descriptive column names
 */

CREATE TABLE clean.cuis_encuesta AS
SELECT  home_id,
	family_id,
	process_id,
	process_type,
	survey_origin,
	program_id,
	survey_start_time,

	/* We have to be careful in declaring timestamp for end of survey. In
	 * some cases, it looks like hora_term is measured on a 12 hour clock,
	 * but hora_init is in a 24 clock. Also, we are using the date from
	 * the survey_date field, but sometimes the end of the survey crossed
	 * into the next day.
	 */
	CASE WHEN survey_end_time > survey_start_time THEN survey_end_time
	WHEN survey_end_time < survey_start_time AND survey_end_time + INTERVAL '12 hours' > survey_start_time
	THEN survey_end_time + INTERVAL '12 hours'
	WHEN survey_end_time < survey_start_time AND survey_end_time + INTERVAL '24 hours' > survey_start_time
	THEN survey_end_time + INTERVAL '24 hours'
	ELSE NULL END AS survey_end_time
FROM (

     SELECT llave_hogar_h AS home_id,
     	    id_mdm_h AS family_id,

       	    /* survey background */
       	    folio_prog_proy AS process_id,
       	    c_tipo_proc AS process_type,
       	    c_origen_encuesta AS survey_origin,
       	    c_programa_social AS program_id,

       	    /* convert cleaned dates + times to datetime types */
       	    CASE WHEN hora_ini_clean IS NULL THEN NULL
       	    ELSE TO_TIMESTAMP(survey_date || ' ' || hora_ini_clean, 'DD/MM/YYYY HH24:MI')
       	    END AS survey_start_time,

       	    CASE WHEN hora_term_clean IS NULL THEN NULL
       	    ELSE TO_TIMESTAMP(survey_date || ' ' || hora_term_clean, 'DD/MM/YYYY HH24:MI')
       	    END AS survey_end_time

       	    FROM (

       	    	 SELECT *,

	      	 /* set all dates outside of 2010s to null (these seem like errors) */
              	 CASE WHEN SUBSTR(fh_levanta, 7, 3) != '201' THEN NULL
       	      	 ELSE SUBSTR(fh_levanta, 1, 10) END
	      	 AS survey_date,

	      	 /* clean the start hour to HH:MM */
       	      	 CASE WHEN hora_ini = ' ' THEN NULL
       	      	 WHEN STRPOS(hora_ini, ':') = 0 AND CHAR_LENGTH(hora_ini) = 2 THEN CONCAT(hora_ini, ':00')
       	      	 WHEN CHAR_LENGTH(hora_ini) = 1 THEN CONCAT('0', CONCAT(hora_ini, ':00')) /* convert things like 5 to 05:00 */
       	      	 WHEN substr(hora_ini, 3, 1) = ':' AND CHAR_LENGTH(hora_ini) = 4 THEN CONCAT('0', REVERSE(hora_ini)) /* takes things like 74:2 to 02:47 */
       	      	 WHEN substr(hora_ini, 3, 1) != ':' AND CHAR_LENGTH(hora_ini) = 4 THEN CONCAT('0', hora_ini) /* takes things like 5:00 to 05:00 */
       	      	 WHEN CHAR_LENGTH(hora_ini) = 2 THEN CONCAT(hora_ini, ':00')
       	      	 WHEN STRPOS(hora_ini, ': ') != 0 THEN REPLACE(hora_ini, ': ', ':0') /* times like 09: 0 go to 09:00 */
       	      	 WHEN substr(hora_ini, 5, 1) = ':' THEN NULL /* one end time is '1 10:' */
       	      	 ELSE hora_ini END AS hora_ini_clean,

	      	 /* clean the end hour to HH:MM */
       	      	 CASE WHEN hora_term = ' ' THEN NULL
       	      	 WHEN STRPOS(hora_term, 'Q') != 0 THEN NULL /* typos in some have Qs in date fields */
       	      	 WHEN STRPOS(hora_term, ':') = 0 AND CHAR_LENGTH(hora_term) = 2 THEN CONCAT(hora_term, ':00')
       	      	 WHEN CHAR_LENGTH(hora_term) = 1 THEN CONCAT('0', CONCAT(hora_term, ':00')) /* convert things like 5 to 05:00 */
       	      	 WHEN substr(hora_term, 3, 1) = ':' AND CHAR_LENGTH(hora_term) = 4 THEN CONCAT('0', REVERSE(hora_term)) /* takes things like 74:2 to 02:47 */
       	      	 WHEN substr(hora_term, 3, 1) != ':' AND CHAR_LENGTH(hora_term) = 4 THEN CONCAT('0', hora_term) /* takes things like 5:00 to 05:00 */
       	      	 WHEN CHAR_LENGTH(hora_term) = 2 THEN CONCAT(hora_term, ':00')
       	      	 WHEN STRPOS(hora_term, ': ') != 0 THEN REPLACE(hora_term, ': ', ':0') /* times like 09: 0 go to 09:00 */
       	      	 WHEN substr(hora_term, 5, 1) = ':' THEN NULL /* one end time is '1 10:' */
       	      	 ELSE hora_term END
       	      	 AS hora_term_clean
       	      	 FROM raw.cuis_encuesta
		 ) cleaned_hora_cuis
     ) cleaned_times

/* This script cleans and combines the the raw.localidades_rurales and
 * raw.localidades_urbanas during the SEDESOL pipeline. It is almost
 * exactly the same as the one that cleans manzanas, except the word
 * localidades has been substituted everywhere. The input are SQL
 * databases containing locality shapefiles and data from INEGI, one
 * database each for rural and urban locations. The output has
 * essentially three differences from the input,
 *    - It combines rural and urban data
 *    - It renames columns to give more detail on the census output
 *    - It converts the spatial reference from to WGS (4236)
 */

CREATE TABLE clean.localidades AS
SELECT 	*,
	ST_Y(ST_CENTROID(geom)) AS latitude,
	ST_X(ST_CENTROID(geom)) AS longitude
FROM (
     SELECT
            cve_locc AS locality_id,
            ST_Transform(ST_SetSRID(geom, 8624), 4236) AS geom,
	    nom_loc AS locality_name,
	    nom_mun AS municipality_name,
	    nom_ent AS state_name,
	    TRUE AS urban,

	    /* age and gender demographics */
	    CAST(pobtot AS NUMERIC) AS population,
	    CAST(pobfem AS NUMERIC) AS population_female,
	    CAST(p_0a2 AS NUMERIC) AS population_age_0_to_2,
	    CAST(p_0a2_f AS NUMERIC) AS population_age_0_to_2_female,
	    CAST(p_3ymas AS NUMERIC) AS population_age_greater_3,
	    CAST(p_3ymas_f AS NUMERIC) AS population_age_greater_3_female,
	    CAST(p_5ymas AS NUMERIC) AS population_age_greater_5,
	    CAST(p_5ymas_f AS NUMERIC) AS population_age_greater_5_female,
	    CAST(p_12ymas AS NUMERIC) AS population_age_greater_12,
	    CAST(p_12ymas_f AS NUMERIC) AS population_age_greater_12_female,
	    CAST(p_15ymas AS NUMERIC) AS population_age_greater_15,
	    CAST(p_15ymas_f AS NUMERIC) AS population_age_greater_15_female,
	    CAST(p_18ymas AS NUMERIC) AS population_age_greater_18,
	    CAST(p_18ymas_f AS NUMERIC) AS population_age_greater_18_female,
	    CAST(p_60ymas AS NUMERIC) AS population_age_greater_60,
	    CAST(p_60ymas_f AS NUMERIC) AS population_age_greater_60_female,
	    CAST(prom_hnv AS NUMERIC) AS fertility_rate,

	    /* migration info */
	    CAST(pnacent AS NUMERIC) AS population_born_in_state,
	    CAST(pnacent_m AS NUMERIC) AS population_men_born_in_state,
	    CAST(pnacoe AS NUMERIC) AS population_born_out_of_state,
	    CAST(pnacoe_m AS NUMERIC) AS population_men_born_out_of_state,
	    CAST(presoe05 AS NUMERIC) AS population_immigrant_since_2005,
	    CAST(presoe05_m AS NUMERIC) AS population_immigrant_men_since_2005,

	    /* indigenous population info */
	    CAST(p3ym_hli AS NUMERIC) AS population_speak_indigenous,
	    CAST(p3ym_hli_m AS NUMERIC) AS population_male_speak_indigenous,
	    CAST(p3hlinhe AS NUMERIC) AS population_not_speak_spanish,
	    CAST(p3hlinhe_m AS NUMERIC) AS population_male_not_speak_spanish,
	    CAST(p3hli_he AS NUMERIC) AS population_speak_indigenous_and_spanish,
	    CAST(p3hli_he_m AS NUMERIC) AS population_male_speak_indigenous_and_spanish,
	    CAST(phog_ind AS NUMERIC) AS n_indigenous_households,

	    /* disability info */
	    CAST(pcon_lim AS NUMERIC) AS population_limited_activity,
	    CAST(pclim_mot AS NUMERIC) AS population_limited_mobility,
	    CAST(pclim_vis AS NUMERIC) AS population_limited_vision,
	    CAST(pclim_leng AS NUMERIC) AS population_limited_speaking,
	    CAST(pclim_aud AS NUMERIC) AS population_limited_hearing,
	    CAST(pclim_men AS NUMERIC) AS population_limited_senility,
	    CAST(pclim_men2 AS NUMERIC) AS population_limited_mental,

	    /* education info */
	    CAST(p3a5_noa AS NUMERIC) AS population_ages_3_5_no_school,
	    CAST(p3a5_noa_m AS NUMERIC) AS population_male_ages_3_5_no_school,
	    CAST(p6a11_noa AS NUMERIC) AS population_ages_6_11_no_school,
	    CAST(p6a11_noam AS NUMERIC) AS population_male_ages_6_11_no_school,
	    CAST(p12a14noa AS NUMERIC) AS population_ages_12_14_no_school,
	    CAST(p12a14noam AS NUMERIC) AS population_male_ages_12_14_no_school,
	    CAST(p15a17a AS NUMERIC) AS population_ages_15_17_in_school,
	    CAST(p15a17a_m AS NUMERIC) AS population_male_ages_15_17_in_school,
	    CAST(p18a24a AS NUMERIC) AS population_ages_18_24_in_school,
	    CAST(p18a24a_m AS NUMERIC) AS population_male_ages_18_24_in_school,
	    CAST(p8a14an AS NUMERIC) AS population_ages_8_14_illiterate,
	    CAST(p8a14an_m AS NUMERIC) AS population_male_ages_8_14_illiterate,
	    CAST(p15ym_an AS NUMERIC) AS population_age_over_15_illiterate,
	    CAST(p15ym_an_m AS NUMERIC) AS population_male_age_over_15_illiterate,
	    CAST(p15ym_se AS NUMERIC) AS population_age_over_15_no_school,
	    CAST(p15ym_se_m AS NUMERIC) AS population_male_age_over_15_no_school,
	    CAST(p15pri_in AS NUMERIC) AS population_age_over_15_no_primary_school,
	    CAST(p15pri_inm AS NUMERIC) AS population_male_age_over_15_no_primary_school,
	    CAST(p15sec_in AS NUMERIC) AS population_age_over_15_no_secondary_school,
	    CAST(p15sec_inm AS NUMERIC) AS population_male_age_over_15_no_secondary_school,
	    CAST(p18ym_pb AS NUMERIC) AS population_age_over_18_basic_schooling,
	    CAST(p18ym_pb_m AS NUMERIC) AS population_male_age_over_18_basic_schooling,

	    /* socioeconomic characteristics */
	    CAST(pea AS NUMERIC) AS population_economically_active,
	    CAST(pea_m AS NUMERIC) AS population_male_economically_active,
	    CAST(pe_inac AS NUMERIC) AS population_not_economically_active,
	    CAST(pe_inac_m AS NUMERIC) AS population_male_not_economically_active,
	    CAST(pocupada AS NUMERIC) AS population_employed,
	    CAST(pocupada_m AS NUMERIC) AS population_male_employed,
	    CAST(pdesocup AS NUMERIC) AS population_unemployed,
	    CAST(pdesocup_m AS NUMERIC) AS population_male_unemployed,

	    /* health services */
	    CAST(psinder AS NUMERIC) AS population_no_health_services,
	    CAST(pder_ss AS NUMERIC) AS population_entitled_to_health_services,
	    CAST(pder_imss AS NUMERIC) AS population_covered_by_imss,
	    CAST(pder_iste AS NUMERIC) AS population_covered_by_issste,
	    CAST(pder_istee AS NUMERIC) AS population_covered_by_state_issste,
	    CAST(pder_segp AS NUMERIC) AS population_covered_by_seguro_popular,

	    /* marriage info */
	    CAST(p12ym_solt AS NUMERIC) AS population_over_12_unmarried,
	    CAST(p12ym_casa AS NUMERIC) AS population_over_12_married,
	    CAST(p12ym_sepa AS NUMERIC) AS population_over_12_divorced,

	    /* religion info */
	    CAST(pcatolica AS NUMERIC) AS population_catholic,
	    CAST(pncatolica AS NUMERIC) AS population_other_christian,
	    CAST(potras_rel AS NUMERIC) AS population_other_religions,
	    CAST(psin_relig AS NUMERIC) AS population_non_religious,

	    /* census households */
    	    CAST(tothog AS NUMERIC) AS n_census_households,
    	    CAST(hogjef_m AS NUMERIC) AS n_male_head_of_household,
    	    CAST(hogjef_f AS NUMERIC) AS n_female_head_of_household,
    	    CAST(pobhog AS NUMERIC) AS n_census_persons,

	    /* housing */
	    CAST(ocupvivpar AS NUMERIC) AS n_people_private_homes,
	    CAST(prom_ocup AS NUMERIC) AS average_number_home_occupants,
	    CAST(pro_ocup_c AS NUMERIC) AS average_number_occupants_per_room,
	    CAST(vph_pisodt AS NUMERIC) AS n_different_floor_material,
	    CAST(vph_pisoti AS NUMERIC) AS n_private_homes_dirt_floors,
	    CAST(vph_1dor AS NUMERIC) AS n_homes_with_one_bedroom,
	    CAST(vph_2ymasd AS NUMERIC) AS n_homes_with_at_least_two_bedrooms,
	    CAST(vph_1cuart AS NUMERIC) AS n_homes_with_one_room,
	    CAST(vph_2cuart AS NUMERIC) AS n_homes_with_two_rooms,
	    CAST(vph_3ymasc AS NUMERIC) AS n_homes_with_three_or_more_rooms,
	    CAST(vph_c_elec AS NUMERIC) AS n_homes_with_electricity,
	    CAST(vph_s_elec AS NUMERIC) AS n_homes_without_electricity,
	    CAST(vph_aguadv AS NUMERIC) AS n_homes_with_plumbing,
	    CAST(vph_aguafv AS NUMERIC) AS n_homes_without_plumbing,
	    CAST(vph_excsa AS NUMERIC) AS n_homes_with_toilet,
	    CAST(vph_drenaj AS NUMERIC) AS n_homes_with_sewage_access,
	    CAST(vph_c_serv AS NUMERIC) AS n_homes_with_full_infrastructure,
	    CAST(vph_snbien AS NUMERIC) AS n_homes_without_any_goods,
	    CAST(vph_radio AS NUMERIC) AS n_homes_with_radio,
	    CAST(vph_tv AS NUMERIC) AS n_homes_with_tv,
	    CAST(vph_refri AS NUMERIC) AS n_homes_with_refigerador,
	    CAST(vph_lavad AS NUMERIC) AS n_homes_with_washing_machine,
	    CAST(vph_autom AS NUMERIC) AS n_homes_with_car,
	    CAST(vph_pc AS NUMERIC) AS n_homes_with_computer,
	    CAST(vph_telef AS NUMERIC) AS n_homes_with_telephone,
	    CAST(vph_cel AS NUMERIC) AS n_homes_with_cell_hone,
	    CAST(vph_inter AS NUMERIC) AS n_homes_with_internet,

	    /* poverty measures */
	    CAST(h_pobres AS NUMERIC) AS n_households_in_poverty,
	    CAST(p_pobres AS NUMERIC) AS n_people_in_poverty,

	    /* carencies */
	    CAST(V5 AS NUMERIC) AS n_housholds_without_basic_housing_services,
	    CAST(V6 AS NUMERIC) AS n_people_without_basic_housing_services,
	    CAST(V7 AS NUMERIC) AS n_households_without_water,
	    CAST(V8 AS NUMERIC) AS n_people_without_water,
	    CAST(V9 AS NUMERIC) AS n_households_without_sewage,
	    CAST(V10 AS NUMERIC) AS n_people_without_sewage,
	    CAST(V11 AS NUMERIC) AS n_households_without_electricity,
	    CAST(V12 AS NUMERIC) AS n_people_without_electricity,
	    CAST(V13 AS NUMERIC) AS n_households_without_quality_living_space,
	    CAST(V14 AS NUMERIC) AS n_people_without_quality_living_space,
	    CAST(V15 AS NUMERIC) AS n_households_dirt_floor,
	    CAST(V16 AS NUMERIC) AS n_people_dirt_floor,
	    CAST(V17 AS NUMERIC) AS n_households_overcrowding,
	    CAST(V18 AS NUMERIC) AS n_people_overcrowding,
	    CAST(V19 AS NUMERIC) AS n_households_no_health,
	    CAST(V20 AS NUMERIC) AS n_people_no_health,
	    CAST(V21 AS NUMERIC) AS n_households_lack_education,
	    CAST(V22 AS NUMERIC) AS n_people_lack_education

	    FROM raw.localidades_urbanas
UNION
     SELECT cve_locc AS locality_id,
            ST_Transform(ST_SetSRID(geom, 8624), 4236) AS geom,
	    nom_loc AS locality_name,
	    nom_mun AS municipality_name,
	    nom_ent AS state_name,
	    FALSE AS urban,

	    /* age and gender demographics */
	    NULL AS population,
	    NULL AS population_female,
	    NULL AS population_age_0_to_2,
	    NULL AS population_age_0_to_2_female,
	    NULL AS population_age_greater_3,
	    NULL AS population_age_greater_3_female,
	    NULL AS population_age_greater_5,
	    NULL AS population_age_greater_5_female,
	    NULL AS population_age_greater_12,
	    NULL AS population_age_greater_12_female,
	    NULL AS population_age_greater_15,
	    NULL AS population_age_greater_15_female,
	    NULL AS population_age_greater_18,
	    NULL AS population_age_greater_18_female,
	    NULL AS population_age_greater_60,
	    NULL AS population_age_greater_60_female,
	    NULL AS fertility_rate,

	    /* migration info */
	    NULL AS population_born_in_state,
	    NULL AS population_men_born_in_state,
	    NULL AS population_born_out_of_state,
	    NULL AS population_men_born_out_of_state,
	    NULL AS population_immigrant_since_2005,
	    NULL AS population_immigrant_men_since_2005,

	    /* indigenous population info */
	    NULL AS population_speak_indigenous,
	    NULL AS population_male_speak_indigenous,
	    NULL AS population_not_speak_spanish,
	    NULL AS population_male_not_speak_spanish,
	    NULL AS population_speak_indigenous_and_spanish,
	    NULL AS population_male_speak_indigenous_and_spanish,
	    NULL AS n_indigenous_households,

	    /* disability info */
	    NULL AS population_limited_activity,
	    NULL AS population_limited_mobility,
	    NULL AS population_limited_vision,
	    NULL AS population_limited_speaking,
	    NULL AS population_limited_hearing,
	    NULL AS population_limited_senility,
	    NULL AS population_limited_mental,

	    /* education info */
	    NULL AS population_ages_3_5_no_school,
	    NULL AS population_male_ages_3_5_no_school,
	    NULL AS population_ages_6_11_no_school,
	    NULL AS population_male_ages_6_11_no_school,
	    NULL AS population_ages_12_14_no_school,
	    NULL AS population_male_ages_12_14_no_school,
	    NULL AS population_ages_15_17_in_school,
	    NULL AS population_male_ages_15_17_in_school,
	    NULL AS population_ages_18_24_in_school,
	    NULL AS population_male_ages_18_24_in_school,
	    NULL AS population_ages_8_14_illiterate,
	    NULL AS population_male_ages_8_14_illiterate,
	    NULL AS population_age_over_15_illiterate,
	    NULL AS population_male_age_over_15_illiterate,
	    NULL AS population_age_over_15_no_school,
	    NULL AS population_male_age_over_15_no_school,
	    NULL AS population_age_over_15_no_primary_school,
	    NULL AS population_male_age_over_15_no_primary_school,
	    NULL AS population_age_over_15_no_secondary_school,
	    NULL AS population_male_age_over_15_no_secondary_school,
	    NULL AS population_age_over_18_basic_schooling,
	    NULL AS population_male_age_over_18_basic_schooling,

	    /* socioeconomic characteristics */
	    NULL AS population_economically_active,
	    NULL AS population_male_economically_active,
	    NULL AS population_not_economically_active,
	    NULL AS population_male_not_economically_active,
	    NULL AS population_employed,
	    NULL AS population_male_employed,
	    NULL AS population_unemployed,
	    NULL AS population_male_unemployed,

	    /* health services */
	    NULL AS population_no_health_services,
	    NULL AS population_entitled_to_health_services,
	    NULL AS population_covered_by_imss,
	    NULL AS population_covered_by_issste,
	    NULL AS population_covered_by_state_issste,
	    NULL AS population_covered_by_seguro_popular,

	    /* marriage info */
	    NULL AS population_over_12_unmarried,
	    NULL AS population_over_12_married,
	    NULL AS population_over_12_divorced,

	    /* religion info */
	    NULL AS population_catholic,
	    NULL AS population_other_christian,
	    NULL AS population_other_religions,
	    NULL AS population_non_religious,

	    /* census households */
    	    NULL AS n_census_households,
    	    NULL AS n_male_head_of_household,
    	    NULL AS n_female_head_of_household,
    	    NULL AS n_census_persons,

	    /* housing */
	    NULL AS n_people_private_homes,
	    NULL AS average_number_home_occupants,
	    NULL AS average_number_occupants_per_room,
	    NULL AS n_different_floor_material,
	    NULL AS n_private_homes_dirt_floors,
	    NULL AS n_homes_with_one_bedroom,
	    NULL AS n_homes_with_at_least_two_bedrooms,
	    NULL AS n_homes_with_one_room,
	    NULL AS n_homes_with_two_rooms,
	    NULL AS n_homes_with_three_or_more_rooms,
	    NULL AS n_homes_with_electricity,
	    NULL AS n_homes_without_electricity,
	    NULL AS n_homes_with_plumbing,
	    NULL AS n_homes_without_plumbing,
	    NULL AS n_homes_with_toilet,
	    NULL AS n_homes_with_sewage_access,
	    NULL AS n_homes_with_full_infrastructure,
	    NULL AS n_homes_without_any_goods,
	    NULL AS n_homes_with_radio,
	    NULL AS n_homes_with_tv,
	    NULL AS n_homes_with_refigerador,
	    NULL AS n_homes_with_washing_machine,
	    NULL AS n_homes_with_car,
	    NULL AS n_homes_with_computer,
	    NULL AS n_homes_with_telephone,
	    NULL AS n_homes_with_cell_hone,
	    NULL AS n_homes_with_internet,

	    /* poverty measures */
	    NULL AS n_households_in_poverty,
	    NULL AS n_people_in_poverty,

	    /* carencies */
	    NULL AS n_housholds_without_basic_housing_services,
	    NULL AS n_people_without_basic_housing_services,
	    NULL AS n_households_without_water,
	    NULL AS n_people_without_water,
	    NULL AS n_households_without_sewage,
	    NULL AS n_people_without_sewage,
	    NULL AS n_households_without_electricity,
	    NULL AS n_people_without_electricity,
	    NULL AS n_households_without_quality_living_space,
	    NULL AS n_people_without_quality_living_space,
	    NULL AS n_households_dirt_floor,
	    NULL AS n_people_dirt_floor,
	    NULL AS n_households_overcrowding,
	    NULL AS n_people_overcrowding,
	    NULL AS n_households_no_health,
	    NULL AS n_people_no_health,
	    NULL AS n_households_lack_education,
	    NULL AS n_people_lack_education

	    FROM raw.localidades_rurales) localidades;

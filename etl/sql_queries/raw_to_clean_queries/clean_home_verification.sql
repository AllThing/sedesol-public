/* This script cleans and combines the 2013 and 2014 home verification data,
 * available for households surveyed by PROSPERA. They are assumed to be
 * contained in postgres database tables raw.home_verification_2013 and
 * raw.home_verification_2014. The output has only a few modifications of the
 * original home verification data,
 *    - Types are converted from strings to numerics or boolean when appropriate
 *    - More descriptive column names are provided.
 */

CREATE TABLE clean.home_verification AS
SELECT 
DISTINCT

    folio,
        
    folio_enca,

    CASE WHEN programa = ' ' THEN NULL
    ELSE programa END AS program,

   
    CASE WHEN tipo_encue = ' ' THEN NULL
    ELSE tipo_encue END
    AS survey_type,

    CAST(
	CASE WHEN vtot_cua = ' ' THEN NULL
	ELSE vtot_cua
	END AS NUMERIC
	) AS verified_n_rooms,

	CAST(
	CASE WHEN vcuarto_d = '1' THEN TRUE
	WHEN vcuarto_d = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_sleep_in_kitchen,

	CAST(
	CASE WHEN vcua_dor = ' ' THEN NULL
	ELSE vcua_dor
	END AS NUMERIC
	) AS verified_n_bedrooms,

	vpiso_viv AS verified_floor_material,

	CAST(
	CASE WHEN vcua_pis_t = ' ' THEN NULL
	WHEN vcua_pis_t = '1' THEN TRUE
	WHEN vcua_pis_t = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_dirt_where_sleep_or_cook,

	vtech_viv AS verified_roof_material,
	vmuro_viv AS verified_wall_material,
	vagua AS verified_water_source,
	vescusado AS verified_toilet,

	CAST(
	CASE WHEN vuso_exc = ' ' THEN NULL
	WHEN vuso_exc = '1' THEN TRUE
	WHEN vuso_exc = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_toilet_exclusively_residents,

	vcon_dren as verified_sewage_type,
	vluz_ele as verified_light_source,

       CAST(
	CASE WHEN vfocos = ' ' THEN NULL
	ELSE vfocos
	END AS NUMERIC
	) AS verified_n_lightbulbs,

	CAST(
	CASE WHEN vestgas = ' ' THEN NULL
	WHEN vestgas = '1' THEN TRUE
	WHEN vestgas = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_stove,

	CAST(
	CASE WHEN vsir_est = ' ' THEN NULL
	WHEN vsir_est = '1' THEN TRUE
	WHEN vsir_est = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_stove_works,

	CAST(
	CASE WHEN vparrill = ' ' THEN NULL
	WHEN vparrill = '1' THEN TRUE
	WHEN vparrill = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_grill,

	CAST(
	CASE WHEN vsir_parr = ' ' THEN NULL
	WHEN vsir_parr = '1' THEN TRUE
	WHEN vsir_parr = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_grill_works,

	CAST(
	CASE WHEN vfogon_1 = ' ' THEN NULL
	WHEN vfogon_1 = '1' THEN TRUE
	WHEN vfogon_1 = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_cooker,

	CAST(
	CASE WHEN vsir_fog1 = ' ' THEN NULL
	WHEN vsir_fog1 = '1' THEN TRUE
	WHEN vsir_fog1 = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_cooker_works,

	CAST(
	CASE WHEN  vrefri = ' ' THEN NULL
	WHEN vrefri = '1' THEN TRUE
	WHEN vrefri = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_refridgerator,

	CAST(
	CASE WHEN vsir_refr  = ' ' THEN NULL
	WHEN vsir_refr = '1' THEN TRUE
	WHEN vsir_refr = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_refridgerator_works,

	CAST(
	CASE WHEN vlicuadora  = ' ' THEN NULL
	WHEN vlicuadora = '1' THEN TRUE
	WHEN vlicuadora = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_blender,

	CAST(
	CASE WHEN vsir_lic  = ' ' THEN NULL
	WHEN vsir_lic = '1' THEN TRUE
	WHEN vsir_lic = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_blender_works,

	CAST(
	CASE WHEN vmicro  = ' ' THEN NULL
	WHEN vmicro = '1' THEN TRUE
	WHEN vmicro  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_microwave,

	CAST(
	CASE WHEN vsir_micro  = ' ' THEN NULL
	WHEN vsir_micro = '1' THEN TRUE
	WHEN vsir_micro = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_microwave_works,

	CAST(
	CASE WHEN vfregadero  = ' ' THEN NULL
	WHEN vfregadero = '1' THEN TRUE
	WHEN vfregadero = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_sink,

	CAST(
	CASE WHEN vsir_freg  = ' ' THEN NULL
	WHEN vsir_freg = '1' THEN TRUE
	WHEN vsir_freg = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_sink_works,

	CAST(
	CASE WHEN vlavadora = ' ' THEN NULL
	WHEN vlavadora = '1' THEN TRUE
	WHEN vlavadora = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_washing_machine,

	CAST(
	CASE WHEN vsir_lava = ' ' THEN NULL
	WHEN vsir_lava = '1' THEN TRUE
	WHEN vsir_lava = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_washing_machine_works,

	CAST(
	CASE WHEN vplancha = ' ' THEN NULL
	WHEN vplancha  = '1' THEN TRUE
	WHEN vplancha  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_clothes_iron,

	CAST(
	CASE WHEN vsir_plan = ' ' THEN NULL
        WHEN vsir_plan  = '1' THEN TRUE
	WHEN vsir_plan  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_clothes_iron_works,

	CAST(
	CASE WHEN vcoser = ' ' THEN NULL
	WHEN vcoser  = '1' THEN TRUE
	WHEN vcoser  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_sewing_machine,

	CAST(
	CASE WHEN vsir_cose = ' ' THEN NULL
	WHEN vsir_cose  = '1' THEN TRUE
	WHEN vsir_cose  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_sewing_machine_works,

	CAST(
	CASE WHEN vtinaco = ' ' THEN NULL
	WHEN vtinaco  = '1' THEN TRUE
	WHEN vtinaco  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_water_tank,

	CAST(
	CASE WHEN vsir_tina = ' ' THEN NULL
	WHEN vsir_tina  = '1' THEN TRUE
	WHEN vsir_tina  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_water_tank_works,

	CAST(
	CASE WHEN vcal_agua = ' '
	THEN NULL WHEN vcal_agua  = '1'
	THEN TRUE WHEN vcal_agua  = '2'
	THEN FALSE END AS BOOLEAN
	) AS verified_own_water_heater,

	CAST(
	CASE
	WHEN vsir_cal = ' ' THEN NULL
	WHEN vsir_cal  = '1' THEN TRUE
	WHEN vsir_cal  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_water_heater_works,

	CAST(
	CASE WHEN vradio = ' ' THEN NULL
	WHEN vradio  = '1' THEN TRUE
	WHEN vradio  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_radio,

	CAST(
	CASE WHEN vsir_radio = ' ' THEN NULL
	WHEN vsir_radio  = '1' THEN TRUE
	WHEN vsir_radio  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_radio_works,

	CAST(
	CASE WHEN vtocadis = ' ' THEN NULL
	WHEN vtocadis  = '1' THEN TRUE
	WHEN vtocadis  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_music_player,

	CAST(
	CASE WHEN vsir_toca = ' ' THEN NULL
	WHEN vsir_toca  = '1' THEN TRUE
	WHEN vsir_toca  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_music_player_works,

	CAST(
	CASE WHEN vtelevis = ' ' THEN NULL
	WHEN vtelevis  = '1' THEN TRUE
	WHEN vtelevis  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_tv,

	CAST(
	CASE WHEN vsir_tv = ' ' THEN NULL
	WHEN vsir_tv  = '1' THEN TRUE
	WHEN vsir_tv  = '2' THEN FALSE
	END AS BOOLEAN) AS verified_tv_works,

	CAST(
	CASE WHEN vant_sky = ' ' THEN NULL
	WHEN vant_sky  = '1' THEN TRUE
	WHEN vant_sky  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_tv_service,

	CAST(
	CASE WHEN vsir_sky = ' ' THEN NULL
	WHEN vsir_sky  = '1' THEN TRUE
	WHEN vsir_sky  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_tv_service_works,

	CAST(
	CASE WHEN vvideocas = ' ' THEN NULL
	WHEN vvideocas  = '1' THEN TRUE
	WHEN vvideocas  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_vhs,

	CAST(
	CASE WHEN vtelefon = ' ' THEN NULL
	WHEN vtelefon  = '1' THEN TRUE
	WHEN vtelefon  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_telephone,

	CAST(
	CASE WHEN vsir_telef = ' ' THEN NULL
	WHEN vsir_telef  = '1' THEN TRUE
	WHEN vsir_telef  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_telephone_works,

	CAST(
	CASE WHEN vcelular = ' ' THEN NULL
	WHEN vcelular  = '1' THEN TRUE
	WHEN vcelular  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_cell_phone,

	CAST(
	CASE WHEN vsir_cel = ' ' THEN NULL
	WHEN vsir_cel  = '1' THEN TRUE
	WHEN vsir_cel  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_cell_phone_works,

	CAST(
	CASE WHEN vcompu = ' ' THEN NULL
	WHEN vcompu  = '1' THEN TRUE
	WHEN vcompu  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_computer,

	CAST(
	CASE WHEN vsir_compu = ' ' THEN NULL
	WHEN vsir_compu  = '1' THEN TRUE
	WHEN vsir_compu  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_computer_works,

	CAST(
	CASE WHEN vventila = ' ' THEN NULL
	WHEN vventila  = '1' THEN TRUE
	WHEN vventila  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_cooling_fan,

	CAST(
	CASE WHEN vsir_vent = ' ' THEN NULL
	WHEN vsir_vent  = '1' THEN TRUE
	WHEN vsir_vent  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_cooling_fan_works,

	CAST(
	CASE WHEN vcalefa = ' ' THEN NULL
	WHEN vcalefa  = '1' THEN TRUE
	WHEN vcalefa  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_home_heating,

	CAST(
	CASE WHEN vsir_cale = ' ' THEN NULL
	WHEN vsir_cale  = '1' THEN TRUE
	WHEN vsir_cale  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_home_heating_works,

	CAST(
	CASE WHEN vclima = ' ' THEN NULL
	WHEN vclima  = '1' THEN TRUE
	WHEN vclima  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_climate_control,

	CAST(
	CASE WHEN vsir_clima = ' ' THEN NULL
	WHEN vsir_clima  = '1' THEN TRUE
	WHEN vsir_clima  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_climate_control_works,

	CAST(
	CASE WHEN vvehiculo = ' ' THEN NULL
	WHEN vvehiculo  = '1' THEN TRUE
	WHEN vvehiculo  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_vehicle,

	CAST(
	CASE WHEN vsir_vehi = ' ' THEN NULL
	WHEN vsir_vehi  = '1' THEN TRUE
	WHEN vsir_vehi  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_vehicle_works,

	CAST(
	CASE WHEN vtractor = ' ' THEN NULL
	WHEN vtractor  = '1' THEN TRUE
	WHEN vtractor  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_tractor,

	CAST(
	CASE WHEN vsir_trac = ' ' THEN NULL
	WHEN vsir_trac  = '1' THEN TRUE
	WHEN vsir_trac  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_tractor_works,

	CAST(
	CASE WHEN vinternet = ' ' THEN NULL
	WHEN vinternet  = '1' THEN TRUE
	WHEN vinternet  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_own_internet,

	CAST(
	CASE WHEN vsir_inter = ' ' THEN NULL
	WHEN vsir_inter  = '1' THEN TRUE
	WHEN vsir_inter  = '2' THEN FALSE
	END AS BOOLEAN
	) AS verified_internet_works,

	CAST(
	CASE WHEN vtraduc = ' ' THEN NULL
	WHEN vtraduc  = '1' THEN TRUE
	WHEN vtraduc  = '2' THEN FALSE
	END AS BOOLEAN
	) AS interview_through_translator,

	CAST(CASE WHEN vverdad = ' ' THEN NULL
	WHEN vverdad  = '1' THEN TRUE
	WHEN vverdad  = '2' THEN FALSE
	END AS BOOLEAN) AS respondent_honest,

	vpobre AS poverty_level,
	observa AS comments,
	id_corte as id_corte
FROM (

     /* Take union over the 2013 and 2014 home verification databases */
     SELECT *,
     	    NULL AS vinternet, /* 2013 doesn't have internet measurements */
	    NULL AS vsir_inter,
	    2013 AS year
     FROM raw.home_verification_2013
     UNION ALL
     SELECT *,
     	    2014 AS year
     FROM raw.home_verification_2014
) home_verif;

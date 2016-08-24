/* This query belongs to the raw -> cleaned tables step in the SEDESOL ETL
 * pipeline. It assumes the table raw.cuis_se_vivienda is present, and
 * defines a new table with the following cleaning transformations,
 *  - Removes columns already present in the SIFODE universal table
 *  - Transforms binary fields to BOOLEAN
 *  - Converts quantitative fields to NUMERIC
 *  - Gives more descriptive column names
 */

CREATE TABLE clean.cuis_se_vivienda AS
SELECT llave_hogar_h AS home_id,
       id_mdm_h AS family_id,

       /* home activities */
       ut_cuida1 AS activities_care,
       ut_volun1 AS activities_volunteer,
       ut_repara1 AS activities_repairs,
       ut_limpia1 AS activities_housework,
       ut_acarrea1 AS activities_water_charcoal,

       /* home finances */
       CAST(
       CASE WHEN con_remesa = '1' THEN True
       WHEN con_remesa = ' ' THEN NULL
       ELSE False
       END
       AS BOOLEAN) AS receive_remittance,

       CAST(
       CASE WHEN gas_alim = ' ' THEN NULL
       WHEN gas_alim = '99999' THEN NULL
       ELSE gas_alim
       END
       AS NUMERIC) as spending_food,

       CAST(
       CASE WHEN gas_vest = ' ' THEN NULL
       WHEN gas_vest = '99999' THEN NULL
       ELSE gas_vest
       END
       AS NUMERIC) as spending_clothes,

       /* food security */
       CAST(
       CASE WHEN com_dia = '-1' THEN NULL
       WHEN com_dia = ' ' THEN NULL
       ELSE com_dia
       END
       AS NUMERIC) as meals_per_day,

       cereal AS food_frequency_cereal,
       verduras AS  food_frequency_vegetables,
       frutas AS food_frequency_fruit,
       leguminosas AS food_frequency_legumes,
       carne_huevo AS food_frequency_meat,
       lacteos AS food_frequency_milk,
       grasas AS food_frequency_sugars,

       CAST(
       CASE WHEN seg_alim_1 = '1' THEN TRUE
       WHEN seg_alim_1 = '2' THEN FALSE
       WHEN seg_alim_1 = ' ' THEN NULL
       END AS BOOLEAN) AS food_had_little_variety,

       CAST(
       CASE WHEN seg_alim_2 = '1' THEN TRUE
       WHEN seg_alim_2 = '2' THEN FALSE
       WHEN seg_alim_2 = ' ' THEN NULL
       END AS BOOLEAN) AS skipped_meals,

       CAST(
       CASE WHEN seg_alim_3 = '1' THEN TRUE
       WHEN seg_alim_3 = '2' THEN FALSE
       WHEN seg_alim_3 = ' ' THEN NULL
       END AS BOOLEAN) AS ate_less_than_should,

       CAST(
       CASE WHEN seg_alim_4 = '1' THEN TRUE
       WHEN seg_alim_4 = '2' THEN FALSE
       WHEN seg_alim_4 = ' ' THEN NULL
       END AS BOOLEAN) AS ran_out_of_food,

       CAST(
       CASE WHEN seg_alim_5 = '1' THEN TRUE
       WHEN seg_alim_5 = '2' THEN FALSE
       WHEN seg_alim_5 = ' ' THEN NULL
       END AS BOOLEAN) AS went_hungry,

       CAST(
       CASE WHEN seg_alim_A = '1' THEN TRUE
       WHEN seg_alim_A = '2' THEN FALSE
       WHEN seg_alim_A = ' ' THEN NULL
       END AS BOOLEAN) AS ate_less_equal_once,

       CAST(
       CASE WHEN seg_alim_B = '1' THEN TRUE
       WHEN seg_alim_B = '2' THEN FALSE
       WHEN seg_alim_B = ' ' THEN NULL
       END AS BOOLEAN) AS food_little_variety_child,

       CAST(
       CASE WHEN seg_alim_C = '1' THEN TRUE
       WHEN seg_alim_C = '2' THEN FALSE
       WHEN seg_alim_C = ' ' THEN NULL
       END AS BOOLEAN) AS ate_less_than_should_child,

       CAST(
       CASE WHEN seg_alim_D = '1' THEN TRUE
       WHEN seg_alim_D = '2' THEN FALSE
       WHEN seg_alim_D = ' ' THEN NULL
       END AS BOOLEAN) AS meal_portion_decreased_child,

       CAST(
       CASE WHEN seg_alim_E = '1' THEN TRUE
       WHEN seg_alim_E = '2' THEN FALSE
       WHEN seg_alim_E = ' ' THEN NULL
       END AS BOOLEAN) AS went_hungry_child,

       CAST(
       CASE WHEN seg_alim_F = '1' THEN TRUE
       WHEN seg_alim_F = '2' THEN FALSE
       WHEN seg_alim_F = ' ' THEN NULL
       END AS BOOLEAN) AS slept_hungry_child,

       CAST(
       CASE WHEN seg_alim_G = '1' THEN TRUE
       WHEN seg_alim_G = '2' THEN FALSE
       WHEN seg_alim_G = ' ' THEN NULL
       END AS BOOLEAN) AS ate_less_equal_once_child,

       desay_nin AS children_eat_breakfast,
       desay_lugar AS children_breakfast_place,
       desay_razon AS children_breakfast_reason,

       /* home building conditions */
       CAST(
       CASE WHEN cuart = ' ' THEN NULL
       ELSE cuart
       END
       AS NUMERIC) as n_rooms, /* someone has 68 rooms? */

       CAST(
       CASE WHEN cua_dor = ' ' THEN NULL
       ELSE cua_dor
       END
       AS NUMERIC) as n_bedrooms,

       CAST(
       CASE WHEN coc_duer = '1' THEN TRUE
       WHEN coc_duer = '2' THEN FALSE
       WHEN coc_duer = ' ' THEN NULL
       END AS BOOLEAN) as cook_sleep_same_room,

       c_piso_viv AS floor_material,

       CAST(
       CASE WHEN condi_piso = '1' THEN TRUE
       WHEN condi_piso = '2' THEN FALSE
       WHEN condi_piso = ' ' THEN NULL
       END AS BOOLEAN) as floor_poor_condition,

       CAST(
       CASE WHEN cuar_pis_t = '1' THEN TRUE
       WHEN cuar_pis_t = '2' THEN FALSE
       WHEN cuar_pis_t = ' ' THEN NULL
       END AS BOOLEAN) AS dirt_where_sleep_or_cook,

       c_tech_viv AS roof_material,

       CAST(
       CASE WHEN condi_techo = '1' THEN TRUE
       WHEN condi_techo = '2' THEN FALSE
       WHEN condi_techo = ' ' THEN NULL
       END AS BOOLEAN) AS roof_poor_condition,

       c_muro_viv AS wall_material,

       CAST(
       CASE WHEN condi_muro = '1' THEN TRUE
       WHEN condi_muro = '2' THEN FALSE
       WHEN condi_muro = ' ' THEN NULL
       END AS BOOLEAN) AS wall_poor_condition,

       /* local infrastructure */
       c_escusado AS toilet,

       CAST(
       CASE WHEN uso_exc = '1' THEN TRUE
       WHEN uso_exc = '2' THEN FALSE
       WHEN uso_exc = ' ' THEN NULL
       END AS BOOLEAN) AS toilet_exclusively_residents,

       c_agua_a AS water_source,

       CAST(
       CASE WHEN trat_agua_a = '0' THEN FALSE
       WHEN trat_agua_a = '1' THEN TRUE
       WHEN trat_agua_a = ' ' THEN NULL
       END AS BOOLEAN) AS no_water_treatment,

       CAST(
       CASE WHEN trat_agua_b = '0' THEN FALSE
       WHEN trat_agua_b = '1' THEN TRUE
       WHEN trat_agua_b = ' ' THEN NULL
       END AS BOOLEAN) AS boil_water,

       CAST(
       CASE WHEN trat_agua_c = '0' THEN FALSE
       WHEN trat_agua_c = '1' THEN TRUE
       WHEN trat_agua_c = ' ' THEN NULL
       END AS BOOLEAN) AS chlorinate_water,

       CAST(
       CASE WHEN trat_agua_d = '0' THEN FALSE
       WHEN trat_agua_d = '1' THEN TRUE
       WHEN trat_agua_d = ' ' THEN NULL
       END AS BOOLEAN) AS filter_water,

       CAST(
       CASE WHEN trat_agua_e = '0' THEN FALSE
       WHEN trat_agua_e = '1' THEN TRUE
       WHEN trat_agua_e = ' ' THEN NULL
       END AS BOOLEAN) AS buy_bottled_water,

       c_con_drena AS sewage_type,
       c_basura AS trash_type,

       /* materials goods owned */
       c_combus_cocin AS cooking_fuel,
       fogon_chim AS cooking_equipment,
       ts_refri AS own_refridgerator,
       ts_lavadora AS own_washing_machine,
       ts_vhs_dvd_br AS own_vhs,
       ts_vehi AS own_vehicle,
       ts_telefon AS own_telephone,
       ts_micro AS own_microwave,
       ts_compu AS own_computer,
       ts_est_gas AS own_stove,
       ts_boiler AS own_boiler,
       ts_internet AS own_internet,
       ts_celular AS own_cell_phone,
       ts_television AS own_tv,
       ts_tv_digital AS own_digital_tv,
       ts_tv_paga AS own_tv_service,
       ts_tinaco AS own_water_tank,
       ts_clima AS own_climate_control,
       c_luz_ele AS light_source,
       c_sit_viv AS home_payment,

       /* real estate info */
       CAST(
	CASE WHEN esp_niveles = '1' THEN TRUE
	WHEN esp_niveles = '2' THEN FALSE
	WHEN esp_niveles = ' ' THEN NULL
	END AS BOOLEAN) AS more_than_two_floors,

       CAST(
	CASE WHEN esp_construc = '1' THEN TRUE
	WHEN esp_construc = '2' THEN FALSE
	WHEN esp_construc = ' ' THEN NULL
	END AS BOOLEAN) AS have_construction_space,

       CAST(
	CASE WHEN construc_med = ' ' THEN NULL
	WHEN construc_med = '9999' THEN NULL
	ELSE construc_med
	END AS NUMERIC) AS construction_space_amount,

       CAST(
	CASE WHEN esp_local = '1' THEN TRUE
	WHEN esp_local = '2' THEN FALSE
	WHEN esp_local = ' ' THEN NULL
	END AS BOOLEAN) AS is_local_annex,

       CAST(
	CASE WHEN local_med = ' ' THEN NULL
	WHEN local_med = '9999' THEN NULL
	ELSE local_med
	END AS NUMERIC) AS house_space,

       tie_agri AS land_used_for_agriculture,

       CAST(
        CASE WHEN prop_tierra2 = ' ' THEN NULL
	ELSE prop_tierra2
	END AS NUMERIC) AS n_landowners,

	/* agriculture (not livestock) */
       CAST(
	CASE WHEN c_maiz = '1' THEN TRUE
	WHEN c_maiz = '0' THEN FALSE
        WHEN c_maiz = ' ' THEN NULL
	END AS BOOLEAN) AS grow_maize,

       CAST(
	CASE WHEN c_frij = '1' THEN TRUE
	WHEN c_frij = '0' THEN FALSE
        WHEN c_frij = ' ' THEN NULL
	END AS BOOLEAN) AS grow_beans,

       CAST(
	CASE WHEN c_cere = '1' THEN TRUE
	WHEN c_cere = '0' THEN FALSE
        WHEN c_cere = ' ' THEN NULL
	END AS BOOLEAN) AS grow_cereals,

       CAST(
	CASE WHEN c_frut = '1' THEN TRUE
	WHEN c_frut = '0' THEN FALSE
        WHEN c_frut = ' ' THEN NULL
	END AS BOOLEAN) AS grow_fruit,

       CAST(
	CASE WHEN c_cana = '1' THEN TRUE
	WHEN c_cana = '0' THEN FALSE
        WHEN c_cana = ' ' THEN NULL
	END AS BOOLEAN) AS grow_sugarcane,

       CAST(
	CASE WHEN c_jito = '1' THEN TRUE
	WHEN c_jito = '0' THEN FALSE
        WHEN c_jito = ' ' THEN NULL
	END AS BOOLEAN) AS grow_tomato,

       CAST(
	CASE WHEN c_chil = '1' THEN TRUE
	WHEN c_chil = '0' THEN FALSE
        WHEN c_chil = ' ' THEN NULL
	END AS BOOLEAN) AS grow_chile,

       CAST(
	CASE WHEN c_limn = '1' THEN TRUE
	WHEN c_limn = '0' THEN FALSE
        WHEN c_limn = ' ' THEN NULL
	END AS BOOLEAN) AS grow_lemon,

       CAST(
	CASE WHEN c_papa = '1' THEN TRUE
	WHEN c_papa = '0' THEN FALSE
        WHEN c_papa = ' ' THEN NULL
	END AS BOOLEAN) AS grow_potato,

       CAST(
	CASE WHEN c_cafe = '1' THEN TRUE
	WHEN c_cafe = '0' THEN FALSE
        WHEN c_cafe = ' ' THEN NULL
	END AS BOOLEAN) AS grow_coffee,

       CAST(
	CASE WHEN c_cate = '1' THEN TRUE
	WHEN c_cate = '0' THEN FALSE
        WHEN c_cate = ' ' THEN NULL
	END AS BOOLEAN) AS grow_avocado,

       CAST(
	CASE WHEN c_forr = '1' THEN TRUE
	WHEN c_forr = '0' THEN FALSE
        WHEN c_forr = ' ' THEN NULL
	END AS BOOLEAN) AS grow_hay,

       CAST(
	CASE WHEN c_otro = '1' THEN TRUE
	WHEN c_otro = '0' THEN FALSE
        WHEN c_otro = ' ' THEN NULL
	END AS BOOLEAN) AS grow_other,

       CAST(
	CASE WHEN c_ning = '1' THEN TRUE
	WHEN c_ning = '0' THEN FALSE
        WHEN c_ning = ' ' THEN NULL
	END AS BOOLEAN) AS grow_nothing,

       /* agriculture materials */
       CAST(
	CASE WHEN cul_riego = '1' THEN TRUE
	WHEN cul_riego = '0' THEN FALSE
        WHEN cul_riego = ' ' THEN NULL
	END AS BOOLEAN) AS use_irrigation,

       CAST(
	CASE WHEN cul_maquina = '1' THEN TRUE
	WHEN cul_maquina = '0' THEN FALSE
        WHEN cul_maquina = ' ' THEN NULL
	END AS BOOLEAN) AS use_machines_agriculture,

       CAST(
	CASE WHEN cul_anim = '1' THEN TRUE
	WHEN cul_anim = '0' THEN FALSE
        WHEN cul_anim = ' ' THEN NULL
	END AS BOOLEAN) AS use_animal_agriculture,

       CAST(
	CASE WHEN cul_ferorg = '1' THEN TRUE
	WHEN cul_ferorg = '0' THEN FALSE
        WHEN cul_ferorg = ' ' THEN NULL
	END AS BOOLEAN) AS use_fertilizers,

       CAST(
	CASE WHEN cul_ferquim = '1' THEN TRUE
	WHEN cul_ferquim = '0' THEN FALSE
        WHEN cul_ferquim = ' ' THEN NULL
	END AS BOOLEAN) AS use_chemical_fertilizers,

       CAST(
	CASE WHEN cul_plagui = '1' THEN TRUE
	WHEN cul_plagui = '0' THEN FALSE
        WHEN cul_plagui = ' ' THEN NULL
	END AS BOOLEAN) AS use_pesticides,

       CAST(
	CASE WHEN uso_hid_tra = '1' THEN TRUE
	WHEN uso_hid_tra = '0' THEN FALSE
        WHEN uso_hid_tra = ' ' THEN NULL
	END AS BOOLEAN) AS use_hydroponics,

       /* agriculture (livestock) */
       CAST(
	CASE WHEN caballos = '97' THEN NULL
        WHEN caballos = ' ' THEN NULL
	ELSE caballos
	END AS NUMERIC) AS n_horses,

       CAST(
	CASE WHEN burros = '97' THEN NULL
        WHEN burros = ' ' THEN NULL
	ELSE burros
	END AS NUMERIC) AS n_burros,

       CAST(
	CASE WHEN bueyes = '97' THEN NULL
        WHEN bueyes = ' ' THEN NULL
	ELSE bueyes
	END AS NUMERIC) AS n_oxen,

       CAST(
	CASE WHEN chivos = '97' THEN NULL
        WHEN chivos = ' ' THEN NULL
	ELSE chivos
	END AS NUMERIC) AS n_goats,

       CAST(
	CASE WHEN reses = '97' THEN NULL
        WHEN reses = ' ' THEN NULL
	ELSE reses
	END AS NUMERIC) AS n_cows,

       CAST(
	CASE WHEN gallinas = '97' THEN NULL
        WHEN gallinas = ' ' THEN NULL
	ELSE gallinas
	END AS NUMERIC) AS n_chickens,

       CAST(
	CASE WHEN cerdos = '97' THEN NULL
        WHEN cerdos = ' ' THEN NULL
	ELSE cerdos
	END AS NUMERIC) AS n_pigs,

       CAST(
	CASE WHEN conejos = '97' THEN NULL
        WHEN conejos = ' ' THEN NULL
	ELSE conejos
	END AS NUMERIC) AS n_rabbits

FROM raw.cuis_se_vivienda;

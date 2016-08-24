CREATE TABLE clean.enigh_poblacion AS
SELECT folioviv,
       foliohog,
       numren,
       parentesco,
       sexo,
       CASE WHEN edad = ' ' THEN NULL
       ELSE CAST(edad AS NUMERIC)
       END AS edad,
       madre_hog,
       madre_id,
       padre_hog,
       padre_id,
       disc1,
       disc2,
       disc3,
       disc4,
       disc5,
       disc6,
       disc7,
       causa1,
       causa2,
       causa3,
       causa4,
       causa5,
       causa6,
       causa7,
       hablaind,
       lenguaind,
       hablaesp,
       comprenind,
       etnia,
       alfabetism,
       asis_esc,
       nivel,
       grado,
       tipoesc,
       tiene_b,
       otorg_b,
       forma_b,
       tiene_c,
       otorg_c,
       forma_c,
       nivelaprob,
       gradoaprob,
       antec_esc,
       residencia,
       edo_conyug,
       pareja_hog,
       conyuge_id,
       segsoc,
       CASE WHEN ss_aa = ' ' THEN NULL
       ELSE CAST(ss_aa AS NUMERIC)
       END AS ss_aa,
       CASE WHEN ss_mm = ' ' THEN NULL
       ELSE CAST(ss_mm AS NUMERIC)
       END AS ss_mm,
       redsoc_1,
       redsoc_2,
       redsoc_3,
       redsoc_4,
       redsoc_5,
       redsoc_6,
       CASE WHEN hor_1 = ' ' THEN NULL
       ELSE CAST(hor_1 AS NUMERIC)
       END AS hor_1,
       CASE WHEN min_1 = ' ' THEN NULL
       ELSE CAST(min_1 AS NUMERIC)
       END AS min_1,
       usotiempo1,
       CASE WHEN hor_2 = ' ' THEN NULL
       ELSE CAST(hor_2 AS NUMERIC)
       END AS hor_2,
       CASE WHEN min_2 = ' ' THEN NULL
       ELSE CAST(min_2 AS NUMERIC)
       END AS min_2,
       usotiempo2,
       CASE WHEN hor_3 = ' ' THEN NULL
       ELSE CAST(hor_3 AS NUMERIC)
       END AS hor_3,
       CASE WHEN min_3 = ' ' THEN NULL
       ELSE CAST(min_3 AS NUMERIC)
       END AS min_3,
       usotiempo3,
       CASE WHEN hor_4 = ' ' THEN NULL
       ELSE CAST(hor_4 AS NUMERIC)
       END AS hor_4,
       CASE WHEN min_4 = ' ' THEN NULL
       ELSE CAST(min_4 AS NUMERIC)
       END AS min_4,
       usotiempo4,
       CASE WHEN hor_5 = ' ' THEN NULL
       ELSE CAST(hor_5 AS NUMERIC)
       END AS hor_5,
       CASE WHEN min_5 = ' ' THEN NULL
       ELSE CAST(min_5 AS NUMERIC)
       END AS min_5,
       usotiempo5,
       CASE WHEN hor_6 = ' ' THEN NULL
       ELSE CAST(hor_6 AS NUMERIC)
       END AS hor_6,
       CASE WHEN min_6 = ' ' THEN NULL
       ELSE CAST(min_6 AS NUMERIC)
       END AS min_6,
       usotiempo6,
       CASE WHEN hor_7 = ' ' THEN NULL
       ELSE CAST(hor_7 AS NUMERIC)
       END AS hor_7,
       CASE WHEN min_7 = ' ' THEN NULL
       ELSE CAST(min_7 AS NUMERIC)
       END AS min_7,
       usotiempo7,
       CASE WHEN hor_8 = ' ' THEN NULL
       ELSE CAST(hor_8 AS NUMERIC)
       END AS hor_8,
       CASE WHEN min_8 = ' ' THEN NULL
       ELSE CAST(min_8 AS NUMERIC)
       END AS min_8,
       usotiempo8,
       segpop,
       atemed,
       inst_1,
       inst_2,
       inst_3,
       inst_4,
       inst_5,
       inscr_1,
       inscr_2,
       inscr_3,
       inscr_4,
       inscr_5,
       inscr_6,
       inscr_7,
       inscr_8,
       prob_anio,
       prob_mes,
       prob_sal,
       aten_sal,
       servmed_1,
       servmed_2,
       servmed_3,
       servmed_4,
       servmed_5,
       servmed_6,
       servmed_7,
       servmed_8,
       servmed_9,
       servmed_10,
       servmed_11,
       CASE WHEN hh_lug = ' ' THEN NULL
       ELSE CAST(hh_lug AS NUMERIC)
       END AS hh_lug,
       CASE WHEN mm_lug = ' ' THEN NULL
       ELSE CAST(mm_lug AS NUMERIC)
       END AS mm_lug,
       CASE WHEN hh_esp = ' ' THEN NULL
       ELSE CAST(hh_esp AS NUMERIC)
       END AS hh_esp,
       CASE WHEN mm_esp = ' ' THEN NULL
       ELSE CAST(mm_esp AS NUMERIC)
       END AS mm_esp,
       pagoaten_1,
       pagoaten_2,
       pagoaten_3,
       pagoaten_4,
       pagoaten_5,
       pagoaten_6,
       pagoaten_7,
       noatenc_1,
       noatenc_2,
       noatenc_3,
       noatenc_4,
       noatenc_5,
       noatenc_6,
       noatenc_7,
       noatenc_8,
       noatenc_9,
       noatenc_10,
       noatenc_11,
       noatenc_12,
       noatenc_13,
       noatenc_14,
       noatenc_15,
       noatenc_16,
       norecib_1,
       norecib_2,
       norecib_3,
       norecib_4,
       norecib_5,
       norecib_6,
       norecib_7,
       norecib_8,
       norecib_9,
       norecib_10,
       norecib_11,
       razon_1,
       razon_2,
       razon_3,
       razon_4,
       razon_5,
       razon_6,
       razon_7,
       razon_8,
       razon_9,
       razon_10,
       razon_11,
       diabetes,
       pres_alta,
       peso,
       segvol_1,
       segvol_2,
       segvol_3,
       segvol_4,
       segvol_5,
       segvol_6,
       segvol_7,
       CASE WHEN hijos_viv = ' ' THEN NULL
       ELSE CAST(hijos_viv AS NUMERIC)
       END AS hijos_viv,
       CASE WHEN hijos_mue = ' ' THEN NULL
       ELSE CAST(hijos_mue AS NUMERIC)
       END AS hijos_mue,
       CASE WHEN hijos_sob = ' ' THEN NULL
       ELSE CAST(hijos_sob AS NUMERIC)
       END AS hijos_sob,
       trabajo_mp,
       motivo_aus,
       act_pnea1,
       act_pnea2,
       num_trabaj
FROM raw.inegi_poblacion_2014;

CREATE TABLE clean.enigh_vivi AS
SELECT folioviv,
       tipo_viv,
       ubica_geo AS locality_id,
       ageb,
       tam_loc,
       mat_pared,
       LTRIM(mat_techos, '0') AS mat_techos,
       CASE WHEN (mat_pisos = '&') THEN NULL
       ELSE mat_pisos
       END AS mat_pisos,
       CASE WHEN antiguedad = ' ' THEN NULL
       ELSE CAST(antiguedad AS NUMERIC)
       END AS antiguedad,
       antigua_ne,
       cocina,
       cocina_dor,
       CASE WHEN cuart_dorm = ' ' THEN NULL
       ELSE CAST(cuart_dorm AS NUMERIC)
       END AS cuart_dorm,
       CASE WHEN num_cuarto = ' ' THEN NULL
       ELSE CAST(num_cuarto AS NUMERIC)
       END AS num_cuarto,
       disp_agua,
       dotac_agua,
       excusado,
       uso_compar,
       sanit_agua,
       biodigest,
       CASE WHEN bano_comp = ' ' THEN NULL
       ELSE CAST(bano_comp AS NUMERIC)
       END AS bano_comp,
       CASE WHEN bano_excus = ' ' THEN NULL
       ELSE CAST(bano_excus AS NUMERIC)
       END AS bano_excus,
       CASE WHEN bano_regad = ' ' THEN NULL
       ELSE CAST(bano_regad AS NUMERIC)
       END AS bano_regad,
       drenaje,
       disp_elect,
       CASE WHEN focos_inca = ' ' THEN NULL
       ELSE CAST(focos_inca AS NUMERIC)
       END AS focos_inca,
       CASE WHEN focos_ahor = ' ' THEN NULL
       ELSE CAST(focos_ahor AS NUMERIC)
       END AS focos_ahor,
       combustible,
       estufa_chi,
       eli_basura,
       tenencia,
       CASE WHEN renta = ' ' THEN NULL
       ELSE CAST(renta AS NUMERIC)
       END AS renta,
       CASE WHEN estim_pago = ' ' THEN NULL
       ELSE CAST(estim_pago AS NUMERIC)
       END AS estim_pago,
       CASE WHEN pago_viv = ' ' THEN NULL
       ELSE CAST(pago_viv AS NUMERIC)
       END AS pago_viv,
       pago_mesp,
       tipo_adqui,
       viv_usada,
       tipo_finan,
       num_dueno1,
       hog_dueno1,
       num_dueno2,
       hog_dueno2,
       escrituras,
       lavadero,
       fregadero,
       regadera,
       tinaco_azo,
       cisterna,
       pileta,
       calent_sol,
       calent_gas,
       medidor_luz,
       bomba_agua,
       tanque_gas,
       aire_acond,
       calefacc,
       CASE WHEN tot_resid = ' ' THEN NULL
       ELSE CAST(tot_resid AS NUMERIC)
       END AS tot_resid,
       CASE WHEN tot_hom = ' ' THEN NULL
       ELSE CAST(tot_hom AS NUMERIC)
       END AS tot_hom,
       CASE WHEN tot_muj = ' ' THEN NULL
       ELSE CAST(tot_muj AS NUMERIC)
       END AS tot_muj,
       CASE WHEN tot_hog = ' ' THEN NULL
       ELSE CAST(tot_hog AS NUMERIC)
       END AS tot_hog,
       est_socio,
       est_dis,
       upm,
       CASE WHEN factor_viv = ' ' THEN NULL
       ELSE CAST(factor_viv AS NUMERIC)
       END AS factor_viv
FROM raw.inegi_vivi_2014;

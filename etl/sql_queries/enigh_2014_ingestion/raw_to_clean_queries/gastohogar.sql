create table clean.enigh_gastohogar as
select 
folioviv,
foliohog,
clave,
tipo_gasto,
mes_dia,
forma_pago,
lugar_comp,
orga_inst,
frecuencia,
fecha_adqu,
fecha_pago,
case when cantidad = ' ' then NULL else cast(replace(cantidad,',','.') as numeric) end as cantidad,
case when gasto = ' ' then NULL else cast(replace(gasto,',','.') as numeric) end as gasto,
case when pago_mp = ' ' then NULL else cast(replace(pago_mp,',','.') as numeric) end as pago_mp,
case when costo = ' ' then NULL else cast(replace(costo,',','.') as numeric) end as costo,
case when inmujer = ' ' then NULL else cast(replace(inmujer,',','.') as numeric) end as inmujer,
inst_1,
inst_2,
num_meses,
num_pagos,
ultim_pago,
case when gasto_tri = ' ' then NULL else cast(replace(gasto_tri,',','.') as numeric) end as gasto_tri,
case when gasto_nm = ' ' then NULL else cast(replace(gasto_nm,',','.') as numeric) end as gasto_nm,
case when gas_nm_tri = ' ' then NULL else cast(replace(gas_nm_tri,',','.') as numeric) end as gas_nm_tri,
case when imujer_tri = ' ' then NULL else cast(replace(imujer_tri,',','.') as numeric) end as imujer_tri
FROM
raw.inegi_gastohogar_2014;


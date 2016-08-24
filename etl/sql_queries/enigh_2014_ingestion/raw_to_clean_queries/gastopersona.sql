create table clean.enigh_gastopersona as
select
folioviv,
foliohog,
numren,
clave,
tipo_gasto,
mes_dia,
frec_rem,
inst,
forma_pago,
inscrip,
colegia,
material,
case when cantidad = ' ' then NULL else cast(replace(cantidad,',','.') as numeric) end as cantidad,
case when gasto = ' ' then NULL else cast(replace(gasto,',','.') as numeric) end as gasto,
case when costo = ' ' then NULL else cast(replace(costo,',','.') as numeric) end as costo,
case when gasto_tri = ' ' then NULL else cast(replace(gasto_tri,',','.') as numeric) end as gasto_tri,
case when gasto_nm = ' ' then NULL else cast(replace(gasto_nm,',','.') as numeric) end as gasto_nm,
case when gas_nm_tri = ' ' then NULL else cast(replace(gas_nm_tri,',','.') as numeric) end as gas_nm_tri
from raw.inegi_gastopersona_2014;

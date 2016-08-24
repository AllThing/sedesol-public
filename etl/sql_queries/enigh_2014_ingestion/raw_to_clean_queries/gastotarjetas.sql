create table clean.enigh_gastotarjetas as
select
folioviv,
foliohog,
clave,
case when gasto = ' ' then NULL else cast(gasto as numeric) end as gasto,
case when pago_mp = ' ' then NULL else cast(pago_mp as numeric) end as pago_mp,
case when gasto_tri = ' ' then NULL else cast(replace(gasto_tri,',','.') as numeric) end as gasto_tri
from raw.inegi_gastotarjetas_2014;

create table clean.enigh_ingresos as
select 
folioviv,
foliohog,
numren,
clave,
mes_1,
mes_2,
mes_3,
mes_4,
mes_5,
mes_6,
case when ing_1 = ' ' then NULL else cast(ing_1 as numeric) end,
case when ing_2 = ' ' then NULL else cast(ing_2 as numeric) end ,
case when ing_3 = ' ' then NULL else cast(ing_3 as numeric) end ,
case when ing_4 = ' ' then NULL else cast(ing_4 as numeric) end ,
case when ing_5 = ' ' then NULL else cast(ing_5 as numeric) end ,
case when ing_6 = ' ' then NULL else cast(ing_6 as numeric) end ,
cast(replace(ing_tri,',','.') as numeric) as ing_tri
FROM
raw.inegi_ingresos_2014;

create table clean.enigh_erogaciones as
select 
folioviv,
foliohog,
clave,
case when mes_1 = ' ' then NULL else mes_1 end,
case when mes_2 = ' ' then NULL else mes_2 end,
case when mes_3 = ' ' then NULL else mes_3 end,
case when mes_4 = ' ' then NULL else mes_4 end,
case when mes_5 = ' ' then NULL else mes_5 end,
case when mes_6 = ' ' then NULL else mes_6 end,
case when ero_1 = ' ' then NULL else cast(replace(ero_1,',','.') as numeric) end as ero_1,
case when ero_2 = ' ' then NULL else cast(replace(ero_2,',','.') as numeric) end as ero_2,
case when ero_3 = ' ' then NULL else cast(replace(ero_3,',','.') as numeric) end as ero_3,
case when ero_4 = ' ' then NULL else cast(replace(ero_4,',','.') as numeric) end as ero_4,
case when ero_5 = ' ' then NULL else cast(replace(ero_5,',','.') as numeric) end as ero_5,
case when ero_6 = ' ' then NULL else cast(replace(ero_6,',','.') as numeric) end as ero_6,
case when ero_tri = ' ' then NULL else cast(replace(ero_tri,',','.') as numeric) end as ero_tri
FROM
raw.inegi_erogaciones_2014;

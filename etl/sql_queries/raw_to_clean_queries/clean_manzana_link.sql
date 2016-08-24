CREATE TABLE clean.manzana_link AS
SELECT manzana_id,
       manzana_dist,
       id_unico AS sifode_pub_id
from raw.manzana_link;

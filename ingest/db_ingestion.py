""" Load Sedesol data into an existing schema in a postgres database.

This is a commandline script that can be used to ingest all the raw data from
Sedesol into a Postgres schema. For a description of the input requiredfor this
script, type python3 db_ingestion.py -h in the command line. The mose common
use case will be calling

python3 ingest/db_ingestion.py --all 1

from the 'sedesol' github repository directory.

The base path to the directory containing all the data and schema name are
constants fixed at the start of the script. We assume the presence of the
following files, in subdirectories below the base path defined below. Depending
 on the arguments with which this script is called, not all files may actually
 be necessary.

conf/
  db_profile.yaml
census/
  2015/
   eic2015_01_csv.zip
   eic2015_02_csv.zip
            .
            .
            .
coneval/
  pobreza_14.csv
  indice_de_rezago_social.csv
cuis/
  DOMICILIO_A.csv
  ENCUESTA_A.csv
         .
         .
         .
cve_mapping/
  localidads.dbf
  municipalities.dbf
geo_pub/
  DOM_UNI_PUB_*.rar
home_verification/
  VERIFICACION.rar
imss/
  salario_imss_A.csv
pub/
  PUB_subset.csv
sepomex/
  MX.zip
sifode/
  SIFODE_DOMICILIOS_NOMVIAL_1.csv
  SIFODE_DOMICILIOS_NOMVIAL_2.csv
  SIFODE_DOMICILIOS_NOMVIAL_3.csv
                .
                .
                .
sifode_univ_int/
  SIFODE19_2_INT_VF_A.csv
  SIFODE19_2_UNIV_VF_A.csv
spatial_objects/
  inegi_2010/
    manzanas_rurales/
      01-MANZANAS-RURALES.rar
      02-MANZANAS-RURALES.rar
              .
              .
              .
    manzanas_urbanas/
      01-MANZANAS-URBANAS.rar
      02-MANZANAS-URBANAS.rar
              .
              .
              .
    localidades_rurales/
              .
              .
              .
    localidades_urbanas/
  denue/
    denue_00_11_csv.zip
  manzana_links.csv
  red_nacional_de_caminos_2015/
    conjunto_de_datos/
       estructura/
          estructura.dbf
          estructura.prj
          estructura.shp
          estructura.shpx
       localidad/
          localidad.dbf
          localidad.prj
          localidad.shp
        maniobra_prohibida/
               .
               .
               .
"""

import argparse
import os
import yaml
import db_ingestion_table_funs as dbui

# Constants
BASE_PATH = os.path.join("/mnt", "data", "sedesol", "etl_test")
SCHEMA_NAME = "ingestion_result"

# Parse command line arguments to determine which tables to load
parser = argparse.ArgumentParser()

parser.add_argument("--all", help="Load all the data for the Sedesol problem.")
parser.add_argument("--coneval", help="Load coneval data, giving income estimates at the municipality level.")
parser.add_argument("--cuis", help="Load CUIS data, including survey results collected by SEDESOL.")
parser.add_argument("--cve_mapping", help="Load the mappings from locality and municipality numbers to standardized geographic names.")
parser.add_argument("--denue", help="Load spatial data from DENUE.")
parser.add_argument("--equivalencias", help="Load the data containing equivalences between new and old locality numbers")
parser.add_argument("--home_verification", help="Load home verification data, collected by Prospera surveyers to independently assess household needs")
parser.add_argument("--imss", help="Load independent income estimates from IMSS")
parser.add_argument("--inegi_spatial", help="Load spatial data from the 2010 INEGI census.")
parser.add_argument("--geo_pub", help="Load sample of addresses for individuals in PUB")
parser.add_argument("--geo_sifode", help="Load geographic address SIFODE data.")
parser.add_argument("--pub_sub", help="Load small subset of PUB data")
parser.add_argument("--manzana_link", help="Load link between manzana IDs and PUB id_unico")
parser.add_argument("--partial_census", help="Load partial census information from 2015")
parser.add_argument("--red_nacional", help="Load red_nacional_de_caminos_2015 spatial data.")
parser.add_argument("--sepomex", help="Load SEPOMEX postal data.")
parser.add_argument("--sifode", help="Load SIFODE data.")

parser.parse_args()

# Setup utility functions and password / configuration info
conf = {}
with open("conf/db_profile.yaml", "r") as f:
    db_profile = yaml.load(f)
    conf["USER"] = db_profile["PGUSER"]
    conf["PASSWORD"] = db_profile["PGPASSWORD"]

with open("conf/connection.yaml", "r") as f:
    conn_profile = yaml.load(f)
    conf["HOST"] = conn_profile["CONNECTION_HOST"]

# load the necessary tables
args = parser.parse_args()
if args.coneval or args.all:
    dbui.load_coneval(BASE_PATH, conf, SCHEMA_NAME)
if args.cuis or args.all:
    dbui.load_cuis(BASE_PATH, conf, SCHEMA_NAME)
if args.cve_mapping or args.all:
    dbui.load_cve_mapping(BASE_PATH, conf, SCHEMA_NAME)
if args.denue or args.all:
    dbui.load_denue(BASE_PATH, conf, SCHEMA_NAME)
if args.equivalencias or args.all:
    dbui.load_equivalencias(BASE_PATH, conf, SCHEMA_NAME)
if args.geo_pub or args.all:
    dbui.load_geo_pub(BASE_PATH, conf, SCHEMA_NAME)
if args.geo_sifode or args.all:
    dbui.load_geo_sifode(BASE_PATH, conf, SCHEMA_NAME)
if args.home_verification or args.all:
    dbui.load_home_verification(BASE_PATH, conf, SCHEMA_NAME)
if args.imss or args.all:
    dbui.load_imss(BASE_PATH, conf, SCHEMA_NAME)
if args.inegi_spatial or args.all:
    dbui.load_inegi_spatial(BASE_PATH, conf, SCHEMA_NAME)
if args.partial_census or args.all:
    dbui.load_census(BASE_PATH, conf, SCHEMA_NAME)
if args.pub_sub or args.all:
    dbui.load_pub_sub(BASE_PATH, conf, SCHEMA_NAME)
if args.red_nacional or args.all:
    dbui.load_red_nacional(BASE_PATH, conf, SCHEMA_NAME)
if args.sepomex or args.all:
    dbui.load_sepomex(BASE_PATH, conf, SCHEMA_NAME)
if args.sifode or args.all:
    dbui.load_sifode(BASE_PATH, conf, SCHEMA_NAME)
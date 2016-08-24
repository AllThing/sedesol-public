""" (Sedesol-specific) Functions for ingestion to postgres

This file contains functions to aid ingestion from the raw Sedesol data
to a postgres database schema. See sedesol/ingest/db_ingestion.py for a
description of how to layout files for this script to apply."""

import os

from luigi import configuration
import logging
import logging.config

import db_ingestion_utils as dbu

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


def load_coneval(base_path, conf, schema_name="schema_name"):
    """Load the coneval data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/coneval" should contain a file "pobreza_14.rar"
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading CONEVAL...")
    dbu.csv_to_db_table(os.path.join(base_path, "coneval", "pobreza_14.csv"),
                        conf, "coneval_pobreza", schema_name)
    dbu.csv_to_db_table(os.path.join(base_path, "coneval", 
                        "indice_de_rezago_social.csv"),
                        conf, "indice_de_rezago_social", schema_name)


def load_cuis(base_path, conf, schema_name="schema_name"):
    """Load the CUIS data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/cuis" should contain files "ENCUESTA_A.csv", "PERSONA_A",...
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    tables_to_load = {
        "cuis_encuesta": os.path.join(base_path, "cuis",
            "ENCUESTA_A.csv"),
        "cuis_person_a": os.path.join(base_path, "cuis",
            "PERSONA_A.csv"),
        "cuis_se_vivienda_a": os.path.join(base_path, "cuis", 
            "SE_VIVIENDA_A.csv"),
        "cuis_integrante_a": os.path.join(base_path, "cuis", 
            "INTEGRANTE_A.csv"),
        "cuis_se_integrante_a": os.path.join(base_path, "cuis", 
            "SE_INTEGRANTE_A.csv"),
        "cuis_domicilio_a": os.path.join(base_path, "cuis",
            "DOMICILIO_A.csv"),
        "cuis_vista_encuestador_a": os.path.join(base_path, "cuis",
            "VISITA_ENCUESTADOR_A.csv"),
    }

    logger.info("Loading CUIS data...")
    dbu.csv_to_db_table_wrapper(tables_to_load, conf, schema_name)


def load_home_verification(base_path, conf, schema_name="schema_name"):
    """Load the home verification data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/home_verification" should contain a file
     "home_verification.rar"
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading home verification data...")
    dbu.unpack_in_place(os.path.join(base_path, "home_verification"),
                        "VERIFICACION.rar")

    for year in ["2013", "2014"]:
        cur_path = os.path.join(base_path, "home_verification", 
            "VERIFICACION", year)
        cur_year = "home_verification_" + year

        dbu.dbfs_to_csv(cur_path)
        dbu.merge_csvs_in_dir(cur_path, cur_year + ".csv")
        dbu.csv_to_db_table(os.path.join(cur_path, cur_year + ".csv"), conf,
                            cur_year, schema_name)


def load_imss(base_path, conf, schema_name="schema_name"):
    """Load the IMSS data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/imss" should contain a file "salario_imss_A.csv.rar".
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading IMSS...")
    dbu.csv_to_db_table(os.path.join(base_path, "imss", "salario_imss_A.csv"),
                        conf, "imss_salario", schema_name)


def merge_census(base_path, census_type):
    """ Create separate directories for the persona and vivienda census data

    Rather than simply merging all the CSVs in the unzipped census directory,
    it is important to merge the household (vivienda) and person level
    data separately. This function helps create subdirectories for these types
    of census files, and puts the associated files in them, so that
    dbu.merge_csvs_in_dir() can be applied.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/census/2015" should contained the results of unzipping
     "2015_Partial_Census-2016-05-20.zip".
    :param census_type [string] The name of the prefix used to create
    subdirectories, each containing consistent files.
    :return None
    :rtype None
    :side-effects Creates a subdiectory with the name census_type in the
    census/2015/ diretory, and moves the files with names containing the
    string census_type to this new subdiretory.
    """
    census_dir = os.path.join(base_path, "census", "2015")
    cur_files = [x for x in os.listdir(census_dir) if census_type in x]
    cur_files = [x for x in cur_files if x.endswith("CSV")]

    # move files of the required type
    dbu.move_to_subdir(cur_files, census_dir, census_type)

    # convert to lower case
    subdir_path = os.path.join(census_dir, census_type)
    for csv_file in os.listdir(subdir_path):
        os.rename(os.path.join(subdir_path, csv_file),
                  os.path.join(subdir_path, csv_file.lower()))


def load_census(base_path, conf, schema_name="schema_name"):
    """Load the 2015 partial census data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/census/2015" should contain a file
     "2015_Partial_Census-2016-05-20.zip".
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading 2015 partial census data...")
    dbu.unpack_all_in_dir(os.path.join(base_path, "census", "2015"), "zip")
    merge_census(base_path, "TR_PERSONA")
    merge_census(base_path, "TR_VIVIENDA")

    # merge csvs
    csv_paths = [os.path.join(base_path, "census", "2015", "TR_PERSONA"),
                 os.path.join(base_path, "census", "2015", "TR_VIVIENDA")]
    csv_files = ["partial_census_2015_personas.csv",
                 "partial_census_2015_viviendas.csv"]

    for i, path in enumerate(csv_paths):
        dbu.merge_csvs_in_dir(path, csv_files[i])
        file_without_extension = csv_files[i].split(".")[0]
        cur_file = os.path.join(path, csv_files[i])
        dbu.iconv_conversion(cur_file)

        dbu.csv_to_db_table(cur_file, conf, 
            file_without_extension, schema_name)


def load_geo_sifode(base_path, conf, schema_name="schema_name"):
    """Load the geographical data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/sifode" should contain .rar files with names like
     "SIFODE_DOMICILIOS_NOMVIAL_1.rar"...
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the geographical sifode data to the database.
    """
    logger.info("Loading geographic sifode data...")
    sifode_dir = os.path.join(base_path, "sifode")
    dbu.unpack_all_in_dir(sifode_dir)
    dbu.dbfs_to_csv(sifode_dir)
    dbu.merge_csvs_in_dir(sifode_dir, "sifode.csv")
    dbu.csv_to_db_table(os.path.join(sifode_dir, "sifode.csv"),
                        conf, "geo_cuis", schema_name)


def load_sifode(base_path, conf, schema_name="schema_name"):
    """Load the Sifode data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/sifode_univ_int" should contain two files,
     "SIFODE19_2_INT_VF_A.csv" and "SIFODE19_2_UNIV_VF_A.csv"
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading SIFODE UNIV and INT data...")
    tables_to_load = {
        "sifode_univ": os.path.join(base_path, "sifode_univ_int", 
            "SIFODE19_2_UNIV_VF_A.csv"),
        "sifode_int": os.path.join(base_path, "sifode_univ_int", 
            "SIFODE19_2_INT_VF_A.csv"),
    }
    dbu.csv_to_db_table_wrapper(tables_to_load, conf, schema_name)


def load_sepomex(base_path, conf, schema_name="schema_name"):
    """Load the SEPOMEX data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/sepomex" should contain the file "MX.zip".
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading SEPOMEX data...")
    dbu.unpack_in_place(os.path.join(base_path, "sepomex"), "MX.zip", "zip")

    # additional conversion from tsv to csv
    file_path = os.path.join(base_path, "sepomex", "MX.txt")
    os.system("sed -i 's/\t/,/g' %s" % file_path)
    dbu.csv_to_db_table(file_path, conf, "sepomex", schema_name, False)


def load_geo_pub(base_path, conf, schema_name="schema_name"):
    """Load the geographic information in PUB

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/geo_pub" should contain *.rar files containing the data.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading geographic pub data...")
    dbu.unpack_all_in_dir(os.path.join(base_path, "geo_pub"))
    dbu.dbfs_to_csv(os.path.join(base_path, "geo_pub"))
    dbu.merge_csvs_in_dir(os.path.join(base_path, "geo_pub"), "geo_pub.csv")
    dbu.csv_to_db_table(os.path.join(base_path, "geo_pub", "geo_pub.csv"),
                        conf, "geo_pub", schema_name)


def load_pub_sub(base_path, conf, schema_name="schema_name"):
    """Load a subset of the PUB data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/pub/" should contain PUB_subset.csv.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables cuis_* to the database specified in conf.
    """
    logger.info("Loading PUB subset...")
    file_path = os.path.join(base_path, "pub", "PUB_subset.csv")
    os.system("sed -i 's/|/,/g' %s" % file_path)
    dbu.csv_to_db_table(file_path, conf, "pub_sub", schema_name)


def load_inegi_spatial(base_path, conf, schema_name="schema_name"):
    """Load the 2010 census spatial data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/spatial_objects/inegi_2010" should contain four folders,
     manzanas_rurales, manzanas_urbanas, localidades_rurales, and
     localidades_urbanas, each with .rar files containing spatial information
     for each state.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables inegi spatial data to the database
     specified in conf.
    """
    logger.info("Loading INEGI spatial data...")
    all_data = ["manzanas_rurales", "manzanas_urbanas", "localidades_rurales",
                "localidades_urbanas"]
    for cur_data in all_data:
        logger.info("Loading %s..." % cur_data)
        rar_dir = os.path.join(base_path, "spatial_objects", 
            "inegi_2010", cur_data)
        dbu.load_spatial_from_archive(rar_dir, conf, schema_name, cur_data,
                                      "latin1")


def load_denue(base_path, conf, schema_name="schema_name"):
    """Load the DENUE spatial data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/spatial_objects/denue" should contain .rar files containing
     spatial information.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the denue spatial data to the database 
     specified in conf.
    """
    zip_dir = os.path.join(base_path, "spatial_objects", "denue")
    dbu.load_spatial_from_archive(zip_dir, conf, schema_name, "denue",
                                  "latin1", "zip")


def load_red_nacional(base_path, conf, schema_name="schema_name"):
    """Load the red_nacional_de_caminos_2015 spatial dta

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables inegi spatial data to the database
     specified in conf.
    """
    logger.info("Loading red nacional data...")
    all_data = ["estructura", "localidad", "maniobra_prohibida",
                "poste_de_referencia", "puente", "red", "sitio_de_interes",
                "tarifas", "transbordador", "union"]
    for cur_data in all_data:
        logger.info("Loading %s..." % cur_data)
        shp_dir = os.path.join(base_path, "spatial_objects",
                               "red_nacional_de_caminos_2015",
                               "conjunto_de_datos", cur_data)
        dbu.load_geo(shp_dir, conf, schema_name, cur_data, "latin1")


def load_cve_mapping(base_path, conf, schema_name="schema_name"):
    """Load the CVE mapping catalogs

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database. These data are useful for normalizing the names of localidads
    and municipios across data sets.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. This function
     specifically requires a subdirectory cve_mapping/ containing the files
     localidads.dbf and municipalities.dbf
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the cve mapping catalogsto the database
    """
    logger.info("Loading the CVE mapping data...")
    cur_path = os.path.join(base_path, "cve_mapping")
    dbu.dbfs_to_csv(cur_path)

    tables_to_load = {
        "cve_mapping_localities": os.path.join(cur_path, 
            "localidads.csv"),
        "cve_mapping_municipalities": os.path.join(cur_path, 
            "municipalities.csv"),
    }

    dbu.csv_to_db_table_wrapper(tables_to_load, conf, schema_name)


def load_equivalencias(base_path, conf, schema_name="schema_name"):
    """Load equivalencias data

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. This specifically
     needs a directory called equivalencias containing the file
     CATALOGO_LOCALIDADES_EQUIVALENCIAS.rar
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the tables equivalencias data to the database specified
     in conf.
    """
    archive_dir = os.path.join(base_path, "equivalencias")
    dbu.unpack_all_in_dir(archive_dir)
    equiv_file = os.path.join(archive_dir, "TABLA_DE_EQUIVALENCIA_ABR16")
    dbu.iconv_conversion(equiv_file + ".dbf")
    dbu.dbfs_to_csv(archive_dir)

    dbu.csv_to_db_table(equiv_file + ".csv", conf, 
        "equivalencias", schema_name)


def load_manzana_link(base_path, conf, schema_name="schema_name"):
    """Load the link between id_unico and geocoded manzana ID

    This function assumes the directory structure described at the start of
    this module. Further, it uses a dictionary called conf to connect to the
    database.

    :param base_path [string] The path to the directory containing the
     subdirectories described at the start of this module. Specifically,
     base_path + "/spatial_objects/" should contain a file "manzana_link.csv"
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD", and
     "HOST". See dbu.csv_to_db_cmds() describing how the connection is
     established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Loads the table manzana_link to the database.
    """
    logger.info("Loading manzana links...")
    data_path = os.path.join(base_path, "spatial_objects", "manzana_link.csv")
    dbu.csv_to_db_table(data_path, conf, "manzana_link", schema_name)

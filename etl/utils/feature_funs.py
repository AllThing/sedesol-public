"""
Functions for generating individual sets of features. This module is most
heavily used by the GetFeatures() task.
"""

import logging
import logging.config
import json
from collections import OrderedDict
import warnings
import math
import datetime as dt

import pandas as pd
import numpy as np
import sklearn.preprocessing as ppr
from sklearn.cluster import KMeans

from luigi import configuration
import utils.pg_sedesol as pg_sed

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")

###############################################################################
# High-level feature generation functions
###############################################################################


def parse_features_json(json_path):
    """
    Parse a configuration JSON specifying features to include

    :param string json_path A string giving the path to the JSON file specifying
     features to include
    :return dict A dictionary with two keys, "original" and "derived" giving
     lists with names of features to include. "original" features are those
     contained in the semantic.semantic database, while "derived" are those
     that need to be generated through separate functions.
    :rtype dict
    """

    with open(json_path) as json_file:
        features = json.load(json_file)

    original_features = []
    derived_features = []

    for _, v_group in features.items():
        for feature, feature_indic in v_group["original"].items():
            if feature_indic:
                original_features.append(feature)

        for feature, feature_indic in v_group["derived"].items():
            if feature_indic:
                derived_features.append(feature)

    return {"derived": derived_features, "original": original_features}


def get_features(json_path, schema_name, subset_type, filter_condition=None,
                 grouping_cols=None):
    """
    Wrapper function to get both original and derived features

    This wraps the get_original_features() function, along with any get_*
    function used to get specific derived features specified in the features
    json.

    :param string json_path A string giving the path to the JSON file specifying
     features to include
    :param string subset_type The table in the semantic schema to use in
     generating features. This is specified by the subset_type field in the
     luigi.cfg file, usually.
    :param string filter_condition A condition specifying the subset of rows to
     filter down, within the specified table. This is useful when combined
     with the cv_index column for cross-validation.
    :param features_dir string The path to which to write the leave-fold-out
     feature csv files and schema
    :param features_name string The basename of the csv files containing the
     features. We assume this is in the same directory as features_dir.
    :return None
    :side-effects Writes features and the associated schema to the features_dir.
    """
    # get features available in the DB
    features = parse_features_json(json_path)
    cur_feature_strings = ",".join(features["original"])
    logger.info("Reading %s from semantic db", cur_feature_strings)

    if len(features["original"]) == 0:
        X = None
    else:
        X = pg_sed.get_original_features(features["original"], schema_name,
                                         subset_type, filter_condition,
                                         grouping_cols)

    # get derived features
    for feature in features["derived"]:
        logger.info("Generating %s feature", feature)
        cur_fun = globals()["get_" + feature]
        derived_data_input = globals()["get_data_" + feature]
        u = derived_data_input(schema_name, subset_type, filter_condition,
                               grouping_cols)
        X = pd.concat([X, cur_fun(u)], axis=1)

    return X


###############################################################################
# Utilities for computing derived features
###############################################################################


def get_n_rows_in_table(schema_name, table_name, filter_condition=None):
    """
    Get the number of rows in a database table

    :param string schema_name The name of the schema in which the table to get
     the number of rows from lives.
    :param string table_name The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.

    :return int The number of rows in the semantic.table_name table that
     meet the specified filter condition.
    :rtype int
    """
    if filter_condition is None:
        filter_condition = ""

    conn = pg_sed.db_connection()
    query = "SELECT COUNT(*) FROM %s.%s %s;" % (
        schema_name, table_name, filter_condition
    )

    n_rows_df = pd.io.sql.read_sql(query, conn)
    return n_rows_df['count'][0]

###############################################################################
# Functions for computing derived features
###############################################################################


def get_years_of_education(input_data):
    """
    Number of years of education for most educated person in household

    See aux function years_of_education().

    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series A series giving the number of years of education for the
    most educated person in the household.
    """
    input_data["years_of_education"] = input_data.apply(years_of_education,
                                                        axis=1)
    group_ix = ["folioviv", "expansion_ix"]
    max_years_of_education = input_data.groupby(group_ix)["years_of_education"].max()

    # convert to Series without indices
    max_years_of_education = max_years_of_education.reset_index()
    return max_years_of_education["years_of_education"]


def years_of_education(row):
    # Useful to have this as a separate function since this feature is used for other features
    education_level = row['poblacion_nivel']
    education_grade = row['poblacion_grado']

    if education_level == 1 and education_grade <= 3:
         years = 0
    elif education_level == 2 and education_grade <= 6:
        years = education_grade
    elif education_level == 3 and education_grade <= 3:
        years = education_grade + 6
    elif education_level == 4 and education_grade <= 4:
        years = education_grade + 9
    elif education_level == 5 and education_grade <= 4:
        years = education_grade + 9
    elif education_level == 6 and education_grade <= 4:
        years = education_grade + 6
    elif education_level == 7 and education_grade <= 4:
        years = education_grade + 9
    elif education_level == 8 and education_grade <= 4:
        years = education_grade + 12
    elif education_level == 9 and education_grade <= 6:
        years = education_grade + 12
    elif education_level == 10 and education_grade <= 6:
        years = education_grade + 16
    else:
        years = float("nan")
    return years

def get_data_years_of_education(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for years_of_education: see aux function
    needs columns: 'home_id','person_id', 'education_level','education_grade'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Years of education featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    WITH education AS (

    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    cv_index,

    CASE WHEN poblacion_nivel = ' ' THEN NULL
    WHEN poblacion_nivel = '99' THEN NULL
    ELSE CAST(poblacion_nivel AS NUMERIC)
    END AS poblacion_nivel,

    CASE WHEN poblacion_grado = ' ' THEN NULL
    WHEN poblacion_grado = '99' THEN NULL
    ELSE CAST(poblacion_grado AS NUMERIC)
    END AS poblacion_grado

    FROM %s.%s
    )

    SELECT folioviv,
    numren,
    expansion_ix,
    poblacion_nivel,
    poblacion_grado
    FROM education
    %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    education_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return education_data

def get_own_toilet(input_data):
    """Derived feature function: see aux function
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['own_toilet'] = input_data.apply(own_toilet, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    with_own_toilet = input_data.groupby(group_ix)['own_toilet'].aggregate(max)

    # convert to Series without indices
    with_own_toilet = with_own_toilet.reset_index()
    print(with_own_toilet)
    return with_own_toilet

def own_toilet(row):
    """aux whether family has own toilet connected to main network"""
    return (row['vivi_excusado'] == 1 and row['vivi_uso_compar'] == 2 and row['vivi_sanit_agua'] == 1)

def get_data_own_toilet(schema_name, subset_type,
                            filter_condition=None,
                            grouping_cols = None,):
    """ Generates data for own_toilet, needs
    columns: vivi_excusado vivi_uso_compar vivi_sanit_agua

    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Own toilet featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()

    # specific query for own_toilet
    query = """SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_excusado,
    vivi_uso_compar,
    vivi_sanit_agua
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_toilet_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_toilet_data

def get_no_telephone(input_data):
    """Derived feature function: whether a house has telephone or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_telephone'] = input_data.apply(no_telephone, axis=1)
    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_telephone = input_data.groupby(group_ix)["no_telephone"].max()
    # convert to Series without indices
    have_no_telephone = have_no_telephone.reset_index()
    return have_no_telephone

def no_telephone(row):
    """
    aux compute how many telephones a house has
    """
    try:
        landline = int(row['hogares_telefono'])
    except ValueError or TypeError:
        landline = None
    if landline > 0:
        return 0
    else:
        return 1

def get_data_no_telephone(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for telephone lack: see aux function
    needs columns: 'hogares_telefono'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Telephone Appliance Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_telefono
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_telephone_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_telephone_data

def get_no_refridgerator(input_data):
    """Derived feature function: whether a house has refridgerator or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_refridgerator'] = input_data.apply(no_refridgerator, axis=1)
    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_refridgerator = input_data.groupby(group_ix)["no_refridgerator"].max()
    # convert to Series without indices
    have_no_refridgerator = have_no_refridgerator.reset_index()
    return have_no_refridgerator

def no_refridgerator(row):
    """
    aux compute whether a house has refridgerator or not
    """
    try:
        fridges = int(row['hogares_num_refri'])
    except ValueError or TypeError:
        fridges = None
    if fridges > 0:
        return 0
    else:
        return 1


def get_data_no_refridgerator(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for refridgerator lack: see aux function
    needs columns: 'hogares_num_refri'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Refridgerator Appliance Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_num_refri
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_refridgerator_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_refridgerator_data

def get_no_dvdvid(input_data):
    """Derived feature function: whether a house has dvd / video or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_dvdvid'] = input_data.apply(no_dvdvid, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_dvdvid = input_data.groupby(group_ix)["no_dvdvid"].max()
    # convert to Series without indices
    have_no_dvdvid = have_no_dvdvid.reset_index()
    return have_no_dvdvid

def no_dvdvid(row):
    """
    aux compute whether a house has dvd / video or not
    """
    try:
        dvd = int(row['hogares_num_dvd'])
        video = int(row['hogares_num_video'])
        dvdvid = dvd + video
    except ValueError or TypeError:
        dvdvid = None
    if dvdvid > 0:
        return 0
    else:
        return 1

def get_data_no_dvdvid(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for dvd and video lack: see aux function
    needs columns: 'hogares_num_dvd','hogares_num_video'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Dvd and Video Appliance Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_num_dvd,
    hogares_num_video
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_dvdvid_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_dvdvid_data

def get_no_vehicle(input_data):
    """Derived feature function: whether a house has dvd / video or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_vehicle'] = input_data.apply(no_vehicle, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_vehicle = input_data.groupby(group_ix)["no_vehicle"].max()
    # convert to Series without indices
    have_no_vehicle = have_no_vehicle.reset_index()
    return have_no_vehicle

def no_vehicle(row):
    """
    aux compute how many cars, vans, pickups and motorcicles a house has
    """
    try:
        auto = int(row['hogares_num_auto'])
        van = int(row['hogares_num_van'])
        pickup = int(row['hogares_num_pickup'])
        vehicle = auto + van + pickup
    except ValueError or TypeError:
        vehicle = None
    if vehicle > 0:
        return 0
    else:
        return 1

def get_data_no_vehicle(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for vehicle lack: see aux function
    needs columns: hogares_num_auto,hogares_num_van,hogares_num_pickup,hogares_num_moto
    NOTE don't know if we should take into account (?) num_bici num_trici num_carret num_canoa num_otro
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Vehicle Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_num_auto,
    hogares_num_van,
    hogares_num_pickup
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_vehicle_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_vehicle_data

def get_no_computer(input_data):
    """Derived feature function: whether a house has dvd / video or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_computer'] = input_data.apply(no_computer, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_computer = input_data.groupby(group_ix)["no_computer"].max()
    # convert to Series without indices
    have_no_computer = have_no_computer.reset_index()
    return have_no_computer

def no_computer(row):
    """
    aux compute how many cars, vans, pickups and motorcicles a house has
    """
    try:
        computer = int(row['hogares_num_compu'])
    except ValueError or TypeError:
        computer =  None
    if computer > 0:
        return 0
    else:
        return 1

def get_data_no_computer(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for computer lack: see aux function
    needs columns: hogares_num_compu
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Computer Appliance Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_num_compu
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_computer_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_computer_data

def get_no_microwave(input_data):
    """Derived feature function: whether a house has dvd / video or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['no_microwave'] = input_data.apply(no_microwave, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    have_no_microwave = input_data.groupby(group_ix)["no_microwave"].max()
    # convert to Series without indices
    have_no_microwave = have_no_microwave.reset_index()
    return have_no_microwave

def no_microwave(row):
    """
    aux compute how many microwaves a house has
    """
    try:
        microwave = int(row['hogares_num_micro'])
        return
    except ValueError or TypeError:
        microwave = None
    if microwave > 0:
        return 0
    else:
        return 1

def get_data_no_microwave(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for microwave lack: see aux function
    needs columns: hogares_num_micro
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Microwave Appliance Lack featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_num_micro
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    own_microwave_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return own_microwave_data

def get_carbon_cooking_fuel(input_data):
    """Derived feature function: whether a house uses carbon as carbon_cooking_fuel
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['uses_carbon_cooking_fuel'] = input_data.apply(carbon_cooking_fuel, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    uses_carbon_cooking_fuel = input_data.groupby(group_ix)["uses_carbon_cooking_fuel"].max()
    # convert to Series without indices
    uses_carbon_cooking_fuel = uses_carbon_cooking_fuel.reset_index()
    return uses_carbon_cooking_fuel

def carbon_cooking_fuel(row):
    """
    aux compute whether a house uses carbon as carbon_cooking_fuel
    """
    try:
        fuel = int(row['vivi_combustible'])
        if fuel in [1,2,6]:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_carbon_cooking_fuel(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for carbon_cooking_fuel: see aux function
    needs columns: vivi_combustible
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Cooking Fuel featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_combustible
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    carbon_cooking_fuel_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return carbon_cooking_fuel_data

def get_firm_floor(input_data):
    """Derived feature function: whether a house has floor made of cement, not covered
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['has_firm_floor'] = input_data.apply(firm_floor, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    has_firm_floor = input_data.groupby(group_ix)["has_firm_floor"].max()
    # convert to Series without indices
    has_firm_floor = has_firm_floor.reset_index()
    return has_firm_floor

def firm_floor(row):
    """
    aux compute whether the house has firm floor
    """
    try:
        floor_type = int(row['vivi_mat_pisos'])
        if floor_type == 2:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_firm_floor(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for firm_floor: see aux function
    needs columns: vivi_mat_pisos
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Firm Floor featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_mat_pisos
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    floor_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return floor_data

def get_covered_floor(input_data):
    """Derived feature function: whether a house has covered floor
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['has_covered_floor'] = input_data.apply(covered_floor, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    has_covered_floor = input_data.groupby(group_ix)["has_covered_floor"].max()
    # convert to Series without indices
    has_covered_floor = has_covered_floor.reset_index()
    return has_covered_floor

def covered_floor(row):
    """
    aux compute whether the house has covered floor
    """
    try:
        floor_type = int(row['vivi_mat_pisos'])
        if floor_type == 3:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_covered_floor(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for covered_floor: see aux function
    needs columns: vivi_mat_pisos
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Covered Floor featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_mat_pisos
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    floor_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return floor_data

def get_n_rooms_without_kitchen(input_data):
    """Derived feature function: number of rooms in house without kitchen unless they sleep there and it's the only room
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['rooms_without_kitchen'] = input_data.apply(n_rooms_without_kitchen, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    rooms_without_kitchen = input_data.groupby(group_ix)['rooms_without_kitchen'].aggregate(sum)

    # convert to Series without indices
    rooms_without_kitchen = rooms_without_kitchen.reset_index()
    return rooms_without_kitchen

def n_rooms_without_kitchen(row):
    """
    aux to get number of rooms in house without kitchen unless they sleep there and it's the only room
    """
    try:
        kitchen = int(row['vivi_cocina'])
        nrooms = int(row['vivi_num_cuarto'])
        sleeps_kitchen = int(row['vivi_cocina_dor'])
        if kitchen == 2:
            return nrooms
        elif (kitchen == 1 and sleeps_kitchen == 1):
            return nrooms
        elif (kitchen == 1 and sleeps_kitchen == 2 and nrooms>1):
            return nrooms - 1
        elif (kitchen == 1 and sleeps_kitchen == 2 and nrooms == 1):
            return 1
        else:
            return nrooms
    except ValueError or TypeError:
        return None

def get_data_n_rooms_without_kitchen(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for n_rooms_without_kitchen feature, needs
    columns:     vivi_cocina, vivi_num_cuarto, vivi_cocina_Dor
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Number of Rooms featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_cocina,
    vivi_num_cuarto,
    vivi_cocina_dor
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    rooms_without_kitchen = pd.io.sql.read_sql(query, connection)
    connection.close()
    return rooms_without_kitchen

def get_house_owner(input_data):
    """Derived feature function: whether a house is owned by the inhabs or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['is_house_owner'] = input_data.apply(house_owner, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    is_house_owner = input_data.groupby(group_ix)["is_house_owner"].max()
    # convert to Series without indices
    is_house_owner = is_house_owner.reset_index()
    return is_house_owner

def house_owner(row):
    """
    aux compute whether the house is owned by the inhabitants
    """
    try:
        owner = int(row['vivi_tenencia'])
        if owner in [3,4]:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_house_owner(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for house_owner: see aux function
    needs columns: vivi_tenencia
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("House Owner featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_tenencia
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    house_owner_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return house_owner_data

def get_house_rented(input_data):
    """Derived feature function: whether a house is owned by the inhabs or not
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['is_house_rented'] = input_data.apply(house_rented, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    is_house_rented = input_data.groupby(group_ix)["is_house_rented"].max()
    # convert to Series without indices
    is_house_rented = is_house_rented.reset_index()
    return is_house_rented

def house_rented(row):
    """
    aux compute whether the house is owned by the inhabitants
    """
    try:
        rented = int(row['vivi_tenencia'])
        if rented == 1:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_house_rented(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for house_rented: see aux function
    needs columns: vivi_tenencia
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("House Rented featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_tenencia
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    house_rented_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return house_rented_data

def get_log_inhabitants(input_data):
    """Derived feature function: log of individuals living on a household
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    group_ix = ["folioviv", "expansion_ix"]
    household_level_data = input_data.groupby(group_ix)

    return household_level_data[['poblacion_parentesco']].agg(['count']).apply(np.log)

def get_data_log_inhabitants(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for inhabitants, needs
    columns: poblacion_parentesco

    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Log Inhabitants featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_parentesco
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    log_inhab_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return log_inhab_data

def get_demographic_dependency(input_data):
    """Derived feature function: log of individuals living on a household
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""
    # create auxiliary columns related to the age of every person in household
    input_data['elder'] = input_data.apply(elder, axis=1)
    input_data['child'] = input_data.apply(child, axis=1)
    input_data['not_elder_nor_child'] = input_data.apply(not_elder_nor_child, axis=1)

    group_ix = ["folioviv", "expansion_ix"]

    input_data['elder_total'] = input_data.groupby(group_ix)['elder'].aggregate(sum).reset_index()
    input_data['child_total'] = input_data.groupby(group_ix)['child'].aggregate(sum).reset_index()
    input_data['not_elder_nor_child_total'] = input_data.groupby(group_ix)['not_elder_nor_child'].aggregate(sum).reset_index()

    # function doesn't play very nice when applied to grouped dataframes
    input_data['demographic_dependency'] = input_data.apply(demographic_dependency, axis = 1)
    # so grouping again
    household_level_data_enriched = input_data.groupby(group_ix)
    # convert to Series without indices
    household_level_data_enriched = household_level_data_enriched.reset_index()

    return household_level_data_enriched['demographic_dependency']


def demographic_dependency(row):
    if row['not_elder_nor_child'] > 0:
        return (row['elder_total'] + row['child_total']) / row['not_elder_nor_child_total']
    else: # TODO what to impute here? 0 doesn't make sense for the feature
        return 0

def elder(row):
    """
    aux compute whether a person is over 65
    """
    try:
        age = int(row['poblacion_edad'])
        return row['poblacion_edad'] >= 65
    except ValueError or TypeError:
        return None

def child(row):
    """
    aux compute whether a person is under 15
    """
    try:
        age = int(row['poblacion_edad'])
        return row['poblacion_edad'] <= 15
    except ValueError or TypeError:
        return None

def not_elder_nor_child(row):
    """
    aux compute whether a person is between 16 and 64 years of age
    """
    try:
        age = int(row['poblacion_edad'])
        return row['poblacion_edad'] < 65 and row['poblacion_edad'] > 15
    except ValueError or TypeError:
        return None


def get_data_demographic_dependency(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for demographic dependency feature, needs
    columns: 'poblacion_edad'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Demographic Dependency featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_edad
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    inhabitants_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return inhabitants_data

def get_complete_studies(input_data):
    """Derived feature function: indicates whether the avg of years of studies
    of head of household and partner is > 9
    Requires years_of_education feature to be computed beforehand
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    #create aux column years_of_education first
    input_data["years_of_education"] = input_data.apply(years_of_education,
                                                        axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    aux_education_data = input_data.loc[input_data['poblacion_parentesco'].isin([1,2])].groupby(group_ix)
    average_heads_education = aux_education_data[['years_of_education']].apply(np.mean)
    # convert to Series without indices
    average_heads_education = average_heads_education.reset_index()
    return average_heads_education.apply(lambda data: data['years_of_education'] >= 9 , axis=1)

def get_incomplete_studies(input_data):
    """Derived feature function: indicates whether the avg of years of studies
    of head of household and partner is between 6 and 9
    Requires years_of_education feature to be computed beforehand
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    #create aux column years_of_education first
    input_data["years_of_education"] = input_data.apply(years_of_education,
                                                        axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    aux_education_data = input_data.loc[input_data['poblacion_parentesco'].isin([1,2])].groupby(group_ix)
    average_heads_education = aux_education_data[['years_of_education']].apply(np.mean)
    # convert to Series without indices
    average_heads_education = average_heads_education.reset_index()
    return average_heads_education.apply(lambda data: data['years_of_education'] > 6 and data['years_of_education'] < 9, axis=1)

def get_data_complete_studies(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for complete_studies: see aux function
    needs columns: 'home_id','person_id', 'education_level','education_grade'

    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Years of education featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    WITH education AS (
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    poblacion_parentesco,
    expansion_ix,
    cv_index,
    CASE WHEN poblacion_nivel = ' ' THEN NULL
    WHEN poblacion_nivel = '99' THEN NULL
    ELSE CAST(poblacion_nivel AS NUMERIC)
    END AS poblacion_nivel,
    CASE WHEN poblacion_grado = ' ' THEN NULL
    WHEN poblacion_grado = '99' THEN NULL
    ELSE CAST(poblacion_grado AS NUMERIC)
    END AS poblacion_grado
    FROM %s.%s
    )
    SELECT folioviv,
    numren,
    poblacion_parentesco,
    expansion_ix,
    poblacion_nivel,
    poblacion_grado
    FROM education
    %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    education_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return education_data

def get_data_incomplete_studies(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for incomplete_studies: see aux function
    needs columns: 'home_id','person_id', 'education_level','education_grade'

    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Years of education featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    WITH education AS (
    SELECT DISTINCT ON (folioviv, numren, poblacion_parentesco, expansion_ix)
    folioviv,
    numren,
    poblacion_parentesco,
    expansion_ix,
    cv_index,
    CASE WHEN poblacion_nivel = ' ' THEN NULL
    WHEN poblacion_nivel = '99' THEN NULL
    ELSE CAST(poblacion_nivel AS NUMERIC)
    END AS poblacion_nivel,
    CASE WHEN poblacion_grado = ' ' THEN NULL
    WHEN poblacion_grado = '99' THEN NULL
    ELSE CAST(poblacion_grado AS NUMERIC)
    END AS poblacion_grado
    FROM %s.%s
    )
    SELECT folioviv,
    numren,
    poblacion_parentesco,
    expansion_ix,
    poblacion_nivel,
    poblacion_grado
    FROM education
    %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    education_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return education_data

def get_seguro_popular_receivers(input_data):
    """Derived feature function: number of indiv in a household that have seguro popular
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    group_ix = ["folioviv", "expansion_ix"]
    people_with_seguro_popular = input_data.groupby(group_ix)['poblacion_segpop'].aggregate(sum)

    # convert to Series without indices
    people_with_seguro_popular = people_with_seguro_popular.reset_index()
    return people_with_seguro_popular

def get_data_seguro_popular_receivers(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for seguro_popular_receivers feature, needs
    columns: 'poblacion_segpop'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Seguro Popular Receivers featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_segpop::numeric
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,        filter_condition
    )

    seguro_popular_receivers_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return seguro_popular_receivers_data

def get_people_with_health_services(input_data):
    """Derived feature function: whether at least 1 indiv in a household that have health as a benefit
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['with_health_services'] = input_data.apply(people_with_health_services, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    with_health_services = input_data.groupby(group_ix)['with_health_services'].aggregate(max)

    # convert to Series without indices
    with_health_services = with_health_services.reset_index()
    return with_health_services

def people_with_health_services(row):
    """
    aux to get wheter people have health services or not
    """
    try:
        med_attn = int(row['poblacion_atemed'])
        imss = int(row['poblacion_inst_1'])
        issste = int(row['poblacion_inst_2'])
        issste_state = int(row['poblacion_inst_3'])
        pemex = int(row['poblacion_inst_4'])
        others = int(row['poblacion_inst_5'])
        work_benefit = int(row['poblacion_inscr_1'])
        retirement_benefit = int(row['poblacion_inscr_2'])

        if ( med_attn == 1 and (imss == 1 or issste == 1 or issste_state == 1 or pemex == 1 or others == 1 ) and (work_benefit == 1 or retirement_benefit == 1) ):
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_people_with_health_services(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for workers_with_health_services feature, needs
    columns:     poblacion_atemed,poblacion_inst_1,poblacion_inst_2,poblacion_inst_3,poblacion_inst_4,poblacion_inst_5,poblacion_inscr_1,poblacion_inscr_2
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """


    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Social Security featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_atemed,
    poblacion_inst_1,
    poblacion_inst_2,
    poblacion_inst_3,
    poblacion_inst_4,
    poblacion_inst_5,
    poblacion_inscr_1,
    poblacion_inscr_2
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    people_with_health_services_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return people_with_health_services_data

def get_head_indep_other_social_sec(input_data):
    """Derived feature function: head of household is independent worker but someone in the household gets health as a work benefit
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    # first get who has health as a work benefit
    input_data['health_services'] = input_data.apply(people_with_health_services, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    grouped_data = input_data.groupby(group_ix)

    health_services_indep = grouped_data.apply(head_indep_other_social_sec, axis=1)

    # convert to Series without indices
    health_services_indep = health_services_indep.reset_index()
    return health_services_indep

def head_indep_other_social_sec(group):
    """
    this feature is True if head of household has 'independent_work' = True
    and any of the non head of household people receive health benefits, and
    False otherwise
    """
    household_head_indep_work = False # By default, this is False --- there
                                      # could be households in our data with no
                                      # head
    household_head_indep_work_list = group.loc[group['poblacion_parentesco'].isin([1])]['trabajos_indep']
    if len(household_head_indep_work_list) > 0:
        household_head_indep_work = household_head_indep_work_list.any()
    health_services_non_head_any = False # By default, False because there can be
                                         # households that only have the head
    health_services_non_head_list = group.loc[~group['poblacion_parentesco'].isin([1])]['health_services']
    if len(health_services_non_head_list) > 0: # Non-head people
        health_services_non_head_any = health_services_non_head_list.any()

    return (household_head_indep_work and health_services_non_head_any)


def get_data_head_indep_other_social_sec(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for head_indep_other_social_sec feature, needs
    columns:     poblacion_parentesco,trabajos_indep,poblacion_atemed,poblacion_inst_1,poblacion_inst_2,
    poblacion_inst_3,poblacion_inst_4,poblacion_inst_5,poblacion_inscr_1, poblacion_inscr_2
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Head of Household Indep Worker but other with Health Services featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_parentesco,
    trabajos_indep,
    poblacion_atemed,
    poblacion_inst_1,
    poblacion_inst_2,
    poblacion_inst_3,
    poblacion_inst_4,
    poblacion_inst_5,
    poblacion_inscr_1,
    poblacion_inscr_2

    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    head_indep_other_social_sec_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return head_indep_other_social_sec_data

def get_subordinate_workers(input_data):
    """Derived feature function: number of indiv in a household that have subordinate work
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    group_ix = ["folioviv", "expansion_ix"]
    people_with_work_supervisor = input_data.groupby(group_ix)['trabajos_subor'].aggregate(sum)

    # convert to Series without indices
    people_with_work_supervisor = people_with_work_supervisor.reset_index()
    return people_with_work_supervisor

def get_data_subordinate_workers(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for subordinate_workers feature, needs
    columns: 'trabajos_subor'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Subordinate work featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    trabajos_subor::numeric
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,        filter_condition
    )

    subordinate_work_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return subordinate_work_data

def get_independent_workers(input_data):
    """Derived feature function: number of indiv in a household that have independent work
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    group_ix = ["folioviv", "expansion_ix"]
    people_with_independent_work = input_data.groupby(group_ix)['trabajos_indep'].aggregate(sum)

    # convert to Series without indices
    people_with_independent_work = people_with_independent_work.reset_index()
    return people_with_independent_work

def get_data_independent_workers(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for independent_workers feature, needs
    columns: 'trabajos_indep'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Independent work featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    case when trabajos_indep = ' ' then NULL else trabajos_indep::numeric end as trabajos_indep
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    independent_work_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return independent_work_data

def get_workers_with_no_pay(input_data):
    """Derived feature function: number of indiv in a household that work for no pay
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['works_for_no_pay'] = input_data.apply(workers_with_no_pay, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    people_work_for_no_pay = input_data.groupby(group_ix)['works_for_no_pay'].aggregate(sum)

    # convert to Series without indices
    people_work_for_no_pay = people_work_for_no_pay.reset_index()
    return people_work_for_no_pay

def workers_with_no_pay(row):
    """
    aux to get number of people in a household that work for no pay
    """
    try:
        tipo_pago = int(row['trabajos_pago'])
        if (tipo_pago in [2,3] ):
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_workers_with_no_pay(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for workers_with_no_pay feature, needs
    columns: 'trabajos_pago' trabajos_subor trabajos_indep
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Workers with no Pay featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    case when trabajos_pago= ' ' then NULL else trabajos_pago end as trabajos_pago,
    case when trabajos_subor= ' ' then NULL else trabajos_subor end as trabajos_subor,
    case when trabajos_indep= ' ' then NULL else trabajos_indep end as trabajos_indep
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    workers_with_no_pay_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return workers_with_no_pay_data

def get_adult_food_deficiency(input_data):
    """Derived feature function: whether a house has adults skipping meals or eating once or less
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['has_adult_food_deficiency'] = input_data.apply(has_adult_food_deficiency, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    with_adult_food_deficiency = input_data.groupby(group_ix)["has_adult_food_deficiency"].max()
    # convert to Series without indices
    with_adult_food_deficiency = with_adult_food_deficiency.reset_index()
    return with_adult_food_deficiency

def has_adult_food_deficiency(row):
    """
    aux compute whether there are adults with food deficiency
    """
    try:
        skipped_meals = int(row['hogares_acc_alim5'])
        ate_less_equal_once = int(row['hogares_acc_alim8'])
        if skipped_meals == 1 or ate_less_equal_once == 1: #if any of the two is 1
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_adult_food_deficiency(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for adult_food_deficiency: see aux function
    needs columns: hogares_acc_alim5, hogares_acc_alim8
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """
    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("adult_food_deficiency featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    hogares_acc_alim5,
    hogares_acc_alim8
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    adult_food_deficiency_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return adult_food_deficiency_data

def get_fertile_women(input_data):
    """Derived feature function: number of fertile women in a household
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['fertile_women'] = input_data.apply(fertile_women, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    number_of_fertile_women = input_data.groupby(group_ix)['fertile_women'].aggregate(sum)

    # convert to Series without indices
    number_of_fertile_women = number_of_fertile_women.reset_index()
    return number_of_fertile_women

def fertile_women(row):
    """
    aux compute whether a person is a woman and is in reproductive age
    assumes a column 'age' to be calculated first
    """
    try:
        gender = int(row['poblacion_sexo'])
        age = int(row['poblacion_edad'])
        return gender == 2 and age < 50 and age >=15
    except ValueError or TypeError:
        return None

def get_data_fertile_women(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for fertile_women feature, needs
    columns: poblacion_edad,poblacion_sexo
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Fertile Women featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    poblacion_edad,
    case when poblacion_sexo= ' ' then NULL else poblacion_sexo end as poblacion_sexo
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    fertile_women_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return fertile_women_data

def get_localidades_latlong(input_data):
    for cur_ix, _ in enumerate(input_data.columns.values):
        cur_median = input_data.ix[:, cur_ix].median()
        input_data.ix[:, cur_ix] = input_data.ix[:, cur_ix].fillna(cur_median)

    return input_data


def get_data_localidades_latlong(schema_name, subset_type,
                                 filter_condition=None, grouping_cols=None):
    """
    Retrieve ENIGH data on household level, sorted by family id

    This will include several rows for each household x expansion factor, one
    row for each type of income source.

    :param string filter_condition A filtering condition from which to extract
     the required data.

    :return pd.Frame X A DataFrame containing the latitude information from the
     ENIGH data. This comes from the centroid of the locality specified in the
     vivi table from ENIGH, see http://internet.contenidos.inegi.org.mx/contenidos/productos//prod_serv/contenidos/espanol/bvinegi/productos/nueva_estruc/702825070373.pdf
     (or, inc ase of linkrot, search for Encuesta Nacional de Ingresos
     y Gastos de los Hogares ENIGH 2014)
    """
    X = pg_sed.get_original_features(
        ["folioviv", "expansion_ix", "localidades_longitude", "localidades_latitude"],
        schema_name, subset_type, filter_condition, grouping_cols
    )

    X = X.sort_values(by=["folioviv", "expansion_ix"])
    return X[["localidades_latitude", "localidades_longitude"]]


def get_data_manzana_coordinate(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    return pg_sed.get_original_features(
        ["manzana_latitude", "manzana_longitude"],
        schema_name, subset_type, filter_condition, grouping_cols
    )


def get_metropolitan_loc(input_data):
    """Derived feature function: whether locality has more than 100,000 population
    Between 2,500 and 15,000 is semi urban and is the base "urban" model since the values for
    urban_loc and metropolitan_loc would be 0 so those coeffs would not be included
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['metropolitan'] = input_data.apply(metropolitan_loc, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    metropolitan_location = input_data.groupby(group_ix)['metropolitan'].max()

    # convert to Series without indices
    metropolitan_location = metropolitan_location.reset_index()
    return metropolitan_location

def metropolitan_loc(row):
    """
    aux to get number of people in a household that work for no pay
    """
    try:
        loc_type = int(row['vivi_tam_loc'])
        if loc_type == 1:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_metropolitan_loc(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for metropolitan_loc feature, needs
    columns: 'vivi_tam_loc'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """


    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Metropolitan Location featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_tam_loc
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,        filter_condition
    )

    metropolitan_loc_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return metropolitan_loc_data

def get_urban_loc(input_data):
    """Derived feature function: whether locality has 15,000 < population < 99,999
    Between 2,500 and 15,000 is semi urban and is the base "urban" model since the values for
    urban_loc and metropolitan_loc would be 0 so those coeffs would not be included
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['urban'] = input_data.apply(urban_loc, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    urban_location = input_data.groupby(group_ix)['urban'].max()

    # convert to Series without indices
    urban_location = urban_location.reset_index()
    return urban_location

def urban_loc(row):
    """
    aux to get number of people in a household that work for no pay
    """
    try:
        loc_type = int(row['vivi_tam_loc'])
        if loc_type == 2:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_urban_loc(schema_name, subset_type,
                                filter_condition=None,
                                grouping_cols = None,):
    """ Generates data for urban_loc feature, needs
    columns: 'vivi_tam_loc'
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame
    """


    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("urban Location featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    # call generic psycopg connection
    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    vivi_tam_loc
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,        filter_condition
    )

    urban_loc_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return urban_loc_data

def get_remittances(input_data):
    """Derived feature function: whether a house receives remittances
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""

    input_data['has_remittances'] = input_data.apply(has_remittances, axis=1)

    group_ix = ["folioviv", "expansion_ix"]
    # actually not necessary to be the "max" since they are all the same value but need a grouping function
    with_remittances = input_data.groupby(group_ix)["has_remittances"].max()
    # convert to Series without indices
    with_remittances = with_remittances.reset_index()
    return with_remittances

def has_remittances(row):
    """
    aux compute whether the amount of remittances received by a house is greater than zero
    """
    try:
        remittances = int(row['concentradohogar_remesas'])
        if remittances > 0:
            return 1
        else:
            return 0
    except ValueError or TypeError:
        return None

def get_data_remittances(schema_name, subset_type,
                                filter_condition=None, grouping_cols=None):
    """ Generates data for remittances: see aux function
    needs columns: concentradohogar_remesas
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :return pd.DataFrame A minimal DataFrame that can be used as input to the
     feature generator for this particular derived feature.
    :rtype pd.DataFrame

    """

    if subset_type != "expanded_enigh_subset":
        raise NotImplementedError("Remittances featurization only implemented for ENIGH")

    if filter_condition is None:
        filter_condition = ""

    connection = pg_sed.db_connection()
    query = """
    SELECT DISTINCT ON (folioviv, numren, expansion_ix)
    folioviv,
    numren,
    expansion_ix,
    concentradohogar_remesas
    FROM %s.%s %s;
    """ % (
        schema_name,
        subset_type,
        filter_condition
    )

    remittances_data = pd.io.sql.read_sql(query, connection)
    connection.close()
    return remittances_data

def get_data_log_gasto(schema_name, subset_type, filter_condition=None,
                       grouping_cols=None):
    return np.log10(
        get_data_gasto(schema_name, subset_type, filter_condition, grouping_cols)
    )


def get_data_gasto(schema_name, subset_type, filter_condition=None,
                   grouping_cols=None):
    return pg_sed.get_original_features("gasto", schema_name, subset_type,
                                        filter_condition, ["folioviv", "expansion_ix"],
                                        "SUM")

def get_sqrt_prop_localidad_poor(input_data):
    return np.sqrt(input_data[["sqrt_prop_localidad_poor"]])


def get_data_sqrt_prop_localidad_poor(schema_name, subset_type,
                                      filter_condition=None,
                                      grouping_cols=None):
    """
    Get proportion of the localidad living in poverty
    """
    if filter_condition is None:
        filter_condition = ""

    X = pg_sed.get_original_features(
        ["folioviv", "expansion_ix", "localidades_n_households_in_poverty",
         "localidades_n_census_households"],
        schema_name, subset_type, filter_condition, grouping_cols
    )

    X["sqrt_prop_localidad_poor"] = X["localidades_n_households_in_poverty"] /  X["localidades_n_census_households"]
    X = X.sort_values(by=["folioviv", "expansion_ix"])
    return X

def get_programs_subset(programs_subset):
    """
    Utility to get a list of PUB programs

    Sometimes we want to look at all the pub programs, other times we only want
    to look at the very common ones.

    :praram string programs_subset Either "all" or "common", depending on
     whether we want to look at all the programs or just a subset of them.
    :return list of strings programs The associated list of programs.
    :rtype list of strings
    """
    if programs_subset == "all":
        programs = [" ", "0131", "S017", "S048", "E003", "0092", "S176", "0048", "0372", "0109", "0130", "U005", "0220", "S203", "0374", "S241", "S054", "S117", "0066", "0196", "0059", "0286", "0172", "S118", "S021", "0263", "0219", "0171", "0373", "0103", "S058", "0017", "S216", "E016", "0376", "0375", "S071", "0133", "0101", "0102", "S065", "0018", "S174", "S072", "S016", "0202", "S038", "0170", "0200", "0221", "0085", "S057", "S052", "0225", "0079", "0019"]
    elif programs_subset == "common":
        programs = [" ", "s072", "u005", "s038", "s052", "0131", "s071", "s176", "0048", "0170", "0219"]
    else:
        raise ValueError("programs_subset must be either 'all' or 'common'")
    return programs


def get_data_beneficiary_programs(schema_name, subset_type,
                                  filter_condition=None, grouping_cols=None,
                                  programs_subset="common"):
    """
    :return programs_table A dataframe where each row is a person and each
    column is a program. The cells in the dataFrame are boolean, and a cell is
    set to True if the person is enrolled in the program, False otherwise.
    :rtype pd.DataFrame
    """
    programs = get_programs_subset(programs_subset)
    program_cases = ["""COUNT(case program_id WHEN '%s' THEN 1 ELSE NULL END) > 0
                        AS program_%s""" %(program, program) for program in programs]
    programs_table_query = """SELECT
                            %s
                            FROM
                            %s.%s
                            %s
                            GROUP BY person_id
                            ORDER BY person_id;""" \
                            %(",".join(program_cases),
                              schema_name, subset_type,
                              filter_condition)
    logger.info("Executing: %s", programs_table_query)

    conn = pg_sed.db_connection()
    programs_table = pd.io.sql.read_sql(programs_table_query, conn)
    conn.close()
    return programs_table

###############################################################################
# Spatial features
###############################################################################


def identify_train_test_split(filter_condition_str):
    """
    Identifies whether the split is train or test based on the filter condition
    string
    """
    if "<>" in filter_condition_str:
        return "train"
    elif "=" in filter_condition_str:
        return "test"
    else:
        raise ValueError("filter_condition_str must contain either <> or =")


def get_data_prop_households_in_locality_with_lack(indicator):
    """
    Functional to define a function to get the proportion of households in each
    locality that have a certain indicator as True. For test splits, the
    proportions are computed on the train split to avoid leaking information
    when the indicator is the same as the response variable.

    :param string indicator The indicator column on which to compute the
     proportions
    :returns function A function that single column dataframe where each row
     corresponds to a person, and the column `prop` is the proportion of
     households in the person's locality who have the lack corresponding to
     the indicator
    """
    def f(schema_name, subset_type, filter_condition=None, grouping_cols=None):
        phase = identify_train_test_split(filter_condition)
        get_all_localities_props_subquery = """WITH household_data AS
                    (SELECT
                    DISTINCT ON(home_id)
                    *
                    FROM
                    %s.%s
                    %s
                    AND %s is NOT NULL),

                    all_localities_props AS
                    (
                    SELECT
                    locality_id,
                    (COUNT(case when %s::boolean = true then 1 else NULL end) * 1.0)/COUNT(*) as prop_%s
                    FROM household_data
                    GROUP BY locality_id
                    )
                    """ %(
                        schema_name, subset_type,
                        filter_condition if phase == "train" else filter_condition.replace("=", "<>"),
                        indicator, indicator, indicator)

        join_with_split_data_subquery = """,

                    localities_in_split AS
                    (
                    SELECT
                    DISTINCT ON(person_id)
                    locality_id
                    FROM
                    %s.%s
                    %s
                    ORDER BY person_id
                    )

                    SELECT prop_%s
                    FROM
                    localities_in_split
                    LEFT JOIN
                    all_localities_props
                    USING(locality_id)""" %(
                        schema_name, subset_type, filter_condition,
                        indicator
                    )

        full_query = "%s %s;" % (
            get_all_localities_props_subquery,
            join_with_split_data_subquery
        )
        conn = pg_sed.db_connection()
        logger.info("Executing: %s", full_query)
        prop_households_with_lack_of_indicator = pd.io.sql.read_sql(full_query, conn)
        conn.close()
        return prop_households_with_lack_of_indicator

    return f


def get_data_important_locality_id(schema_name, subset_type,
                                   filter_condition=None, grouping_cols=None):
    if grouping_cols != "home_id":
        warnings.warn("ignoring grouping_cols argument to get_data_important_locality_id()")

    return pg_sed.get_original_features(["important_locality_id"],
                                        schema_name, subset_type,
                                        filter_condition,
                                        grouping_cols="home_id")


def prototype_kernel(X, c, sigma=0.01):
    """
    Get RBF similarities between samples and a collection of centroids

    :param np.ndarray X A two-dimensional (n x p) numpy array whose rows
     represent samples and columns represent features. We want the similarity
     between each of these samples and the centroids
    :param np.ndarray c A two-dimensional (k x p) numpy array whose rows
     represent centroids and columns represent features.
    :param float sigma The bandwidth of the RBF kernel when computing distances
    :return np.ndarray K A two-dimensional (n x k) numpy array of kernel
     similarities between samples and the centroids. Specifically, the ik^th
     entry of this array is e^{-1/(2\sigma ^ 2) ||x_{i} - c_{k}||_{2}^{2}}]},
     which is just the RBF similarity between the i^th sample and the k^th
     centroid.
    :rtype np.ndarray
    """
    n = X.shape[0]
    K = c.shape[0]
    kernel_sim = np.zeros((n, K))

    logger.info("Computing kernel features...")
    for i in range(n):
        for k in range(K):
            kernel_sim[i, k] = np.exp(- 1 / (2 * sigma ** 2) * (
                np.linalg.norm((X[i, :] - c[k, :]) ** 2)
            ))

        if i % 25 == 0:
            logger.info("Completed sample %s" % i)

    return kernel_sim


def get_spatial_centroids(schema_name, subset_type, filter_condition=None,
                          grouping_cols=None, K=300, spatial_type="manzana"):
    """
    Return spatial cluster centroids

    These are lat / longs close to where many people live. It can be used to
    generate features according to the prototype_kernel, for example. This is
    just returning the cluster centroids after running k-medoids.

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param int K The number of centroids to return.
    :param string spatial_type The prefix to latitude / longitude to use when
     extracting spatial features from the database. Specifically, we will look
     for columns like spatial_type + "_latitude" in the database
     schema_name.subset_type.
    :return tuple of np.ndarrays Two numpy arrays representing (1) centroids
     A K x 2 numpy array of coordinates for the centroids of lat / longs in
     the specified data and (2) the latitudes / longitudes used to generate
     these coordinates.
    :rtype tuple of np.ndarrays
    """
    query_features = [spatial_type + s for s in ["_latitude", "_longitude"]]
    lat_long = pg_sed.get_original_features(query_features,
                                            schema_name, subset_type,
                                            filter_condition,
                                            grouping_cols)

    lat_long = preprocess_impute_numeric(lat_long, {"strategy": "median"})
    kmeans_result = KMeans(n_clusters=K).fit(lat_long)
    return (kmeans_result.cluster_centers_, lat_long)


def get_data_manzana_kernel(schema_name, subset_type, filter_condition=None,
                            grouping_cols=None, K=300):
    """
    Compute kernel similarities to manzana centroids (data retrival)

    This is the data retrieval component for generating a spatial feature using
    the manzanas. The idea is to encode the similarity between each manzana and
    a collection of "prototypes" / "centroids". Specifically, we return
    exp(-1/2 sigma^2 ||distance to centroid||^2) where the centroids are chosen
    by k-medoids.

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param int K The number of centroids to return.
    :param float sigma The bandwidth of the RBF kernel when computing distances
    :return tuple of np.ndarrays Two numpy arrays representing (1) centroids
     A K x 2 numpy array of coordinates for the centroids of lat / longs in
     the specified data and (2) the latitudes / longitudes used to generate
     these coordinates.
    :rtype tuple
    """
    return get_spatial_centroids(
        schema_name,
        subset_type,
        filter_condition,
        grouping_cols,
        K,
        "manzana"
    )


def get_manzana_kernel(input_data, sigma=0.01):
    """
    Compute kernel similarities to manzana centroids (feature derivation)

    This is feature derivation retrieval component for generating a spatial
    feature using the manzanas. The idea is to encode the similarity between
    each manzana and a collection of "prototypes" / "centroids". Specifically,
    we return exp(-1/2 sigma^2 ||distance to centroid||^2) where the centroids
    are chosen by k-means.

    :param tuple of np.ndarrays Two numpy arrays representing (1) centroids
     A K x 2 numpy array of coordinates for the centroids of lat / longs in
     the specified data and (2) the latitudes / longitudes used to generate
     these coordinates.
    :return pd.DataFrame An n x K DataFrame whose ik^th element is the kernel
     similarity between the i^th manzana and the K^th prototype.
    :rtype pd.DataFrame
    """
    centroids, lat_long = input_data
    K = centroids.shape[0]

    kernel_df = pd.DataFrame(
        prototype_kernel(np.array(lat_long), centroids, sigma),
        columns=["centroid_" + str(k) for k in range(K)]
    )
    return kernel_df

###############################################################################
# Beneficiary programs features
###############################################################################


def get_programs_subset(programs_subset):
    """
    Utility to get a list of PUB programs

    Sometimes we want to look at all the pub programs, other times we only want
    to look at the very common ones.

    :praram string programs_subset Either "all" or "common", depending on
     whether we want to look at all the programs or just a subset of them.
    :return list of strings programs The associated list of programs.
    :rtype list of strings
    """
    if programs_subset == "all":
        programs = [" ", "0131", "S017", "S048", "E003", "0092", "S176", "0048", "0372", "0109", "0130", "U005", "0220", "S203", "0374", "S241", "S054", "S117", "0066", "0196", "0059", "0286", "0172", "S118", "S021", "0263", "0219", "0171", "0373", "0103", "S058", "0017", "S216", "E016", "0376", "0375", "S071", "0133", "0101", "0102", "S065", "0018", "S174", "S072", "S016", "0202", "S038", "0170", "0200", "0221", "0085", "S057", "S052", "0225", "0079", "0019"]
    elif programs_subset == "common":
        programs = [" ", "s072", "u005", "s038", "s052", "0131", "s071", "s176", "0048", "0170", "0219"]
    else:
        raise ValueError("programs_subset must be either 'all' or 'common'")
    return programs


def get_data_beneficiary_programs(schema_name, subset_type,
                                  filter_condition=None, grouping_cols=None,
                                  programs_subset="all"):
    """
    :return programs_table A dataframe where each row is a person and each
    column is a program. The cells in the dataFrame are boolean, and a cell is
    set to True if the person is enrolled in the program, False otherwise.
    :rtype pd.DataFrame
    """
    programs = get_programs_subset(programs_subset)
    program_cases = ["""COUNT(case program_id WHEN '%s' THEN 1 ELSE NULL END) > 0
                        AS program_%s""" %(program, program) for program in programs]
    programs_table_query = """SELECT
                            %s
                            FROM
                            %s.%s
                            %s
                            GROUP BY person_id
                            ORDER BY person_id;""" \
                            %(",".join(program_cases),
                              schema_name, subset_type,
                              filter_condition)
    logger.info("Executing: %s", programs_table_query)

    conn = pg_sed.db_connection()
    programs_table = pd.io.sql.read_sql(programs_table_query, conn)
    conn.close()
    return programs_table


def get_data_benefit_description_average(schema_name, subset_type,
                                         filter_condition=None,
                                         grouping_cols=None):
    """
    Get distribution of types of beneficiary descriptions, per person

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param string programs_subset The subset of programs over which to calculate
     averages. Can be set to either "all" (in which case all programs are
     considered) or "common" (in which case only a smaller set of common
     programs are considered).
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns are
     programs on which we compute average payments on.
    :rtype pd.DataFrame

    example
    -------
    schema_name = "semantic"
    subset_type = "sifode_sample_100000"
    filter_condition = "WHERE cv_index <> 0"
    get_data_beneficiary_description_indicator(schema_name, subset_type,
                                               filter_condition,
                                               "person_id")
    """
    benefit_counts = get_data_benefit_description_counts(schema_name,
                                                         subset_type,
                                                         filter_condition,
                                                         grouping_cols)
    person_sums = benefit_counts.sum(axis=1)
    benefit_avg = benefit_counts.div(person_sums, axis=0)

    benefit_avg.columns = [s.replace("description_count_", "description_avg_")
                            for s in list(benefit_avg.columns.values)]
    return benefit_avg.fillna(0)


def get_data_benefit_description_indicator(schema_name, subset_type,
                                           filter_condition=None,
                                           grouping_cols=None):
    """
    Get whether different beneficiary descriptions appear

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param string programs_subset The subset of programs over which to calculate
     averages. Can be set to either "all" (in which case all programs are
     considered) or "common" (in which case only a smaller set of common
     programs are considered).
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns are
     programs on which we compute average payments on.
    :rtype pd.DataFrame

    example
    -------
    schema_name = "semantic"
    subset_type = "sifode_sample_100000"
    filter_condition = "WHERE cv_index <> 0"
    get_data_beneficiary_description_indicator(schema_name, subset_type,
                                               filter_condition,
                                               "person_id")
    """
    benefit_counts = get_data_benefit_description_counts(schema_name,
                                                         subset_type,
                                                         filter_condition,
                                                         grouping_cols)

    benefit_counts.columns = [s.replace("description_count_", "description_indicator_")
                              for s in list(benefit_counts.columns.values)]

    return benefit_counts > 0


def get_data_benefit_description_counts(schema_name, subset_type,
                                        filter_condition=None,
                                        grouping_cols=None):
    """
    Get the number of times different beneficiary descriptions appear

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param string programs_subset The subset of programs over which to calculate
     averages. Can be set to either "all" (in which case all programs are
     considered) or "common" (in which case only a smaller set of common
     programs are considered).
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns are
     programs on which we compute average payments on.
    :rtype pd.DataFrame

    example
    -------
    schema_name = "semantic"
    subset_type = "sifode_sample_100000"
    filter_condition = "WHERE cv_index <> 0"
    get_data_beneficiary_description_counts(schema_name, subset_type,
                                            filter_condition,
                                            "person_id")
    """
    # parse arguments
    grouping_str = pg_sed.parse_grouping_cols(grouping_cols)

    # construct portion of query for each benefit
    benefit_descriptions = [" ", "8", "48", "2", "254", "233", "69", "4","55", "99", "38", "52", "240", "101", "288", "34", "28", "68", "43", "47", "287", "65", "261", "75", "73", "30", "76", "62", "285", "235", "21", "115", "72", "64", "97", "60", "33", "16", "269", "105", "236", "104", "59", "37", "264", "263", "98", "87", "280", "270", "81", "167", "49", "278", "11", "45", "199", "36", "61", "27", "13", "286", "44", "56", "39", "276", "271", "88", "7", "54", "106", "15", "84", "42", "274", "80", "58", "17", "50", "111", "238", "272", "242", "95", "67", "23", "63", "151", "239", "277", "26", "281", "279", "35", "41"]
    benefit_cases = ["""COUNT(case benefit_description WHEN '%s' THEN 1 ELSE NULL END)
    AS benefit_description_count_%s""" % (benefit, benefit)
                     for benefit in benefit_descriptions]

    # construct full query
    benefit_counts_query = """SELECT
    %s
    FROM %s.%s
    %s
    GROUP BY %s
    ORDER BY %s""" % (
        ",".join(benefit_cases),
        schema_name,
        subset_type,
        filter_condition,
        grouping_str,
        grouping_str
    )
    logger.info("Executing: %s", benefit_counts_query)

    conn = pg_sed.db_connection()
    benefit_counts_table = pd.io.sql.read_sql(benefit_counts_query, conn)
    conn.close()

    return benefit_counts_table


def get_data_beneficiary_program_payment(aggregation="SUM"):
    """
    Funtional to make functions that give individual's payments

    Instead of computing the total payments to a person through a program, we
    can get the average amount in each payment cycle. This makes people who have
    been receiving many payments comparable to those who only receive a few.

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :param string programs_subset The subset of programs over which to calculate
     averages. Can be set to either "all" (in which case all programs are
     considered) or "common" (in which case only a smaller set of common
     programs are considered).
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns are
     programs on which we compute average payments on.
    :rtype pd.DataFrame

    example
    -------
    schema_name = "semantic"
    subset_type = "sifode_sample_100000"
    filter_condition = "WHERE cv_index <> 0"
    get_data_beneficiary_program_payment_average(schema_name, subset_type,
                                                 filter_condition,
                                                 "person_id"])
    """
    def f(schema_name, subset_type, filter_condition, grouping_cols=None,
          programs_subset="common"):

        grouping_str = pg_sed.parse_grouping_cols(grouping_cols)
        programs = get_programs_subset(programs_subset)
        program_cases = ["""CAST(%s(CASE program_id WHEN '%s' THEN payment_amount ELSE NULL END) AS NUMERIC)
        AS payment_%s_%s""" % (aggregation, program, program) for program in programs]

        programs_query = """SELECT
        %s
        FROM %s.%s
        %s
        GROUP BY %s
        ORDER BY %s""" % (
            ",".join(program_cases),
            schema_name,
            subset_type,
            filter_condition,
            grouping_str,
            grouping_str,
            aggregation
        )
        logger.info("Executing: %s", programs_query)

        conn = pg_sed.db_connection()
        programs_table = pd.io.sql.read_sql(programs_query, conn)
        conn.close()
        return programs_table
    return f

###############################
# Time Based Feature Generation
###############################

def get_cuis_survey_start_end_features(input_data):
    """Derived feature function: gets time features based on the cuis start and end date
    :param pd.DataFrame input_data A DataFrame giving the input data necessary
     for generating the feature of interest.
    :return pd.Series z A Series giving a features derived from the z DataFrame.
    :rtype pd.Series"""
    return input_data

def get_data_cuis_survey_start_end_features(schema_name, subset_type, filter_condition=None, grouping_cols=None):
    """
    Get all time features related to survey_start and survey_end times

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns
     include "survey_length", and all the time features generated by the
     general_date_features_factory method (year,month,hour,etc)
    :rtype pd.DataFrame

    """
    if filter_condition is None:
        filter_condition = ""

    X = pg_sed.get_original_features(
        ["survey_start_time", "survey_end_time"],
        schema_name, subset_type, filter_condition, grouping_cols
    )

    start = pd.to_datetime(X["survey_start_time"],infer_datetime_format=True)
    end = pd.to_datetime(X["survey_end_time"],infer_datetime_format=True)
    result_data = pd.DataFrame(end - start,columns=['survey_length'])
    survey_length = pd.DataFrame(list([ r.seconds / 60 if hasattr(r,"seconds") else None for r in result_data.survey_length]),columns=['survey_length'])

    standard_time_features = general_date_features_factory('survey_start_time',schema_name, subset_type, filter_condition, grouping_cols)
    data = pd.concat([survey_length,standard_time_features],axis=1)
    ret = pd.DataFrame(data)
    return ret

def get_date_of_survey_date_features(input_data):
    return input_data

def get_data_date_of_survey_date_features(schema_name, subset_type, filter_condition=None, grouping_cols=None):
    """
    Get all time features related to date_of_survey field in PUB

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns
     include all the time features generated by the
     general_date_features_factory method (year,month,hour,etc) given "date_of_survey"
    :rtype pd.DataFrame

    """
    #this is the date column for when the survey was taken as seen in PUB
    return general_date_features_factory('date_of_survey',schema_name, subset_type, filter_condition, grouping_cols)


def get_date_of_birth_date_features(input_data):
    return input_data

def get_data_date_of_birth_date_features(schema_name, subset_type, filter_condition=None, grouping_cols=None):
    """
    Get all time features related to date_of_birth

    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns
     include all the time features generated by the
     general_date_features_factory method (year,month,hour,etc) given "date_of_birth"
    :rtype pd.DataFrame

    """
    return general_date_features_factory('date_of_birth',schema_name, subset_type, filter_condition, grouping_cols)


def general_date_features_factory(colname,schema_name, subset_type, filter_condition=None, grouping_cols=None):
    """
    Get all time features we can derived from a given column name.

    :param string colname. The colname to derive time features from. It is
     expected that its values in the DB are datetimes.
    :param string schema_name The schema in which to look up the program
     beneficiary data.
    :param string subset_type The table in the semantic schema to get the number
     of rows from.
    :param string filter_condition A condition specifying the subset of rows to
     filter down to when calculating table size. This is useful when combined
     with the cv_index column for cross-validation.
    :param list of strings grouping_cols A list of strings specifiying over what
     unit to calculate the average payments over.
    :return pd.DataFrame A dataframe whose rows are at the level specified by
     grouping-cols (they are also sorted by grouping_cols) and whose columns
     include all the time features generated by the
     general_date_features_factory method (year,month,hour,etc) given colname.
    :rtype pd.DataFrame

    """
    if filter_condition is None:
        filter_condition = ""

    if grouping_cols is None:
        grouping_cols = "home_id"

    logger.debug("GET DATE FEATURES: %s ,   %s,    %s,     %s %s" % ( colname, schema_name, subset_type, filter_condition, grouping_cols ))
    X = pg_sed.get_original_features( [colname], schema_name, subset_type, filter_condition, grouping_cols)

    date_start = pd.to_datetime(X[colname],infer_datetime_format=True)
    #feats = ["year","day","month","dayofweek","minute","hour","dayofyear","weekofyear","is_month_start","is_month_end","is_year_start","is_year_end"]
    feats = ["year","day","month","dayofweek","minute","hour","dayofyear","weekofyear","is_month_start","is_month_end","is_year_start","is_year_end"]
    feats_d = date_start.apply(lambda x: [ getattr(x,f) if hasattr(x,f) else None for f in feats ])

    feats_data = pd.DataFrame(list(feats_d),columns=[colname + "_" + c for c in feats])

    #TODO: handle the 127 year old in data cleaning, but for now just give null value
    #FOR now don't include timestamp since 1370633910 occurs 182 times and thats throwing things off.. we could filter this if need be
    #start_seconds = date_start.apply(lambda x: x.timestamp() if hasattr(x,'timestamp') and str(type(x)) != "<class 'pandas.tslib.NaTType'>" and str(x) != '1888-07-08 18:00:00' else None)
    #epoch_time_start = pd.DataFrame( list(start_seconds),columns=[colname + '_start_timestamp'])

    current_age = date_start.apply(cur_age)
    current_age_df = pd.DataFrame( list(current_age), columns=[colname + '_current_age'])

    data = pd.concat([epoch_time_start,current_age,feats_data],axis=1)
    ret = pd.DataFrame(data)
    return ret

def cur_age( x ):
    try:
        if hasattr(x,'today') and str(type(x)) != "<class 'pandas.tslib.NaTType'>":
            tod = dt.datetime.utcnow().timestamp()
            cur = x.timestamp()
            diff = tod - cur
            indays = diff / (24*3600*365)
            y = math.floor(indays)
        else: 
            y = None       
    except:
        logger.debug("Warning %s gave incorrect date conversion" % x)
        y = None
    return y



###############################################################################
# define globals using some of these functionals
###############################################################################

get_data_beneficiary_program_payment_total = get_data_beneficiary_program_payment("SUM")
get_data_beneficiary_program_payment_average = get_data_beneficiary_program_payment("AVG")
get_data_prop_households_with_food_deprivation = get_data_prop_households_in_locality_with_lack("indicator_food_deprivation")

deprivations = ["overcrowding",
                "access_to_water",
                "fuel_for_cooking_deprivation",
                "basic_housing_services",
                "quality_of_dwellings_in_housing_deprivation"]

for deprivation in deprivations:
    def_string = """get_data_prop_households_with_{0} = \
    get_data_prop_households_in_locality_with_lack("indicator_for_{0}")""".format(deprivation)
    exec(def_string)


# we need to define these functions so that the globals() call in get_features())
# has a function to match against
def identity_fun(x):
    return x

identity_feature_funs = ["localidades_latlong",
                         "manzana_coordinate",
                         "gasto", "log_gasto",
                         "beneficiary_programs",
                         "prop_households_with_food_deprivation",
                         "prop_households_with_overcrowding",
                         "prop_households_with_access_to_water",
                         "prop_households_with_fuel_for_cooking_deprivation",
                         "prop_households_with_basic_housing_services",
                         "prop_households_with_quality_of_dwellings_in_housing_deprivation",
                         "important_locality_id",
                         "beneficiary_program_payment_total",
                         "beneficiary_program_payment_average",
                         "benefit_description_average",
                         "benefit_description_indicator",
                         "benefit_description_counts"]

for cur_var in identity_feature_funs:
    def_string = """get_{0} = identity_fun""".format(cur_var)
    exec(def_string)

###############################################################################
# Preprocessing utilities
###############################################################################


def preprocess_features(X, preprocessing_json):
    """
    Preprocessing wrapper function

    This applies the series of preprocessing options specified in the
    process_json object. Each entry in the json specifies (1) the name of the
    preprocessing option (e.g., impute_numeric) and (2) the parameters for that
    preprocessing task (e.g., "strategy": "median" vs. "strategy": "mean"). The
    naming is intended to be as similar to sklearn's preprocessing module as
    possible (we just pass (2) in as keyword arguments to the sklearn
    preprocessing object).

    :param pd.DataFrame X The pandas dataframe to preprocess.
    :param string processing_json The path to the json file specifying which
     preprocessing tasks to perform and how to perform them. For example, we
     could use

      {
          "impute_categorical": {
        	"strategy": "missing_category"
          },
          "impute_numeric": {
        	"strategy": "median",
        	"axis": "0"
          },
          "standard_scaler": "pass"
      }

    The function will look for preprocessing functions with the key names
    prefixed with preprocess_*, e.g., preprocess_impute_numerics. If the
    associated value is "pass", that preprocessing option will not be applied.
    Otherwise, the associated dictionary below will be passed in as an argument
    to the function. The preprocessing options will be applied in the order they
    are specified in the JSON.

    :return A preprocessed version of the X DataFrame
    :rtype pd.DataFrame()
    """
    with open(preprocessing_json) as preprocess_file:
        processing_opts = json.load(preprocess_file,
                                    object_pairs_hook=OrderedDict)

    #logger.debug("IN PREPROCESS FEATURES: X.shape %s" % X.shape)
    for opt, params in processing_opts.items():
        if params != "pass":
            process_fun = globals()["preprocess_" + opt]
            X = process_fun(X, params)

    #logger.debug("IN PREPROCESS FEATURES AFTER SHAPE: X.shape %s" % X.shape)
    return X


def preprocess_impute_numeric(X, params):
    """
    Impute missing values in numeric columns

    This just a wrapper for the preprocessing.Imputer class in sklearn.
    See http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Imputer.html

    :param pd.DataFrame X The pandas dataframe to preprocess.
    :param dict params A dictionary of keyword arguments that will be passed
     directly to the Imputer class in the preprocessing module from sklearn.
    :return X A verison of X where the numeric columns are imputed according to
     the params provided to the Imputer module.
    :rtype pd.DataFrame
    """
    imp = ppr.Imputer(**params)
    numerics = [col in ["float64", "int64"] for col in X.dtypes]
    if sum(numerics) != 0:
        tmp = imp.fit_transform(X.loc[:, numerics])
        try: 
            X.loc[:, numerics] = tmp
        except:
            if sum(numerics) != tmp.shape[1]:
                raise ValueError("Number of Columns before and after imputation not equal")
    else:
        warnings.warn("No numeric columns to impute.")
    return X


def preprocess_impute_categorical(X, params):
    """
    Fill in missing values in categorical columns

    The sklearn Imputer() class doesn't deal with the case that columns are
    categorical, so we provide some imputation techniques on our own. The
    currently implemented strategies are,
       "most_frequent": Replace missing values with the category that is most
                        common.
       "missing_category": Create a new category representing missingness.

    :param pd.DataFrame X The pandas dataframe to preprocess.
    :param dict params A dictionary of options to use in the imputation. The
     only key that it will look at is "strategy", which has the options
     described in the section above.
    :return X A version of the pandas data frame where categorical columns have
     been imputed in the way specified by params.
    :rtype pd.DataFrame
    """
    categoricals = np.where(X.dtypes == "object")[0]

    for ix in categoricals:
        if params["strategy"] == "most_frequent":
            try:
                fill_value = X.iloc[:, ix].value_counts().index[0]
            except: 
                if len(X.iloc[:, ix].value_counts()) == 0:
                   warnings.warn("Trying to impute categorical with all NaNs")
                   fill_value = 0
         

        elif params["strategy"] == "missing_category":
            fill_value = "missing"
        else:
            raise ValueError(
                """'strategy' argument in imputer must be either  most_frequent
                or missing_category"""
            )

        X.iloc[:, ix] = X.iloc[:, ix].fillna(fill_value)

    return X


def preprocess_remove_constants( X, params):
    """
    Remove columns of just one value through out
    """
    for c in X.columns:
       first = X[c][0]
       if np.all(X[c].values == first):
           logger.debug("Remove constant column %s" % c)
           X = X.drop(c,1)
       elif np.all(pd.isnull(X[c]).values):  
           logger.debug("Remove nana column %s" % c)
           X = X.drop(c,1)
       

    return X

def preprocess_standard_scaler(X, params):
    """
    Center / scale numeric columns using the StandardScaler

    This just a wrapper for the preprocessing.Imputer class in sklearn.
    See http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html#sklearn.preprocessing.StandardScaler

    :param pd.DataFrame X The pandas dataframe to preprocess.
    :param dict params A dictionary of keyword arguments that will be passed
     directly to the StandardScaler class in the preprocessing module from
     sklearn. Basically, should you center, scale, or do both.
    :return X A verison of X where the numeric columns have been centered /
     scaled as specified by params.
    :rtype pd.DataFrame
    """
    scaler = ppr.StandardScaler(**params)
    numerics = [col in ["float64", "int64"] for col in X.dtypes]
    if sum(numerics) != 0:
        X.loc[:, numerics] = scaler.transform(X.loc[:, numerics])
    else:
        warnings.warn("No numeric columns to scale.")
    return X


def preprocess_get_dummies(X, params):
    """
    Get dummy variables for categorical features

    This just a wrapper for get_dummies in pandas. See
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.get_dummies.html

    :param pd.DataFrame X The pandas dataframe to preprocess.
    :param dict params A dictionary of keyword arguments that will be passed
     directly to pd.get_dummies.
    :return X_dummies A verison of X where the categorical columns have been
     converted to their dummy encoding.
    :rtype pd.DataFrame
    """
    params["data"] = X
    X_dummies = pd.get_dummies(**params)
    k_categories = X_dummies.shape[1]

    if k_categories > 1000:
        warnings.warn("""Creating one-hot-encoding for feature with %s levels,
        are you sure you want to proceed?""" % k_categories)
    return X_dummies

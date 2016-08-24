"""
Functions for extracting and generating responses from semantic data subset

The main function in this module is write_responses(), which extracts data from
a specified subset of the semantic data (e.g., imss) that further meets a filter
condition (e.g., not in the current cross-validation fold). This is the
the essential functionality of the GetResponses task in write_responses.py.

Examples:

schema_name = "semantic_underreport"
subset_name = "prospera_with_important_localities"
filter_condition = "WHERE cv_index <> 0"

n_rooms_data = get_data_diff_n_rooms(schema_name, subset_name, filter_condition)
diff_n_rooms = get_diff_n_rooms(n_rooms_data)
"""

import logging
import logging.config
import numpy as np
import pandas as pd
from luigi import configuration
import utils.pg_sedesol as pg_sed

LOGGING_CONF = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")

def get_responses(responses, schema_name, subset_type, filter_condition=None):
    """
    Internal function used by write_responses()

    Write responses gets responses and writes them to file; this function
    returns the actual responses insteasd.

    :param string responses A list of response types to extract. Each type
     of response must correspond to a function in this module.
    :param string subset_type The table in the semantic schema to use in
     generating features. This is specified by the subset_type field in the
     luigi.cfg file, usually.
    :param string filter_condition A condition specifying the subset of rows to
     filter down, within the specified table. This is useful when combined
     with the cv_index column for cross-validation.
    :return responses pd.DataFrame A data frame whose columns are different
     response variables
    :rtype pd.DataFrame
    """
    logger.info("Getting responses %s", responses)
    responses = [r.strip() for r in responses.split(",")]

    i = 0
    for response in responses:
        logger.info("Generating %s response", response)
        cur_fun = globals()["get_" + response]
        derived_responses_input = globals()["get_data_" + response]
        response_input = derived_responses_input(schema_name,
                                                 subset_type,
                                                 filter_condition)
        if i == 0:
            responses = cur_fun(response_input)
            i += 1
        else:
            responses = pd.concat([responses, cur_fun(response_input)], axis=1)
    return responses


def write_responses(responses, schema_name, subset_type, filter_condition=None,
                    responses_dir="./", responses_basename="responses"):
    """
    Wrapper function to get both original and derived features

    This wraps the get_original_features() function, along with any get_*
    function used to get specific derived features specified in the features
    json.

    :param string responses A list of response types to extract. Each type
     of response must correspond to a function in this module.
    :param string subset_type The table in the semantic schema to use in
     generating features. This is specified by the subset_type field in the
     luigi.cfg file, usually.
    :param string filter_condition A condition specifying the subset of rows to
     filter down, within the specified table. This is useful when combined
     with the cv_index column for cross-validation.
    :param responses_dir string The path to which to write the leave-fold-out
     response csv files along with the schema
    :param responses_basename string The basename of the files to which we write
     the leave-fold-out response csv files along with the schema
    :return None
    :side-effects Writes responses leaving out each fold responses_dir, along
     with the schema.
    """
    responses = get_responses(responses, schema_name, subset_type,
                              filter_condition)

    # write repsonses to file
    responses_str = responses_basename + pg_sed.hash_if_needed(subset_type)
    base_path = pg_sed.process_filter_name(responses_dir, filter_condition,
                                           responses_str)
    responses_path = base_path + ".csv"
    schema_path = base_path + "_schema.csv"

    logger.info("Writing responses to %s", responses_path)
    pg_sed.write_data_with_schema(responses, responses_path, schema_path)


###############################################################################
# Response generator functions
###############################################################################


def get_log_household_income(input_data):
    """
    Get log base 10 of each household's income

    This sums over each individual in the home's income source. We take the log
    because the incomes are quite highly right skewed

    :param pd.DataFrame input_data A pandas dataframe containing a column with
     monthly incomes. Each row must correspond to a household.
    """
    # we want it to be a DataFrame, not a series
    response = np.log10(input_data[["monthly_income"]])
    return response

def get_data_log_household_income(schema_name, subset_type,
                                  filter_condition=None):
    """
    Retrieve income data on household level, sorted by family id

    This will include several rows for each household x expansion factor, one
    row for each type of income source.

    :param string schema_name The name fo the schema from which to extract this
     data
    :param string subset_type The name of the table within the specified schema
     from which to extract the required data.
    :param string filter_condition A filtering condition from which to extract
     the required data.

    :return pd.Frame Y A DataFrame containing several columns related to
     household income. These come from the ingresos table in ENIGH, see
     page 17 in http://internet.contenidos.inegi.org.mx/contenidos/productos//prod_serv/contenidos/espanol/bvinegi/productos/nueva_estruc/702825070373.pdf
     (or, inc ase of linkrot, search for Encuesta Nacional de Ingresos
     y Gastos de los Hogares ENIGH 2014)
    """
    if filter_condition is None:
        filter_condition = ""

    query = """SELECT folioviv,
                expansion_ix,
                SUM(ing_tri) / 4.0 as monthly_income
                FROM %s.%s %s
                GROUP BY folioviv, expansion_ix
                ORDER BY folioviv, expansion_ix;""" \
             % (schema_name, subset_type, filter_condition if filter_condition else '')


    logger.info("Executing: %s", query)
    conn = pg_sed.db_connection()
    response = pd.io.sql.read_sql(
        query,
        conn
    )

    conn.close()
    return response


def get_data_function_factory(feature_name, grouping_cols="person_id"):
    """
    Function factory to get data for original data features

    Getting data for variables that are already in the database have similar
    structure, but we need the data-getter functions to exist for the globals()
    call in get_response() function. This at least reduces the amount of code
    that hasto be written.

    :param string feature_name The name of the feature where we want a
     data getter funciton.
    :return function f A function that gets the required feature after being
     specified the schema / table, cv subset, and columns to group according to.
    :rtype function
    """
    def f(schema_name, subset_type, filter_condition=None):
        if filter_condition == None:
            filter_condition = ""
        return pg_sed.get_original_features(feature_name,
                                            schema_name,
                                            subset_type,
                                            filter_condition=filter_condition,
                                            grouping_cols=grouping_cols)
    return f


###############################################################################
# Response functions for SIFODE and ENIGH
###############################################################################

# data getters
deprivations = ["education_deprivation_ic_rezedu",
                "education_deprivation_ic_rezedu_3a15",
                "education_deprivation_ic_rezedu_81",
                "education_deprivation_ic_rezedu_82",
                "health_care_deprivation",
                "social_security_deprivation",
                "employed_without_social_security_deprivation",
                "elders_without_social_security_deprivation",
                "other_rubrics_without_social_security",
                "quality_of_dwellings_in_housing_deprivation",
                "floor_materials_deprivation",
                "roof_materials_deprivation",
                "wall_materials_deprivation",
                "overcrowding",
                "basic_housing_services",
                "access_to_water",
                "drainage_services_deprivation",
                "electricity_services_deprivation",
                "fuel_for_cooking_deprivation"]

enigh_level_features = ["respondent_honest",
                        "imss_salary"]

def identity_fun(x):
    return x

for cur_var in deprivations:
    data_def_string = """get_data_{0} = get_data_function_factory("indicator_for_{0}")""".format(cur_var)
    response_def_string = """get_{0} = identity_fun""".format(cur_var)
    exec(data_def_string)
    exec(response_def_string)

for cur_var in enigh_level_features:
    data_def_string = """get_data_{0} = get_data_function_factory("indicator_for_{0}",
    grouping_cols=["folioviv", "expansion_ix"])""".format(cur_var)
    exec(data_def_string)

identity_vars = deprivations + \
                enigh_level_features + \
                ["all_discrepancies"]

for cur_var in identity_vars:
    response_def_string = """get_{0} = identity_fun""".format(cur_var)
    exec(response_def_string)

# this column is named differently than the others (indicator_ not in)
get_data_food_deprivation = get_data_function_factory("indicator_food_deprivation")
get_food_deprivation = identity_fun

###############################################################################
# Get responses variables for home verification data
###############################################################################


def get_col_diffs(input_data):
    return pd.DataFrame(
        data=input_data.iloc[:, 0] - input_data.iloc[:, 1],
        columns=["diff_{}".format(input_data.columns[0])]
    )


def get_col_match(input_data):
    return pd.DataFrame(
        data=input_data.iloc[:, 0] == input_data.iloc[:, 1],
        columns=["match_{}".format(input_data.columns[0])]
    )


def get_diff_function_factory(question, diff_type="verified"):
    """
    Function factory to get the difference between reported and measured data

    We count on the fact that the pairs of columns are called '{question}' and
    'verified_{question}'.
    """
    if diff_type == "verified":
        questions_list = [question, "verified_" + question]
    elif diff_type == "works":
        questions_list = [question,
                          "verified_" + question,
                          "verified_" + question.replace("own_", "")+ "_works"]
    else:
        raise ValueError("'diff_type' must be either 'verified' or 'works'")

    def f(schema_name, subset_type, filter_condition=None):
        if filter_condition == None:
            filter_condition = ""
        return pg_sed.get_original_features(questions_list,
                                            schema_name, subset_type,
                                            filter_condition,
                                            grouping_cols="home_id")
    return f


def convert_input(f, convert_types=["to_float"]):
    """
    Modify functions so that they clean their input

    Thinking in a functional way, it can be useful to modify a function so it
    prepares its input before executing. This implements that idea for a few
    cleaning steps required when working with the home verification data.

    :param function f The function to modify so that it cleans its input before
     executing.
    :param list of strings convert_types  A list of operations to apply to the
     input data before applying the function f. Currently implements the
     following operations,
         'replace_97s': convert '97' to '5'
         'group_less_5': convert numbers less than 5 to 0
         'get_own': Get whether someone owns / doesn't own an appliance (1 means
          doesn't own, 0 means own).
         'get_works': Get whether someone owns an appliance and it works (1
          means it doesn't work (or the person doesn't own it, 0 means a working
          one is available).
         'to_float': Convert strings to floats
         'reverse_sign': Reverse the sign of a float

    :return g A version of the function f that applies the conversion operations
     before executing.
    """
    def g(input_data):
        if "replace_97s" in convert_types:
            input_data = input_data.replace("97", "5")

        if "group_less_5" in convert_types:
            input_data = input_data.replace("5", 1)
            input_data = input_data.replace(["1", "2", "3", "4"], 0)

        if "get_own" in convert_types:
            input_data = input_data.replace("11", 0)
            input_data = input_data.replace("12", 0)
            input_data = input_data.replace("21", 1) # larger -> more lacks
            input_data = input_data.replace("22", 1)

            verif_col = [i for (i, s) in enumerate(input_data.columns.values)
                         if "verified_own" in s]
            input_data.iloc[:, verif_col] = 1 - input_data.iloc[:, verif_col] # want larger -> more lacks
            input_data = input_data.iloc[:, [0] + verif_col]

        if "get_works" in convert_types:
            input_data = input_data.replace("11", 0)
            input_data = input_data.replace("12", 1)
            input_data = input_data.replace("21", 1)
            input_data = input_data.replace("22", 1)

            works_col = [i for (i, s) in enumerate(input_data.columns.values)
                         if "works" in s]
            input_data.iloc[:, works_col] = 1 - input_data.iloc[:, works_col] # want larger -> more lacks
            input_data = input_data.iloc[:, [0] + works_col]

        if "to_float" in convert_types:
            input_data = input_data.astype("float64")

        if "reverse_sign" in convert_types:
            input_data = -(input_data)

        return f(input_data)

    return g


# different groups of functions need different arguments
numeric_vars = ["n_rooms", "n_bedrooms", "floor_material", "roof_material",
                "wall_material", "toilet", "sewage_type", "water_source",
                "light_source"]
own_diff_vars = ["stove", "refridgerator", "microwave", "washing_machine",
                 "water_tank", "tv", "telephone", "cell_phone",
                 "computer", "climate_control", "vehicle", "internet"]

# define all the data getters
for cur_var in numeric_vars:
    data_def_string = """get_data_diff_{0} = get_diff_function_factory("{0}")""".format(cur_var)
    exec(data_def_string)

for cur_var in own_diff_vars:
    own_data_string = """get_data_diff_own_{0} = get_diff_function_factory("own_{0}", "works")""".format(cur_var)
    works_data_string = """get_data_diff_{0}_works = get_data_diff_own_{0}""".format(cur_var)
    exec(own_data_string)
    exec(works_data_string)

# define all the response functions
for cur_var in own_diff_vars:
    own_def_string = """get_diff_own_{0} = convert_input(get_col_diffs, ["get_own"])""".format(cur_var)
    works_def_string = """get_diff_{0}_works = convert_input(get_col_diffs, ["get_works"])""".format(cur_var)
    exec(own_def_string)
    exec(works_def_string)

for cur_var in ["n_rooms", "n_bedrooms", "floor_material", "roof_material", "wall_material"]:
    def_string = """get_diff_{0} = convert_input(get_col_diffs)""".format(cur_var)
    exec(def_string)

get_diff_toilet = convert_input(get_col_diffs, ["replace_97s", "to_float", "reverse_sign"])
get_diff_sewage_type = convert_input(get_col_diffs, ["replace_97s", "to_float", "reverse_sign"])
get_diff_water_source = convert_input(get_col_diffs, ["to_float", "reverse_sign"])
get_diff_light_source = convert_input(get_col_diffs, ["replace_97s", "to_float", "group_less_5"])


###############################################################################
# Summary discrepancy measures for home verification data
###############################################################################

def get_data_all_discrepancies(schema_name, subset_type, filter_condition=None):
    """
    Get matrix of discrepancies for each question in CUIS

    This loops over each of the 'diff' questions defined above and puts the
    result in a single big matrix. It is slower than if there were only a single
    query to retrieve all columns, but the implementation is much simpler (and
    less prone to new bugs).
    """
    numeric_vars = ["n_rooms", "n_bedrooms", "floor_material", "roof_material",
                    "wall_material", "toilet", "sewage_type", "water_source",
                    "light_source"]
    own_diff_vars = ["stove", "refridgerator", "microwave", "washing_machine",
                     "water_tank", "tv", "telephone", "cell_phone",
                     "computer", "climate_control", "vehicle", "internet"]

    all_vars = numeric_vars + \
               ["own_{}".format(s) for s in own_diff_vars ] + \
               ["{}_works".format(s) for s in own_diff_vars ]

    i = 0
    for cur_var in all_vars:
        logger.info("Extracting response for {}".format(cur_var))
        cur_fun = globals()["get_diff_{}".format(cur_var)]
        input_fun = globals()["get_data_diff_{}".format(cur_var)]
        input_data = input_fun(schema_name, subset_type, filter_condition)

        if i == 0:
            diff_data = cur_fun(input_data)
            i += 1
        else:
            diff_data = pd.concat([diff_data, cur_fun(input_data)], axis=1)

    return diff_data


get_data_any_discrepancy = get_data_all_discrepancies
get_data_count_discrepancies = get_data_all_discrepancies
get_data_discrepancy_degree = get_data_all_discrepancies


def get_any_discrepancy(input_data):
    """
    Get whether there were any discrepancies in home verification

    This looks at whether any question (among responses that are not missing)
    had a response that was inconsistent between self-reported and verified
    ownership.

    Note that we ignore questions where the response is missing and that
    a discrepancy can be due to either over or underreporting.
    """
    input_data = input_data.fillna(0)
    discrepancy_mask = input_data == 0
    return discrepancy_mask.apply(
        lambda x: np.any(x == 0),
        axis=1
    )

def get_any_underreporting(input_data):
    """
    Get whether there were any under reporting in home verification

    This looks at whether any question (among responses that are not missing)
    had a response that was under reported between self-reported and verified
    ownership.

    Note that we ignore questions where the response is missing and that
    a discrepancy is due to underreporting.
    """
    input_data = input_data.fillna(0)
    discrepancy_mask = input_data > 0
    return discrepancy_mask.apply(
        lambda x: np.any(x == 1),
        axis=1
    )

def get_count_discrepancies(input_data):
    """
    Count the number of discrepancies in home verification

    Note that we ignore questions where the response is missing and that
    a discrepancy can be due to either over or underreporting.
    """
    input_data = input_data.fillna(0)
    discrepancy_mask = input_data == 0
    return discrepancy_mask.apply(sum, axis=1)


def get_discrepancy_degree(input_data):
    """
    Get the degree of discrepancies across questions

    Large positive numbers mean high under-reporting. Large negative numbers
    mean large over-reporting. Some questions which are numeric are directly
    added into this value -- it's not normalized. So, the relative contribution
    from these numeric variables (like number of rooms in the home) is somewhat
    larger than for questions with binary outcomes.

    Note that this ignores questions where the response is missing.
    """
    input_data = input_data.fillna(0)
    return input_data.apply(np.sum, axis=1)


###############################################################################
# Get responses variables for enigh income data
###############################################################################

def get_log_income_without_transferences(input_data):
    """
    Get income without transferences. Estimated income should not contain money from
    social or government programs
    :param pd.DataFrame input_data A pandas dataframe containing a column with
     monthly incomes. Each row must correspond to a household.
    """
    # we want it to be a DataFrame, not a series
    response = np.log(input_data[["income_without_transferences"]])
    return response


def get_data_log_income_without_transferences(schema_name, subset_type,
                                  filter_condition=None):
    """
    Retrieve income without transferences data on household level, sorted by family id
    This takes into account the original variable that is estimated: income without transferences from social programs
    This will include several rows for each household x expansion factor, one
    row for each type of income source.
    :param string schema_name The name fo the schema from which to extract this
     data
    :param string subset_type The name of the table within the specified schema
     from which to extract the required data.
    :param string filter_condition A filtering condition from which to extract
     the required data.
    :return pd.Frame Y A DataFrame containing several columns related to
     household income. These come from the concentradohogar table in ENIGH, see
     page 21 in http://internet.contenidos.inegi.org.mx/contenidos/productos//prod_serv/contenidos/espanol/bvinegi/productos/nueva_estruc/702825070373.pdf
     (or, inc ase of linkrot, search for Encuesta Nacional de Ingresos
     y Gastos de los Hogares ENIGH 2014)
    """
    if filter_condition is None:
        filter_condition = ""

    query = """SELECT distinct on (folioviv,expansion_ix)
                folioviv,
                expansion_ix,
                concentradohogar_ing_cor - concentradohogar_transfer - concentradohogar_bene_gob - concentradohogar_rentas as income_without_transferences
                FROM %s.%s %s
        ORDER BY folioviv, expansion_ix;""" \
             % (schema_name, subset_type, filter_condition if filter_condition else '')



    logger.info("Executing: %s", query)
    conn = pg_sed.db_connection()
    response = pd.io.sql.read_sql(
        query,
        conn
    )

    conn.close()
    return response


def get_has_discrepancy(input_data):
    """
    Get has_discrepancy response for each family
    :param pd.DataFrame input_data A one column DataFrame specifying whether
     the family had any discrepancy between their cuis and home verifaction.
    :return pd.DataFrame
    :rtype pd.DataFrame
    """
    return pd.DataFrame( input_data['has_discrepancy'].astype('int'))

def get_data_has_discrepancy(schema_name, subset_type, filter_condition=None):
    """
    Get data for has_discrepancy
    This function only exists so the globals() call in write_responses() will work
    """
    if filter_condition == None:
        filter_condition = ""

    X = get_data_all_discrepancies(schema_name, subset_type, filter_condition=filter_condition)
    return pd.DataFrame( get_any_discrepancy(X), columns=['has_discrepancy'])

def get_has_underreporting(input_data):
    """
    Get has_discrepancy response for each family
    :param pd.DataFrame input_data A one column DataFrame specifying whether
     the family had any under reporting discrepancy between cuis and home verifaction.
    :return pd.DataFrame
    :rtype pd.DataFrame
    """
    return pd.DataFrame( input_data['has_underreporting'].astype('int') )

def get_data_has_underreporting(schema_name, subset_type, filter_condition=None):
    """
    Get data for has_underreporting
    This function only exists so the globals() call in write_responses() will work
    """
    if filter_condition == None:
        filter_condition = ""
    X = get_data_all_discrepancies(schema_name, subset_type, filter_condition=filter_condition)
    return pd.DataFrame( get_any_underreporting(X), columns=['has_underreporting'])

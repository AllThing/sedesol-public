"""
Retrieve model fits from the database

Once we've fitted models in the pipeline, it is useful to be able to extract
their fitted parameters and predictions so they can be studied interactively.
These functions facilitate that retrieval.
"""

import pickle
import json
import logging
import logging.config
import warnings
from copy import deepcopy
import pandas as pd
import numpy as np
from luigi import configuration

from utils.run_models_funs import get_model_data, to_matrix
import utils.feature_funs as ft
from utils.response_funs import get_responses
from utils.pg_sedesol import db_connection


LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


def get_models(models_schema, models_table, ids=None):
    """
    Download  metadata and model objects from a models database table

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param list of strings ids A list of database model IDs to filter the
     results down to.
    :return dict A dictionary keyed by the 'model' id variable in the database.
     The model object that can be used in prediction is stored in the 'fits'
     field.
    :rtype dict

    Examples
    --------
    # retrieves models from the underreporting schema for model ids 14 and 18
    models = get_models("models", "underreporting", ['14', '18'])
    """
    if ids is None:
        models_filter = ""
    else:
        models_filter = "WHERE model IN (%s)" % (",".join(ids))

    connection = db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * from %s.%s %s" % (
        models_schema,
        models_table,
        models_filter
    ))
    field_names = [i[0] for i in cursor.description]
    metadata = pd.DataFrame(cursor.fetchall(), columns=field_names)
    metadata = metadata.set_index('model', drop=True)
    metadata = json.loads(metadata.to_json(orient="index"))

    # combine binaries and metadata into one list
    model_results = dict()
    for k in metadata.keys():
        model_results[k] = metadata[k].copy()

        try:
            with open(metadata[k]["binary_path"], "rb") as binary_obj:
                model_binary = pickle.load(binary_obj)
            model_results[k]["fits"] = model_binary
        except IOError:
            warnings.warn("Binary not found for model %s" % k)
            model_results[k]["fits"] = None

    return model_results


def get_model_preds(model_fits, model_data):
    """
    Extract predictions from a fitted model

    :param dictionary of sklearn objects model_fits A dictionary whose keys are
     different responses to do prediction on and whose values are fitted model
      objects for each of those responses.
    :param dict model_data The dictionary that results from a call to
     get_model_data() in run_models_funs.py.
    :return dictionary of pd.DataFrames A dictionary with two keys (train and
     test), giving the fitted response values (or predicted probabilities) for
     the train / test splits. Within each split, the returned data frame has
     each response as a separate column (e.g., healthcare deprivation,
     and food deprivation might be two columns).
    :rtype dict

    Examples
    ---------
    # get model results
    models = get_models("models", "underreporting", ['14', '18'])
    model_fits = models["14"]["fits"]

    # get predictions for those models
    basenames = json.loads(models["14"]["model_data"])
    model_data = get_model_data(basenames, model_data_schema, "intersection")
    preds = get_model_preds(model_fits, model_data)
    """
    y_hat = {"train": None, "test": None}
    for split in ["train", "test"]:
        y = model_data[split + "_responses"]
        X = model_data[split + "_features"]
        y_hat[split] = pd.DataFrame(np.empty((y.shape[0], 0)))

        for response, model_fit in model_fits.items():
            if hasattr(model_fit, "predict_proba"):
                cur_y_hat = model_fit.predict_proba(X)[:, 1]
            else:
                cur_y_hat = model_fit.predict(X)
            y_hat[split][response] = pd.Series(cur_y_hat)

    return y_hat


def get_all_model_preds(models_schema, models_table, ids=None):
    """
    Wrapper function to get all model predictions

    This loops over the models in the curent schema and generates their
    predictions on their training and testing sets. These train / test sets
    may be different for different models -- it looks up the table to use by
    inspecting the config information in the models database table.

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param list of strings ids A list of database model IDs to filter the
     results down to.
    :return dict preds A dictionary of predictions, whose keys are the 'model'
     ids from the models database table. Each element is a dictionary itself,
     keyed by train vs. test split. Within these, the Y's are n x r matrices,
     for n samples and r response types.
    :rtype dictionary of dictionaries

    Examples
    --------
    # Get train / test predictions from the underreporting schema
    preds = get_all_model_preds("models", "underreporting")
    """
    models = get_models(models_schema, models_table, ids)
    model_data_paths = set()
    all_model_data = dict()
    preds = dict()

    for model_key, model_result in models.items():
        model_fits = model_result["fits"]
        cur_path = model_result["model_data"]

        if cur_path not in model_data_paths:
            all_model_data[cur_path] = get_model_data(
                json.loads(cur_path),
                model_result["model_data_schema"],
                "intersection"
            )
            model_data_paths.add(cur_path)
        preds[model_key] = get_model_preds(model_fits, all_model_data[cur_path])

    return preds

def refit_underreporting_model_and_predict_on_sifode(models_schema, models_table, model_id):
    """
    Function to generate final ranked list for under reporting

    Given a model that was trained/test on different folds of the prospera data, 
    retrieve it, refit it on all prospera homes, and then predict under reporting
    probability for each house hold in sifode using it, and construct dataframe 
    to return which has home_id, prob_underreporting, estimated income, and 
    self reported income for each household.  This df will be used by shiny app
    to allow SEDESOL to interact with the data by changing weights.

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param string model_id A database model IDs filter the
    :return pd.DataFrame .  Home ids, probablity of underreporting, estimated and
    self reported income for every house in SIFODE db.

    Examples
    --------
    #get model 139 from models.underreporting_mult4 and return predicted list using it on all sifode houses.
    df_sifode_houses = refit_underreporting_model_and_predict_on_sifode( "models", "underreporting_mult4",'139') 
    """

    model_num = model_id     #model_id = "1"    -- to run on all prospera
                             #models_table = "underreporting_mult12"

    save_directory = '/mnt/data/sedesol/model_binaries/'

    logger.debug("Get model %s from %s" % (model_num, models_table))
    model = retrieve.get_models(models_schema, models_table, [model_num])    

    logger.debug("Retrain model on full prospera db")
    retrained_model = retrieve.retrain_model("semantic_underreport",model[model_num],"home_id")
    model_sifode = deepcopy(retrained_model)['retrained']

    try:
        logger.debug("Save results to tmp/model_sifode.pkl")
        with open(save_directory + 'model_sifode.pkl', "wb") as file_obj:
            pickle.dump(model_sifode, file_obj)
    except:
        warnings.warn("problem saving pickle results")


    logger.debug("Generate features/responses for whole sifode family table")
    
    #run on all sifode
    full_data_schema = "semantic_pub_imputation"
    model_sifode["subset"] = "sifode_houses_with_manzana_ifo"   
    sifode_data = retrieve.get_model_specified_features(full_data_schema, model_sifode, "home_id")

    #for dev purposes use these 3
    #full_data_schema = "semantic_underreport"
    #model_sifode["subset"] = "pmhmll20k"
    #sifode_data = retrieve.get_model_specified_features(full_data_schema, model_sifode, "home_id")


    logger.debug("Save features/response for sifode to tmp/sifode_data.csv")
    sifode_data.to_csv(save_directory + 'sifode_data.csv',index=False)
    
    logger.debug("Predict has_underreporting, has_discrepancy for whole sifode family table")
    y_hat = pd.DataFrame()
    for response, model_fit in model_sifode["fits"].items():
        y_hat[response] = model_fit.predict_proba(sifode_data)[:,1]
   
    logger.debug("Save yhat output to tmp/y_hat.csv")
    y_hat.to_csv(save_directory + 'y_hat.csv',index=False)

    logger.debug("Generate outputlist for shiny app")
   
    sifode_home_ids = pg_sed.get_original_features(['home_id','estimated_income','income_self_reported','type_of_locality'], full_data_schema, model_sifode["subset"], filter_condition=None, grouping_cols="home_id", aggregation_fun="DISTINCT ON")
    outputlist = pd.concat( [ sifode_home_ids, y_hat ], axis = 1)

    logger.debug("Save list and return it")
    outputlist.to_csv(save_directory + "final_outputlist_underreport.csv", index=False)
    return outputlist
    
def get_model_specified_features(semantic_schema, model_fit, grouping_cols):
    """
    Get the (un cv filtered) data associated with a model fit

    To retrain, it is necessary to extract features in the same was as was used
    to generate the CV filtered training data. This means the same features and
    preprocessing needs to be applied. This function extracts that data, using
    just the features and preprocessing JSON objects specified in the resultign
    model fit.

    :param string semantic_schema The name of the semantic schema within which
     the un-featurized data lives.
    :param dict model_fit A fitted model and metadata dictionary, as output by
     calling get_models() with a single id variable.
    :param string or list of strings groupign_cols The names of the columns that
     specify individual rows in the featurized data.
    :return pd.DataFrame X The feature matrix corresponding to the models
     training features, but using all the samples.
    """
    X = ft.get_features(
        model_fit["features_path"],
        semantic_schema,
        model_fit["subset"],
        None,
        grouping_cols
    )

    # preprocessing
    X = ft.preprocess_features(
        X, model_fit["preprocessing"]
    )

    # get columns that were used for training (some new levels might have
    # appeared in test that were not present in train)
    train_cols = get_model_data(
        json.loads(model_fit["model_data"]),
        model_fit["model_data_schema"],
        "intersection"
    )["train_features"].columns.values
    intersect_train_and_X = np.intersect1d(X.columns, train_cols)
    return X[intersect_train_and_X]


def retrain_model(semantic_schema, model_fit, grouping_cols):
    """
    Retrain a model on full data

    This retrieves the features and responses associated with a fitted model,
    but computed without any CV splitting. It then refits the same model type
    on the new data.

    :param string semantic_schema The name of the semantic schema within which
     the un-featurized data lives.
    :param dict model_fit A fitted model and metadata dictionary, as output by
     calling get_models() with a single id variable.
    :param string or list of strings groupign_cols The names of the columns that
     specify individual rows in the featurized data.
    :return dict A dictionary with the following elements
       X: The complete (not CV filtered) X pd.DataFrame used in retraining
       y: The complete (not CV filtered) y np.arary used in retraining
       retrained: A model object like model_fit, but with a new
          'fits' value, pointing to new retrained models instead. Also, it
           includes a new flag retrained_model['retrained'] = True in order to
           distinguish between the original and retrained models.
    :rtype dict
    """
    # get full data on which to nretrain
    logger.info("Retrieving full data to retrain model.")
    X = get_model_specified_features(semantic_schema, model_fit, grouping_cols)
    y = get_responses(
        model_fit["response"],
        semantic_schema,
        model_fit["subset"]
    )
    y_names = y.columns.values
    y = to_matrix(y)

    logger.info("Retraining model on full data.")
    retrained_model = deepcopy(model_fit)
    retrained_model["retrained"] = True
    for y_ix, y_name in enumerate(y_names):
        train_ix = np.isfinite(y[:, y_ix])
        retrained_model["fits"][y_name] = model_fit["fits"][y_name].fit(
            X.loc[train_ix, :],
            y[train_ix, y_ix]
        )

    return {"X": X, "y": y, "retrained": retrained_model}


def get_matching_models(models_schema, models_table, filter_condition):
    """
    Get the models that satisfy a SQL condition

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param string filter_condition The condition which the models returned must
     satisfy. This string must be able to follow the word WHERE in a SQL query.
     E.g., "subset_type = 'sifode'"" is a legitmate string argument.
    :return list of strings The IDs for models matching the specified condition,
     in the required models database table.
    :rtype list of strings

    Example
    -------
    matching_models = get_matching_models(
        "models",
        "pub_imputation_spatial_subsets",
        "subset = 'sifode_geo_census_subset_31'"
    )
    """
    model_query = """SELECT model
    FROM {}.{} WHERE {}""".format(
        models_schema,
        models_table,
        filter_condition
    )

    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(model_query)
    model_metadata = cursor.fetchall()
    return [m[0] for m in model_metadata]


def get_generalization_predictions(models_schema, models_table, model_id,
                                   training_schema, generalization_schema,
                                   generalization_table, training_grouping,
                                   generalization_grouping):
    """
    Get predictions on a generalization set

    This is mainly used to extract final predicted probabilities for subsets of 
    PUB, given models trained on associated subsets of SIFODE.

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param list of strings ids A list of database model IDs to filter the
     results down to.
    :param string generalization_schema The schema from which to draw the data
     to compute predictions on.
    :param string generalization_table The table within the generalization 
     schema that will be the source of predictions.
    :param training_groupings string or list of string The grouping parameter 
     when calling get_features for the training model data.
    :param generalization_groupings string or list of string The grouping 
     parameter when calling get_features for the training model data.
    :return pd.DataFrame y_hat A dataframe whose columns index predictions to 
     different responses.

    Example
    -------
    get_generalization_predictions(
        "models", 
        "pub_imputation_spatial_subsets",
        "14",
        "semantic_pub_imputation",
        "pub_geo_census_subset_17",
        "person_id",
        "sifode_pub_id"
    )
    """
    conn = db_connection()
    cursor = conn.cursor()
    
    model = get_models(
        models_schema,
        models_table,
        [model_id])[model_id]
    retrained = retrain_model(
        training_schema,
        model,
        training_grouping
    )

    model_copy = deepcopy(model)
    model_copy["subset"] = generalization_table
    generalization_data = get_model_specified_features(
        generalization_schema,
        model_copy,
        generalization_grouping
    )

    y_hat = pd.DataFrame()
    for response, model_fit in model["fits"].items():
        y_hat[response] = model_fit.predict_proba(generalization_data)[:, 1]

    return y_hat

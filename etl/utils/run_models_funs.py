"""
Helper functions for main model fitting and evaluation tasks in run_models.py
"""

import json
import warnings
from copy import deepcopy

import logging
import logging.config

import numpy as np
import pandas as pd
import sklearn.linear_model as lm
import sklearn.tree as tree
import sklearn.ensemble as ensemble
from sklearn.neighbors import KNeighborsClassifier
from sklearn.gaussian_process import GaussianProcess
from sklearn import svm
from sklearn import metrics
from sklearn.metrics import roc_auc_score
from collections import OrderedDict

from luigi import configuration
import utils.pg_sedesol as pg_sed

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")

###############################################################################
# Constants, specifying possible models and metrics
###############################################################################

MODELS_MAPPING = {
    "elnet": lm.ElasticNet(),
    "sgd_class": lm.SGDClassifier(),
    "sgd_reg": lm.SGDRegressor(),
    "ridge": lm.Ridge(),
    "gp": GaussianProcess(),
    "tree_reg": tree.DecisionTreeRegressor(),
    "tree_class": tree.DecisionTreeClassifier(),
    "extra_class": ensemble.ExtraTreesClassifier(),
    "extra_reg": tree.ExtraTreeRegressor(),
    "nn_class": KNeighborsClassifier(),
    "rf_reg": ensemble.RandomForestRegressor(),
    "rf_class": ensemble.RandomForestClassifier(),
    "svc": svm.SVC(),
    "linear_svc": svm.LinearSVC(),
    "logistic_reg": lm.LogisticRegression(),
    "multitask_lasso": lm.MultiTaskLasso(),
    "linear_reg": lm.LinearRegression()
}

MULTITASK_MODELS = ["multitask_lasso"]

###############################################################################
# Helper functions
###############################################################################

def get_model_dict(theta, cur_fold, cur_exper, responses, seed, data_path=None,
                   model_data_schema=None):
    """
    Return a string representing a model

    :param string model_name The name of the model type, as in the keys to
     MODELS_MAPPING.
    :param string theta A string of the dictionary encoding the parameter values
     for the current model run. This comes from a single entry of calling
     ParameterGrid() on a value in the models.json.
    :param int cur_fold The fold for the current training data.
    :param luigi.configuration conf The luigi configuration information.
    :return A dictionary giving specification of the model.
    :rtype dict
    """
    # load json object
    features_path = cur_exper["features"]
    preprocessing_path = cur_exper["preprocessing"]
    with open(features_path) as json_file:
        features = json.load(json_file)

    params_string = pg_sed.strip_punct(str(theta))
    model_dict = OrderedDict([
        ("model", cur_exper["model"]["name"]),
        ("responses", responses),
        ("features", cur_exper["features"]),
        ("subset", cur_exper["subset_name"]),
        ("params", params_string),
        ("preprocessing", cur_exper["preprocessing"]),
        ("fold", str(cur_fold)),
        ("seed", seed)
    ])

    # return the results
    model_string = pg_sed.hash_if_needed("".join(model_dict.values()))
    return {
        "string": model_string, "response": responses,
        "features_path": features_path, "features": features,
        "subset": cur_exper["subset_name"], "model": cur_exper["model"]["name"],
        "param": theta, "preprocessing": preprocessing_path, "fold": cur_fold,
        "seed": seed, "model_data_schema": model_data_schema,
        "model_data": data_path
    }

###############################################################################
# Get data, then fit + evaluate + upload data
###############################################################################

def get_model_data(basenames=None, schema_name="model_data",
                   resolution_type="intersection"):
    """
    Extract train and test data for a fold

    :param dict basenames The base of the table names associated with each
     fold, keyed by the type of table (train_features, train_response,
     test_features, and test_response). This is used to identify the table to
     read from the database. For example, if
    :param int cur_fold The fold of train / test data to extract
    :param string schema_name The schema within whith to look for the tables
     specified by basenames.
    :return dict data A dictionary keyed by the values in basenames, with values
     giving pandas dataframes for the correct train / test folds for these data.
    """
    n_tables = len(basenames)
    data = dict(zip(basenames.keys(), [None] * n_tables))
    conn = pg_sed.db_connection()

    for table_type, table_name in basenames.items():
        query = "SELECT * FROM %s.%s" % (schema_name, table_name)
        logger.info("Executing: %s", query)
        data[table_type] = pd.io.sql.read_sql(query, conn)

    data = resolve_unshared_features(data, resolution_type)
    conn.close()
    return data


def resolve_unshared_features(data, resolution_type="intersection",
                              fill_value=float("nan")):
    """
    Resolve features between train and test sets

    When one-hot encoding categorical columns, sometimes columns arise in the
    trainig set that were not visible in the test set, because the categories
    are only visible there. We could either specify in advance what the
    appropriate levels are, or fix the difference whenever it occurs. This
    function implements that second option. Depending on the resolution type,
    it sets the columns of the train and test data to be the union or
    intersection across the two.

    :param dict of pd.DataFrames data A dictionary with two keys,
     "test_features" and "train_features", pointing to the test and training
     data sets. The difference in column names between these data is the main
     issue.
    :param string resolution_type If "intersection", columns that are not shared
     between the two tables will be removed from both. If "union", new columns
     will be introduced and filled with fill_value until the column names agree
     across the two tables.
    :param float fill_value The value to fill in new columns with, in the
    "union" case.
    :return dict of pd.DataFrames A version of the train / test data with any
     differences between columns for the two data sets resolved.
    :rtype dict of pd.DataFrames
    """
    # identify the difference between column names in the datasets
    train_cols = set(data["train_features"].columns.values)
    test_cols = set(data["test_features"].columns.values)

    union_cols = train_cols.union(test_cols)
    intersection_cols = train_cols.intersection(test_cols)

    # if there is no difference, return
    if union_cols == intersection_cols:
        return data

    diff_cols = list(union_cols.difference(intersection_cols))
    warnings.warn("""Some columns only appear in only one of train vs. test: %s""" %
                  ",".join(diff_cols))

    # resolve the difference between columns
    if resolution_type == "intersection":
        keep_cols = list(intersection_cols)
    elif resolution_type == "union":
        for new_col in list(union_cols.difference(test_cols)):
            data["test_features"][new_col] = fill_value
        for new_col in list(union_cols.difference(train_cols)):
            data["train_features"][new_col] = fill_value

        keep_cols = list(union_cols)
    else:
        raise ValueError("resolution_type must be either 'union' or 'intersection'")

    # use columns as specified
    data["test_features"] = data["test_features"][keep_cols]
    data["train_features"] = data["train_features"][keep_cols]
    return data


def fit_model(model_name, theta, train_features, train_responses):
    """
    Fit a sklearn model with a given set of parameters

    :param string model_name The name of the model to train. The actual fitting
     function must be available through tehthe MODEL_MAPPING constant set at the
     top of this module.
    :param dict theta A dictionary specifying the parameters to use when fitting
     the model, in the typical sklearn .set_params() way.
    :param pd.DataFrame train_features A DataFrame giving the features to use
     when training a model.
    :param pd.DataFrame train_responsees A DataFrame giving the responses to use
     when training a model.
    :return dict of dicts results A list of dictionaries giving the fitted model
     object for each response dimension (or, "multitask" if a multitask model
     was used).
    :rtype dict
    """
    model = MODELS_MAPPING[model_name]
    model.set_params(**theta)

    # always convert to 2 dimensional (samples x responses)
    response_names = train_responses.columns.values
    train_responses = to_matrix(train_responses.astype("float64"))
    model_fits = {}

    if model_name not in MULTITASK_MODELS:
        for response_ix in range(train_responses.shape[1]):
            cur_response = train_responses[:, response_ix]
            train_ix = np.isfinite(cur_response)

            if not np.all(train_ix):
                warnings.warn("Dropping rows with null values in response: %s." %
                              response_names[response_ix])
                model.fit(
                    train_features.loc[train_ix, :],
                    cur_response[train_ix]
                )

            cur_fit = model.fit( train_features.loc[train_ix, :], cur_response[train_ix])
            model_fits[response_names[response_ix]] = deepcopy(cur_fit)
    else:
        if np.any(np.isnan(train_features)):
            raise NotImplementedError("""Multitask modeling does not account
            for NaNs at this point. Try another model.""")

        model_fits["-".join(responses_names)] = model.fit(train_features, train_responses)


    return model_fits


def evaluate_model(model_fits, data, metrics):
    """
    Evaluate a fitted model with a .predict() method

    :param model_fits A dictionary of fitted model objects (typically from
     sklearn.fit()). The only requirement though is that it has a .predict() or
     .predict_proba() method.
    :param dict data A dictionary keyed by the values in basenames, with values
     giving pandas dataframes for the correct train / test folds for these data.
    :param list of strings metrics, which must be associated with a
     corresponding eval_{metric}() function.
    :return results A dictionary with the metrics specified in metrics filled
     with values and all others set to None.
    """
    y = data["test_responses"]
    X = data["test_features"]

    # check input / initialize the results
    y_names = y.columns.values
    y = to_matrix(y)
    y_hat = np.empty((y.shape[0], 0))

    # make the predictions
    for y_name in y_names:
        model_fit = model_fits[y_name]
        if hasattr(model_fit, "predict_proba"):
            cur_y_hat = model_fit.predict_proba(X)[:, 1]
        else:
            cur_y_hat = model_fit.predict(X)

        logger.debug("Predicting %s , with yhat %s and cur_y_hat %s" % (y_name, y_hat.shape, cur_y_hat.shape))
        y_hat = np.hstack((y_hat, to_matrix(cur_y_hat)))

    # compute metrics
    results = {}
    for metric in metrics:
        eval_fun = globals()["eval_" + metric]
        results[metric] = eval_fun(y, y_hat, y_names)

    return results


def load_model_results(metrics, model_dict, models_schema="models",
                       models_table="models"):
    """
    Load a model's fit and evaluation metrics to the sedesol database

    :param model_fit The fitted model object (typically from sklearn.fit()). The
     only requirement though is that it has a .predict() or .predict_proba()
     method.
    :param dict metrics A dictionary with the metrics specified in metrics
     filled in with values and all others set to None.
    :param dict model_dict A dict specifying the form of the model. This is the
     output of get_model_dict().
    :return None
    :side-effects Loads a model to the sedesol database.
    :rtype None
    """
    connection = pg_sed.db_connection()
    cursor = connection.cursor()

    # create and insert a row into the model metadata table
    create_sql = """CREATE TABLE IF NOT EXISTS
    %s.%s_json
    (model BIGSERIAL, metadata JSONB);""" % (
        models_schema,
        models_table
    )
    cursor.execute(create_sql)

    insert_sql = """INSERT INTO %s.%s_json(metadata) VALUES(%s);""" % (
        models_schema,
        models_table,
        "%s"
    )

    metadata = str(json.dumps({"config": model_dict, "metrics": metrics}))
    cursor.execute(insert_sql, (metadata,))

    view_sql = """CREATE OR REPLACE VIEW %s.%s AS
    SELECT model,
    metadata->'config'->>'response' AS response,
    metadata->'config'->>'fold' AS fold,
    metadata->'config'->>'features_path' AS features_path,
    metadata->'config'->>'preprocessing' AS preprocessing,
    metadata->'config'->>'subset' AS subset,
    metadata->'config'->>'model_data' AS model_data,
    metadata->'config'->>'model_data_schema' AS model_data_schema,
    metadata->'config'->>'model' AS model_type,
    metadata->'config'->>'param' AS param,
    metadata->'config'->>'time_to_run' AS time_to_run,
    metadata->'metrics' AS metrics,
    metadata->'metrics'->>'mae' AS mae,
    metadata->'metrics'->>'mse' AS mse,
    metadata->'metrics'->>'enigh_income_precision_recall' AS enigh_income_precision_recall,
    metadata->'config'->>'seed' AS seed,
    metadata->'config'->>'string' AS string,
    metadata->'config'->>'run_date' AS run_date,
    metadata->'config'->>'binary_path' AS binary_path
    FROM %s.%s_json;""" % (
        models_schema,
        models_table,
        models_schema,
        models_table
    )
    cursor.execute(view_sql)
    connection.close()

###############################################################################
# model evaluation functions
###############################################################################
def to_matrix(y, task_names=None):
    """
    Convert to a 2-dimensional np array
    """
    y = np.array(y)
    if y.ndim == 1:
        n = len(y)
        y = y.reshape((n, 1))
    return y


def eval_rmse(y, y_hat, task_names=None):
    """
    Root mean squared error
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    rmses = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        rmses[task_names[j]] = float(np.sqrt(
            np.mean((y[eval_ix, j] - y_hat[eval_ix, j]) ** 2
            )))

    return json.dumps(rmses)


def eval_mae(y, y_hat, task_names=None):
    """
    Median absolute error
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    rmses = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        rmses[task_names[j]] = float(np.median(np.abs(
            (y[eval_ix, j] - y_hat[eval_ix, j]) ** 2
            )))

    return json.dumps(rmses)


def eval_raw_input(y, y_hat, task_names=None):
    """
    just return raw y and y_hat
    """
    y_df = pd.concat([pd.DataFrame(y), pd.DataFrame(y_hat)], axis=1)
    return y_df.to_json(orient="values")


def eval_enigh_income_precision_and_recall(y, yhat, task_names=None):
    """
    Return Recall and Precision at Thresholds defined by yhat income estimation
    param: y numpy.array of true responses
    param: yhat numpy.array of response predictions
    output: json object of thresholds list, along with recall list, precision
    list, and percentage of population under each threshold for each threshold
    level
    """
    ymin, ymax = int(yhat.min()), int(yhat.max())
    ks = np.linspace(ymin, ymax, 500)

    recall_at_k = []
    precision_at_k = []
    pct_below = []
    for k in ks:

        # for each income cutoff in ks, see how many people would actually be considered in poverty vs what we predicted
        ylabels = y < k
        yhatlabels = yhat < k

        number_scored = len(yhat) * 1.0
        cur_pct_below = len(yhat[yhat < k]) / number_scored
        pct_below.append(cur_pct_below)

        tp = 0.0     # truepostive count
        pp = 0.0     # predicted positive count
        ap = 0.0     # actual positive count

        for l, _ in enumerate(ylabels):
            if ylabels[l] and yhatlabels[l]:
                tp += 1
            if ylabels[l]:
                ap += 1
            if yhatlabels[l]:
                pp += 1

        if ap == 0:
            recall = 0
        else:
            recall = tp / ap

        if pp == 0:
            precision = 0
        else:
            precision = tp / pp

        recall_at_k.append(recall)
        precision_at_k.append(precision)

    threshholds_scaled = np.array(
        [np.interp(k, [ymin, ymax], [0, 1]) for k in ks]
    )

    return json.dumps({"threshholds": threshholds_scaled.tolist(),
                       "recall": recall_at_k,
                       "precision": precision_at_k,
                       "percentage_of_population": pct_below})


def eval_confusion_matrix(y, y_hat, task_names=None):
    """
    returns the confusion matrices for each task
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    if task_names == None:
        task_names =  [str(x) for x in range(y.shape[1] + 1)]

    confusions = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        confusions[task_names[j]] = metrics.confusion_matrix(
            y[eval_ix, j], y_hat[eval_ix, j]
        ).tolist()

    return json.dumps(confusions)


def eval_classification_report(y, y_hat, task_names=None):
    """
    returns the classfication report for each task:
        precision  recall  f1-score  support
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    if task_names == None:
        task_names =  [str(x) for x in range(y.shape[1] + 1)]

    reports = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        reports[task_names[j]] = metrics.classification_report(
            y[eval_ix, j], y_hat[eval_ix, j]
        )

    return json.dumps(reports)


def eval_under_reporting_precision_and_recall(y, yhat, task_names=None):
    """
    Return Recall and Precision at Thresholds defined by
           difference in report number of rooms vs confirmed number of rooms.
           y is the true number difference, 
           and yhat is the predicted difference
    param: y numpy.array of true responses
    param: yhat numpy.array of response predictions
    output: json object of thresholds list, along with recall list, 
        precision list, and percentage of population under each threshold
        for each threshold level
    """
    ymin, ymax = int(yhat.min()), int(yhat.max())
    ks = np.linspace(ymin, ymax, 100)

    recall_at_k = []
    precision_at_k = []
    pct_below = []

    yhat_scaled = np.array([np.interp(yh, 
        [ymin, ymax], [0, 1]) for yh in yhat])

    # This threshhold means we consider it under reporting if
    # you said you have 3 rooms and in actuality you have 5.

    diff_threshhold = 2

    ylabels = y < diff_threshhold
    for k in ks:

        # for each possible_diff in ks,
        yhatlabels = yhat_scaled < k
        number_scored = len(yhat_scaled) * 1.0
        cur_pct_below = len(yhat[yhat_scaled < k]) / number_scored
        pct_below.append(cur_pct_below)

        tp = 0.0     # truepostive count
        pp = 0.0     # predicted positive count
        ap = 0.0     # actual positive count

        for l, _ in enumerate(ylabels):
            if ylabels[l] and yhatlabels[l]:
                tp += 1
            if ylabels[l]:
                ap += 1
            if yhatlabels[l]:
                pp += 1

        if ap == 0:
            recall = 0
        else:
            recall = tp / ap

        if pp == 0:
            precision = 0
        else:
            precision = tp / pp

        recall_at_k.append(recall)
        precision_at_k.append(precision)

    threshholds_scaled = np.array(
        [np.interp(k, [ymin, ymax], [0, 1]) for k in ks]
    )

    yhat_bin = yhat < diff_threshhold
    auc_score = roc_auc_score(ylabels, yhat_bin)

    return json.dumps({"threshholds": threshholds_scaled.tolist(),
                       "recall": recall_at_k,
                       "precision": precision_at_k,
                       "percentage_of_population": pct_below,
                       "auc": auc_score})


def eval_precision_at_k(y, y_hat, task_names, grid_size=50):
    """
    Wrap eval_precision_at_k_array to work on 2d matrices
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    if task_names == None:
        task_names =  [str(x) for x in range(y.shape[1] + 1)]

    precisions = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        precisions[task_names[j]] = eval_precision_at_k_array(
            y[eval_ix, j], y_hat[eval_ix, j], grid_size
        )

    return json.dumps(precisions)


def eval_recall_at_k(y, y_hat, task_names, grid_size=50):
    """
    Wrap eval_recall_at_k_array to work on 2d matrices
    """
    y = to_matrix(y)
    y_hat = to_matrix(y_hat)

    if task_names == None:
        task_names = [str(x) for x in range(y.shape[1] + 1)]

    recalls = {}
    for j in range(y.shape[1]):
        eval_ix = np.where(np.isfinite(y[:, j]))[0]
        recalls[task_names[j]] = eval_recall_at_k_array(
            y[eval_ix, j], y_hat[eval_ix, j], grid_size
        )

    return json.dumps(recalls)


def eval_precision_at_k_array(y, y_hat, grid_size=50):
    """
    Compute precision at k along a grid of k

    :param np.array y array of true responses
    :param np.rray yhat response scores (between 0 and 1)
    :grid_size int grid_size The number of evenly spaced thresholds to use
     when setting the precision at k.
    :return list of float list of precisions along a grid
     list, and percentage of population under each threshhold for each
     threshhold level
    """
    precisions = []
    for k in range(grid_size):
        k_ix = int(np.floor((k / grid_size) * len(y)))
        threshold = np.sort(y_hat)[k_ix]
        y_indic = (y_hat > threshold).astype("float")
        precisions.append(metrics.precision_score(y, y_indic))

    return precisions


def eval_recall_at_k_array(y, y_hat, grid_size=50):
    """
    Compute recall at k along a grid of k

    :param np.array y array of true responses
    :param np.rray yhat response scores (between 0 and 1)
    :grid_size int grid_size The number of evenly spaced thresholds to use
     when setting the precision at k.
    :return list of float list of recalls along a grid
     list, and percentage of population under each threshhold for each
     threshhold level
    """
    recalls = []
    for k in range(grid_size):
        k_ix = int(np.floor((k / grid_size) * len(y)))
        threshold = np.sort(y_hat)[k_ix]

        y_indic = (y_hat > threshold).astype("float")
        recalls.append(metrics.recall_score(y, y_indic))

    return recalls


def expand_experiments(experiments_path, subsets, output_path=None):
    """
    Duplicate experiment json elements across subsets

    Given the path to a experiments JSON file and a collection of subsetting
    paths, write an expanded version of the experiemnts, with each experiments
    duplicated for each subset. This is useful for scaling up the same modeling
    approach to many different localities. We write an experiments file with one
    geographic subset in mind, and then generate a larger experiments file that
    reruns that experiment for each subset specified by the sql scripts in
    subset_paths.

    :param experiments_path The path to the JSON file containing the main
     experiments that we want to duplicate across subsets.
    :param dict of dicts subsets A dictionaries of dictionaires, each of which
     has two keys -- "subset_path" and "subset_name". The associated fields
     in the resulting experiments file will be filled in to have these subset
     paths and names. The keys in the outer dictionary will be prefixed to the
     current experiment ids in the resulting expanded experiments json file.
    :param list of strings subset_names A list of subset names to use p
     run the main experiments file over.
    :param string output_path The path to write the resulting experiments JSON
     file.
    :return None
    :rtype None
    :side-effects Writes an expanded version of the experiments file to the
     outputs path (or expanded-experiments_path is output_paths is not
     specified)

    Example
    -------

    subsets_names = [
        "sifode_geo_census_subset_01",
        "sifode_geo_census_subset_02"
    ]

    subsets = {}
    for _, subset_name in enumerate(subsets_names):
        subset_path = "./sql_queries/subset_data/subset_%s.sql" % subset_name
        subsets[subset_name] = {"subset_name": subset_name,
                                "subset_file": subset_path}

    experiments_path= "../conf/experiments/manzanas_pub_impute.json"
    expand_experiments(experiments_path, subsets)
    """
    with open(experiments_path) as json_file:
        experiments = json.load(json_file)

    expanded_experiments = {}
    for subset_key, subset_dict in subsets.items():
        for exper_id, exper in experiments.items():

            # replace experiment values with current subset dict's values
            new_dict = deepcopy(exper)
            for subset_field, subset_value in subset_dict.items():
                if subset_key in exper.keys():
                    warnings.warn("Overwriting %s field in experiment." % subset_field)
                new_dict[subset_field] = subset_value

            new_id = "%s_%s" % (exper_id, subset_key)
            expanded_experiments[new_id] = new_dict

    # default writing path
    if output_path is None:
        dirname = os.path.dirname(experiments_path)
        basename = os.path.basename(experiments_path)
        output_path = "%s/%s-%s" % (dirname, "expanded", basename)

    # write the result
    with open(output_path, "w")  as output_file:
        logger.info("Writing expanded experiments to %s." % output_file)
        json.dump(expanded_experiments, output_file, ensure_ascii=False,
                  indent=True)

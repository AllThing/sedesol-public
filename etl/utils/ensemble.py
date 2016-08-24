import utils.retrieve_model_fits as retrieve
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression


def stack_preds(p_test, y_test, p_new):
    """
    Stack Test Predictions

    :param np.array (n x M) p_test A matrix of fitted probabilities on the test
     set. n is the total number of test samples, and M is the number of models
     from which we have generated test predictions.
    :param np.array (length n) y_test The true responses in the test set.  These
     will be used to reweight models with good / bad out-of-sample error.
    :param np.array (n x M) p_new A matrix of fitted probabilities on a
     generalization set. Usually, these are the final model predictions, which
     we want to combine in some way.
    :return A np.array (n x R) The stacked probabilities, aggregating
     predictions across all the models.
    :rtype np.array

    Example
    -------
    n = 100
    M = 20
    R = 10
    y_test = np.array(np.random.random(n) > .4).astype("float")
    p_new = np.random.random((n, M))
    p_test = np.random.random((n, M))
    stack_preds(p_test, y_test, p_new)
    """
    model = LogisticRegression()
    keep_ix = np.where(np.isfinite(y_test))[0]
    model.fit(p_test[keep_ix, :], y_test[keep_ix])
    return model.predict_proba(p_new)[:, 1]


def stack_multi_preds(p_test_multi, y_test_multi, p_new_multi):
    """
    Stack Test Predictions for Multiple Responses

    Most of the problems we deal with have multiple responses. This wraps
    stack_preds() to perform stacking on several responsees at once.

    :param np.array (n X M x R) p_test_multi A matrix of fitted probabilities on
     the test et. n is the total number of test samples, M is the number of
     models from which we have generated test predictions, and R is the number
     of response dimensions along which must give predictions.
    :param np.array (n x R) y_test The true responses in the test set.  These
     will be used to reweight models with good / bad out-of-sample error.
    :param np.array (n X M x R) p_new A matrix of fitted probabilities on a
     generalization set. Usually, these are the final model predictions, which
     we want to combine in some way.
    :return A np.array (n x R) The stacked probabilities, aggregating
     predictions across all the models.
    :rtype np.array

    Example
    -------
    n = 100
    M = 20
    R = 10
    y_test = np.array(np.random.random((n, R)) > .4).astype("float")
    p_new = np.random.random((n, M, R))
    p_test = np.random.random((n, M, R))
    p_stacked = stack_multi_preds(p_test, y_test, p_new)
    """
    n, R, M = p_new_multi.shape
    p_stacked = np.zeros((n, R))

    for r in range(R):

        p_stacked[:, r] = stack_preds(
            p_test_multi[:, r, :],
            y_test_multi[:, r],
            p_new_multi[:, r, :]
        )
    return p_stacked


def get_stacking_input(models_schema, models_table, model_ids,
                       generalization_schema, generalization_table,
                       training_schema, training_grouping,
                       generalization_grouping):
    """
    Get Input Data for Stacking

    :param string models_schema The name of the schema containing the models to
     download.
    :param string models_table The name of the table containing the model
     results.
    :param list of strings model_ids A list of database model IDs to filter the
     results down to.
    :param string generalization_schema The schema from which to draw the data
     to compute predictions on.
    :param string generalization_table The table within the generalization
     schema that will be the source of predictions.
    :param string training_schema The semantic / semantic subset schema from
     which the data was generated for training the model.
    :param training_grouping string or list of string The grouping parameter
     when calling get_features for the training model data.
    :param generalization_grouping string or list of string The grouping
     parameter when calling get_features for the training model data.
    :return A tuple with the following elements
        - p_test (n x R x M) The fitted probabilities on test data
        - y_test (n x R) The truth on the test data
        - p_new (n x R x M) The fitted probabilities on generalization data
    :rtype tuple

    Example
    -------
    models_schema = "models"
    models_table = "pub_imputation_spatial_subsets"
    model_ids = ['31', '57']
    generalization_schema = "semantic_pub_imputation"
    generalization_table = "pub_geo_census_subset_08"
    training_schema = "semantic_pub_imputation"
    training_grouping = "person_id"
    generalization_grouping = "sifode_pub_id"

    stacking_input = get_stacking_input(
        models_schema,
        models_table,
        model_ids,
        generalization_schema,
        generalization_table,
        training_schema,
        training_grouping,
        generalization_grouping
    )
    """
    models = retrieve.get_models(models_schema, models_table, model_ids)

    p_tests = []
    p_news = []
    for model_id, model_metadata in models.items():

        # get associated data
        model_fits = model_metadata["fits"]
        basenames = json.loads(model_metadata["model_data"])
        model_data_schema = model_metadata["model_data_schema"]
        model_data = retrieve.get_model_data(
            basenames,
            model_data_schema,
            "intersection"
        )

        # get stacking input
        p_tests.append(
            retrieve.get_model_preds(model_fits, model_data)["test"]
        )
        if len(p_news) == 0:
            y_test = np.array(model_data["test_responses"])

        p_news.append(
            retrieve.get_generalization_predictions(
                models_schema,
                models_table,
                model_id,
                training_schema,
                generalization_schema,
                generalization_table,
                training_grouping,
                generalization_grouping
            )
        )

    p_test = np.dstack(tuple(p_tests))
    p_new = np.dstack(tuple(p_news))

    return p_test, y_test, p_new


def get_average_pred(p_multi):
    """
    The simplest ensembling approach: Average the predictions

    :param np.array (n X M x R) p_test_multi A matrix of fitted probabilities on
     the test et. n is the total number of test samples, M is the number of
     models from which we have generated test predictions, and R is the number
     of response dimensions along which must give predictions.
    """
    return p_multi.mean(axis=2)

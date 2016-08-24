"""
This just calls some of the code in etl/utils/retrieve_model_fits.py and
etl/utils/ensemble.py to arrive at final predictions for the PUB imputation
problem.
"""

import pandas as pd
import numpy as np
import glob
import utils.pg_sedesol as pg_sed
import utils.ensemble as ensemble

# Fetch predictions for each state and each model
subsets_names = [
    "geo_census_subset_01",
    "geo_census_subset_02",
    "geo_census_subset_03",
    "geo_census_subset_04",
    "geo_census_subset_05",
    "geo_census_subset_06",
    "geo_census_subset_07",
    "geo_census_subset_08",
    "geo_census_subset_09",
    "geo_census_subset_10",
    "geo_census_subset_11",
    "geo_census_subset_12",
    "geo_census_subset_13",
    "geo_census_subset_14",
    "geo_census_subset_1503",
    "geo_census_subset_150_except_1503",
    "geo_census_subset_15_except_150",
    "geo_census_subset_16",
    "geo_census_subset_17",
    "geo_census_subset_18",
    "geo_census_subset_19",
    "geo_census_subset_20",
    "geo_census_subset_21",
    "geo_census_subset_22",
    "geo_census_subset_23",
    "geo_census_subset_24",
    "geo_census_subset_25",
    "geo_census_subset_26",
    "geo_census_subset_27",
    "geo_census_subset_28",
    "geo_census_subset_29",
    "geo_census_subset_30",
    "geo_census_subset_31",
    "geo_census_subset_32"
]

for subset in subsets_names:
    matching_models = get_matching_models(
        "models",
        "pub_imputation_spatial_subsets",
        "subset = 'sifode_%s'" % subset
    )

    for model in matching_models:
        print("subset: %s | model: %s" % (subset, model))
        cur_probs = get_generalization_predictions(
            "models",
            "pub_imputation_spatial_subsets",
            str(model),
            "semantic_pub_imputation",
            "semantic_pub_imputation",
            "pub_" + subset,
            "person_id",
            "sifode_pub_id"
        )

        output_path = "mnt/data/sedesol/pub_preds/single_model/pub_predictions_subset_%s_model_%s" % (
            subset, model
        )
        cur_probs.round(2).to_csv(output_path, index=False)


# average these per model predictions
base_path = "/mnt/data/sedesol/pub_preds/single_model/pub_predictions_subset_geo_census_subset"
ensemble_path = "/mnt/data/sedesol/pub_preds/ensemble/"

state_ids = ["0" + str(s) for s in range(1, 10)] + \
            [str(s) for s in range(10, 15)] + \
            ["1503", "150_except_1503", "15_except_150"] + \
            [str(s) for s in range(16, 33)]

conn = pg_sed.db_connection()
cursor = conn.cursor()

for state in state_ids:
    print("starting state %s" % state)
    home_ids = pg_sed.get_original_features(["sifode_pub_id"], "semantic_pub_imputation",
                                            "pub_geo_census_subset_%s" % state,
                                            None, "sifode_pub_id")
    cur_paths = glob.glob("%s_%s*" % (base_path, state))
    p_hats = []
    for ix, path in enumerate(cur_paths):
        p_hats.append(np.array(pd.read_csv(path)))
    p_hat = np.dstack(tuple(p_hats))
    p_final = ensemble.get_average_pred(p_hat)
    p_final = pd.concat([home_ids, pd.DataFrame(p_final)], axis=1)
    pd.DataFrame(p_final).to_csv("%s/averages_%s.csv" % (ensemble_path, state))

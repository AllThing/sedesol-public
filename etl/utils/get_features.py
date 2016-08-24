"""
Pipeline task for generating features across CV folds
"""

import logging
import logging.config
import json
from collections import OrderedDict

import luigi
import luigi.postgres
from luigi import configuration

import utils.pg_sedesol as pg_sed
import utils.feature_funs as ft
from utils.load_cv_index import LoadCVIndices
from utils.create_schema import CreateSchema

import warnings

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


class GetFeatures(luigi.Task):
    """
    Luigi task to write features to csv locally
    """
    pipeline_task = luigi.Parameter()
    filter_condition = luigi.Parameter(default="")
    features_dict = luigi.Parameter(default="")

    conf = configuration.get_config()
    seed = conf.get("shared", "seed")
    features_dir = conf.get("etl", "tmp_path")


    def requires(self):
        model_data = self.conf.get(self.pipeline_task, "model_data_schema")
        return {"create_schema": CreateSchema(model_data),
                "load_cv_index": LoadCVIndices(self.pipeline_task)}


    def run(self):
        semantic = self.conf.get(self.pipeline_task, "semantic_schema")
        features_basename = self.conf.get(self.pipeline_task, "features_basename")
        features_dict = json.loads(self.features_dict, object_pairs_hook=OrderedDict)

        # get the appropriate aggregation level
        grouping_cols = self.conf.get(self.pipeline_task, "grouping_cols")
        grouping_cols = pg_sed.parse_cfg_string(grouping_cols)
        features = ft.get_features(
            features_dict["features"],
            semantic,
            features_dict["subset_name"],
            self.filter_condition,
            grouping_cols
        )

        processed_features = ft.preprocess_features(
            features, features_dict["preprocessing"]
        )

        features_str = features_basename + \
                       pg_sed.hash_if_needed("".join(features_dict.values()))
        pg_sed.write_data_with_schema_wrapper(
            processed_features,
            self.features_dir,
            self.filter_condition,
            features_str
        )


    def output(self):
        features_basename = self.conf.get(self.pipeline_task, "features_basename")
        features_dict = json.loads(self.features_dict, object_pairs_hook=OrderedDict)

        features_str = features_basename + \
                       pg_sed.hash_if_needed("".join(features_dict.values()))
        base_path = pg_sed.process_filter_name(
            self.features_dir,
            self.filter_condition,
            features_str
        )

        features_path = base_path + ".csv"
        schema_path = base_path + "_schema.csv"
        return [luigi.LocalTarget(features_path),
                luigi.LocalTarget(schema_path)]

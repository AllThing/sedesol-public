"""
Tasks for loading data from CSVs to a database

Our pipeline generates features locally using pandas. Using these tasks, it
loads the results to the sedesol postgres database.
"""

import logging
import logging.config
import json
import re
from collections import OrderedDict

import luigi
from luigi import configuration

import utils.pg_sedesol as pg_sed
from utils.get_features import GetFeatures
from utils.get_responses import GetResponses

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


class LoadCVData(luigi.WrapperTask):
    """
    Wrap LoadData() for each CV fold
    """
    pipeline_task = luigi.Parameter()
    data_dict = luigi.Parameter()
    table_type = luigi.Parameter(default="")
    cur_fold = luigi.Parameter(default=0)
    conf = configuration.get_config()

    def requires(self):
        """
        Use the filter_condition parameter to select all folds except for the
        held out one
        """
        models_data_table = self.conf.get(self.pipeline_task, "model_data_schema")

        if self.table_type == "features":
            table_basename = self.conf.get(self.pipeline_task, "features_basename")
        elif self.table_type == "responses":
            table_basename = self.conf.get(self.pipeline_task, "responses_basename")
        else:
            raise ValueError("""'table_type' parameter must be either features or
            responses.""")

        loaded_dict = json.loads(self.data_dict, object_pairs_hook=OrderedDict)
        table_str = table_basename + \
                    pg_sed.hash_if_needed("".join(loaded_dict.values()))

        # create train data
        filter_condition = "WHERE cv_index <> %s" % self.cur_fold
        train_table = "%s.%s_train_%s" % tuple(
            [models_data_table, table_str, self.cur_fold]
        )

        train_task = LoadData(pipeline_task=self.pipeline_task,
                              table=train_table,
                              table_type=self.table_type,
                              filter_condition=filter_condition,
                              data_dict=self.data_dict)

        # create test data
        filter_condition = "WHERE cv_index = %s" % self.cur_fold
        test_table = "%s.%s_test_%s" % tuple(
            [models_data_table, table_str, self.cur_fold]
        )

        test_task = LoadData(pipeline_task=self.pipeline_task,
                             table=test_table,
                             table_type=self.table_type,
                             filter_condition=filter_condition,
                             data_dict=self.data_dict)
        return [train_task, test_task]


class LoadData(pg_sed.CopyToTableWithTypes):
    """
    Upload features to a postgres database, using schema written to file

    This task takes as input three parameters,

    table: The name to give the table in the postgres database
    filter_condition: A subset of rows within the exper_id table to filter
    down to.
    exper_id: A subset of the semantic database which we assume to be present
    in the semantic.* schema
    """
    pipeline_task = luigi.Parameter()
    table = luigi.Parameter(default="postgres_table")
    table_type = luigi.Parameter(default="")
    filter_condition = luigi.Parameter(default="")
    data_dict = luigi.Parameter(default="")

    # CopyToTable throws an error if this is not provided, though we
    # overwrite it later
    columns = "tmp"

    # sedesol login options
    conf = configuration.get_config()
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    host = db_profile["PGHOST"]
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]


    def requires(self):
        """
        Load features or responses, depending on the table parameter
        """
        if self.table_type == "responses":
            return GetResponses(self.pipeline_task, self.filter_condition,
                                json.loads(self.data_dict)["subset_name"])
        elif self.table_type == "features":
            return GetFeatures(self.pipeline_task, self.filter_condition,
                               self.data_dict)

        else:
            raise ValueError("""'table_type' parameter must be either features
            or responses. It is currently set to: %s""" % self.table_type)

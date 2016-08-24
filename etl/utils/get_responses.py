"""
Pipeline task for generating responses
"""

import logging
import logging.config

import luigi
import luigi.postgres
from luigi import configuration

import utils.pg_sedesol as pg_sed
from utils.response_funs import write_responses
from utils.load_cv_index import LoadCVIndices
from utils.create_schema import CreateSchema

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


class GetResponses(luigi.Task):
    """
    This is a pipeline task for generating responses

    This can be used to get features for a fixed CV fold, via the
    filter_condition parameter (e.g., set "WHERE cv_index <> 1").

    It requires LoadCVIndex(), which creates a cv_index column in the
    semantic table, and CreateSchema(), which creates the schema to which
    these data get loaded.

    The output are csv files stored in responses_dir (specified in the
    luigi.cfg file). These are loaded to the database via the LoadData()
    class at a later step in the pipeline.
    """
    pipeline_task = luigi.Parameter()
    filter_condition = luigi.Parameter()
    subset_table = luigi.Parameter()
    conf = configuration.get_config()
    responses_dir = conf.get("etl", "tmp_path")

    def requires(self):
        """
        Make sure the database environment is set up
        """
        model_data = self.conf.get(self.pipeline_task, "model_data_schema")
        return {"create_schema": CreateSchema(model_data),
                "load_cv_index": LoadCVIndices(self.pipeline_task)}

    def run(self):
        """
        Get the responses for this subset of data
        """
        responses = self.conf.get(self.pipeline_task, "responses")
        semantic = self.conf.get(self.pipeline_task, "semantic_schema")
        responses_basename = self.conf.get(self.pipeline_task,
            "responses_basename")

        logger.info("Getting responses for %s", responses)
        write_responses(
            responses,
            semantic,
            self.subset_table,
            self.filter_condition,
            self.responses_dir,
            responses_basename
        )

    def output(self):
        """
        Write responses obtained by leaving out a fold at a time
        """
        responses_basename = self.conf.get(self.pipeline_task,
                                           "responses_basename")
        responses_str = responses_basename + \
                        pg_sed.hash_if_needed(self.subset_table)
        base_path = pg_sed.process_filter_name(self.responses_dir,
                                               self.filter_condition,
                                               responses_str)
        responses_path = base_path + ".csv"
        schema_path = base_path + "_schema.csv"

        return [luigi.LocalTarget(responses_path),
                luigi.LocalTarget(schema_path)]

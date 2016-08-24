# coding: utf-8

"""
This task creates indices on the clean tables to facilitate a later semantic
join. It requires a directory containing cleaning scripts, specified in the
luigi.cfg file.
"""

import logging
import logging.config

import luigi
import luigi.postgres
from luigi import configuration
import utils.pg_sedesol as pg_sed
from utils.raw_to_clean import RawToCleanSchema

LOGGING_CONF = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")

class CreateIndices(luigi.Task):
    """
    This executes the queries in the clean_table_indexer path

    The queries have to be done in sequence, because we need to first drop any
    queries before creating new ones. We also can't use the QueryString() class,
    because that doesn't have a require statement (and will try making indices
    indices before the clean tables are even created).
    """
    pipeline_task = luigi.Parameter()
    conf = configuration.get_config()
    logging_path = conf.get('etl', 'logging_path')

    def requires(self):
        return RawToCleanSchema(self.pipeline_task)

    def run(self):
        query_filename = self.conf.get(self.pipeline_task,
                                       'clean_table_indexer')
        connection = pg_sed.db_connection()
        connection.autocommit = True
        cursor = connection.cursor()

        all_queries = open(query_filename, 'r').read().split(';')
        for query in all_queries:
            if not query.isspace():
                logger.info("Executing query: " + query)
                cursor.execute(query)
                connection.commit()

        connection.close()
        output_path = "%s%s_clean_indices_created.log" % (
            self.logging_path, self.pipeline_task
        )
        open(output_path, 'a').close()

    def output(self):
        output_path = "%s%s_clean_indices_created.log" % (
            self.logging_path, self.pipeline_task
        )
        return luigi.LocalTarget(output_path)

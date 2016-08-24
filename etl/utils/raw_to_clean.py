"""
Run the individual SQL scripts to convert raw data to clean data.
"""

import os
import luigi
from luigi import configuration

from utils.create_schema import CreateSchema
import utils.pg_sedesol as pg_sed

class RawToCleanSchema(luigi.Task):
    """
        Wrapper task to create the clean schema from the raw schema.
        Executes all the .sql files in the cleaning scripts directory
    """
    pipeline_task = luigi.Parameter()

    conf = configuration.get_config()
    logging_path = conf.get('etl', 'logging_path')

    def requires(self):
        queries_path = self.conf.get(self.pipeline_task,
                                     'table_cleaning_queries_path')
        queries = {
            os.path.join(queries_path, query_file):
            pg_sed.extract_tablename_from_filename(query_file)
            for query_file in os.listdir(queries_path)
        }

        return [CreateSchema("clean")] + [
            pg_sed.QueryFile(query_file, table)
            for query_file, table in queries.items()
        ]

    def run(self):
        output_path = "%s%s_tables_cleaned.log" % (
            self.logging_path, self.pipeline_task
        )
        open(output_path, "a").close()

    def output(self):
        output_path = "%s%s_tables_cleaned.log" % (
            self.logging_path, self.pipeline_task
        )
        return luigi.LocalTarget(output_path)

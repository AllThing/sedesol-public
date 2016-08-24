"""
Pipeline task for setting up a schema for saving models
"""

import luigi
from luigi import configuration
import utils.pg_sedesol as pg_sed

class CreateSchema(luigi.Task):
    """
    Class to create a schema in the sedesol DB

    This expects a parameter giving the name of the schema to create.
    It has no dependencies.
    """
    schema_name = luigi.Parameter()
    conf = configuration.get_config()
    output_param = (conf.get('etl', 'logging_path') + '%s_schema_created.log')

    def requires(self):
        query_string = "CREATE SCHEMA IF NOT EXISTS %s;" % self.schema_name
        return pg_sed.QueryString(query_string)

    def run(self):
        output_path = self.output_param % self.schema_name
        open(output_path, 'a').close() # touch the output file

    def output(self):
        output_path = self.output_param % self.schema_name
        return luigi.LocalTarget(output_path)

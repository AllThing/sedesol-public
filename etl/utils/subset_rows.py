"""
Pipeline task for taking the full semantic database and subsetting to rows
using a SQL query specified in a configuration file.
"""

import json
import logging
import logging.config

import luigi
from luigi import configuration
from utils.clean_to_semantic import CleanToSemantic
import utils.pg_sedesol as pg_sed

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("sedesol.pipeline")


class SubsetRowsQuery(pg_sed.QueryFile):
    """
    Execute a subset rows query

    This subclasses the QueryFile class in order to execute a SQL file
    that performs the susetting. By subclassing, we can tell luigi to require
    the CleanToSemantic() task. This ensures that the full semantic table is
    available before we do any subsetting.

    The QueryFile class takes three parameters,

    - query_file: the file to query
    - table: The table name in the table_updates table that luigi maintains
    - substitutions: A json representing substitutions to make before executing
      the specified query file.

    See QueryFile() for more details. Don't be confused that CleanToSemantic()
    also subclasses this class, and gets its *own* arguments for these three
    parameters.
    """
    conf = configuration.get_config()
    pipeline_task = luigi.Parameter()

    def requires(self):
        query_file = self.conf.get(self.pipeline_task,
            "semantic_creation_file")
        return CleanToSemantic(query_file=query_file,
                               table=self.table,
                               substitutions=self.substitutions,
                               pipeline_task=self.pipeline_task)


class SubsetRows(luigi.Task):
    """
    This task filters the semantic database according to an input parameter.

    We expect that you have written SQL scripts that do the actual subsetting,
    and that these have been pointed to via the subset_files parameter in the
    luigi.cfg file.
    """
    pipeline_task = luigi.Parameter()
    conf = configuration.get_config()
    logging_path = conf.get('etl', 'logging_path')

    def requires(self):
        """
        Perform each of the the SQL queries specified in the conf file
        """
        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        subsets = pg_sed.unique_experiment_subsets(
            experiments_path,
            self.conf,
            self.pipeline_task
        )

        # send of query for each unique subset
        tasks = []
        for subset in subsets:
            substitutions = {
                "semantic_schema": self.conf.get(self.pipeline_task, 
                    "semantic_schema"),
                "semantic_table": self.conf.get(self.pipeline_task, 
                    "semantic_table"),
                "semantic_subset": subset[0]
            }

            logger.info("Subsetting rows according to %s", subset[1])
            tasks.append(
                SubsetRowsQuery(query_file=subset[1],
                                table=substitutions["semantic_subset"],
                                substitutions=json.dumps(substitutions),
                                pipeline_task=self.pipeline_task)
            )

        return tasks

    def run(self):
        """
        Touch the file saying that we've created the subsetted tables
        """
        output_path = "%scompleted_subsetting_%s.log" % (
            self.logging_path, self.pipeline_task
        )
        open(output_path, 'a').close()

    def output(self):
        """
        Check that we've written the script indicating success
        """
        output_path = "%scompleted_subsetting_%s.log" % (
            self.logging_path, self.pipeline_task
        )

        return luigi.LocalTarget(output_path)

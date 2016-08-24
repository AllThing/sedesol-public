"""
Pipeline task for transforming many clean tables into a single semantic one.
"""

import luigi
from luigi import configuration

import utils.pg_sedesol as pg_sed
from utils.create_index import CreateIndices
from utils.create_schema import CreateSchema


class CleanToSemantic(pg_sed.QueryFile):
    """
    Task to join multiple clean tables into a semantic database

    This task assumes the existence of
       (1) A SQL script to perform the actual joining
       (2) A collection of clean databases, a schema called "clean"
       (3) A set of indices for each column in each table of that database, to
           speed up the join.

    It executes the script in (1), which as a side-effect creates a
    semantic table in a schema called semantic.
    """
    pipeline_task = luigi.Parameter()
    conf = configuration.get_config()

    def requires(self):
        """
        This checks the existence of (3) in the class definition.
        """
        semantic_schema = self.conf.get(self.pipeline_task, "semantic_schema")
        return [CreateSchema(semantic_schema),
                CreateIndices(self.pipeline_task)]

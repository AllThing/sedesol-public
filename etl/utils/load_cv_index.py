"""
This task is a component of the pipeline that modifies the subsetted semantic
database to include two new columns,
    - row_number: A row number to impose ordering on the database
    - cv_index: The fold membership of each row in the database
"""

import logging
import logging.config

import luigi
import luigi.postgres
from luigi import configuration

from sklearn.cross_validation import KFold, LabelKFold

from utils.subset_rows import SubsetRows
import utils.pg_sedesol as pg_sed
import utils.feature_funs as ft

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("sedesol.pipeline")


def get_cv_indices(n_rows, k_folds, seed, substitutions, cv_grouping_cols=None):
    """
    Get cv indices from sklearn

    ENIGH raw data has several expense types per household, and we don't
    want the same household split across several cv folds, so use
    LabelCVFold

    :param string or list of strings cv_grouping_cols The feature(s) to use to
     define groupings in cv folds. We don't want samples within these groups to
     appear in separate folds (because that would give the model extra
     information about test).
    :param int n_rows The number of rows total across which we need to divide
     into folds.
    :param int k_folds The number of cross-validation folds
    :param int seed The random state to use
    :param dict substitutions. The dictionary containing schema and
     table names to substitute into the parameterized SQL.
    """
    if cv_grouping_cols is not None:
        labels = pg_sed.get_original_features(
            cv_grouping_cols,
            substitutions["semantic_schema"],
            substitutions["subset_table"]
        )
        labels = labels.sort_values(by=cv_grouping_cols)
        labels = labels.ix[:, 0].values
        return LabelKFold(labels, k_folds)
    else:
        return KFold(n_rows, k_folds, shuffle=True,
                     random_state=seed)


class LoadCVIndices(luigi.WrapperTask):
    """
    Create CV indices for each current subset of the semantic table
    """
    pipeline_task = luigi.Parameter()
    conf = configuration.get_config()

    def requires(self):

        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        subsets = pg_sed.unique_experiment_subsets(
            experiments_path,
            self.conf,
            self.pipeline_task
        )

        tasks = []
        for subset in subsets:
            tasks.append(
                LoadCVIndex(self.pipeline_task, subset_table=subset[0])
            )

        return tasks


class LoadCVIndex(luigi.Task):
    """
    Luigi task to insert cross-validation indices in the subsetted semantic DB
    """
    pipeline_task = luigi.Parameter()
    subset_table = luigi.Parameter()
    conf = configuration.get_config()

    row_number_generator = conf.get('etl', 'row_number_generator')
    seed = int(conf.get('shared', 'seed'))
    k_folds = int(conf.get('shared', 'k_folds'))

    def requires(self):
        return SubsetRows(self.pipeline_task)


    def run(self):
        conn = pg_sed.db_connection()
        cursor = conn.cursor()

        # create an row_number in the table
        cv_grouping_cols = self.conf.get(self.pipeline_task, "cv_grouping_cols")
        cv_grouping_cols = pg_sed.parse_cfg_string(cv_grouping_cols)
        substitutions = {
            "\n": " ",
            "subset_table": self.subset_table,
            "semantic_schema": self.conf.get(self.pipeline_task, "semantic_schema"),
            "cv_order_condition": "ORDER BY {}".format(cv_grouping_cols[0])
        }
        queries = pg_sed.parse_query_strings(self.row_number_generator,
                                             substitutions)

        for query in queries:
            logger.info("Executing %s", query)
            cursor.execute(query)
            conn.commit()

        n_rows = ft.get_n_rows_in_table(substitutions["semantic_schema"],
                                        substitutions["subset_table"])

        logger.info("Creating CV indices using sklearn")
        cv_iterator = get_cv_indices(n_rows, self.k_folds, self.seed,
                                     substitutions, cv_grouping_cols)

        logger.info("Uploading CV indices to postgres")
        for fold, fold_indices in enumerate(cv_iterator):
            # update rows in the current fold (note that python is zero indexed)
            cur_rows = ",".join([str(indices + 1) for indices in fold_indices[1]])
            update_query = "UPDATE %s.%s SET cv_index = %s WHERE row_number in (%s) " % (
                substitutions["semantic_schema"], substitutions["subset_table"],
                str(fold), cur_rows
            )
            logger.info("Executing CV update for fold %s", fold)
            cursor.execute(update_query)


        logger.info("Creating postgres indices on cv_index column")
        index_query = "CREATE INDEX %s_cv_index_idx ON %s.%s(cv_index);" %(
            substitutions["subset_table"],
            substitutions["semantic_schema"],
            substitutions["subset_table"]
        )
        index_query = index_query.replace("%s", substitutions["subset_table"])
        cursor.execute(index_query)

        conn.close()

        # touch the output file
        output_path = "%screated_cv_%s.log" % (
            self.conf.get("etl", "logging_path"),
            self.pipeline_task
        )
        open(output_path, 'a').close()


    def output(self):
        output_path = "%screated_cv_%s.log" % (
            self.conf.get("etl", "logging_path"),
            self.pipeline_task
        )
        return luigi.LocalTarget(output_path)

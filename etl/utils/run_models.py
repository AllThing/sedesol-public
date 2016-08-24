"""
Luigi tasks to run models over already constructed cv databases
"""

import ast
import logging
import logging.config
import json
import os
import pickle
import time
from collections import OrderedDict

import luigi
from luigi import configuration
import utils.run_models_funs as model_funs
import utils.pg_sedesol as pg_sed
from utils.load_data import LoadCVData
from utils.create_schema import CreateSchema
from sklearn.grid_search import ParameterGrid


LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")

class RunModels(luigi.WrapperTask):
    """
    Run, evaluate, and load many models

    This runs models for different model / parameter / subset configurations,
    as specified in the luigi.cfg file, and saves the resulting models to the
    database.
    """
    pipeline_task = luigi.Parameter()
    conf = configuration.get_config()
    k_folds = conf.get("shared", "k_folds")

    def requires(self):
        """
        Set off a task for all the model / parameter / fold combinations

        This is heavily influenced by https://github.com/rayidghani/magicloops/
        """
        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        with open(experiments_path) as experiments_file:
            experiments_dict = json.load(experiments_file)

        tasks = []
        for exper_id, exper in experiments_dict.items():
            params = {param: ast.literal_eval(values) for
                      param, values in exper["model"]["params"].items()}
            parameter_combns = ParameterGrid(params)

            for theta in parameter_combns:
                for cur_fold in range(int(self.k_folds)):
                    cur_task = RunModel(exper_id, str(theta), str(cur_fold),
                                        self.pipeline_task)
                    tasks.append(cur_task)

        return tasks


class RunModel(luigi.Task):
    """
    Run, evaluate, and load a model for a specific data / parameter combination
    """

    # setup parameters
    exper_id = luigi.Parameter()
    theta = luigi.Parameter()
    cur_fold = luigi.Parameter()
    pipeline_task = luigi.Parameter()

    conf = configuration.get_config()
    logging_path = conf.get("etl", "logging_path")
    binaries_path = conf.get("etl", "binaries_path")
    seed = conf.get("shared", "seed")
    models_schema = conf.get("shared", "models_schema")
    conn = pg_sed.db_connection()


    def requires(self):
        """
        We need the cross-validation-fold featurized data
        """
        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        with open(experiments_path) as experiments_file:
            experiments_dict = json.load(experiments_file)

        cur_exper = pg_sed.fill_experiment_defaults(
            experiments_dict[self.exper_id],
            self.conf,
            self.pipeline_task
        )

        features_dict = OrderedDict((
            ("subset_name", cur_exper["subset_name"]),
            ("features", cur_exper["features"]),
            ("preprocessing", cur_exper["preprocessing"])
        ))
        responses_dict = {"subset_name": cur_exper["subset_name"]}
        yield CreateSchema(self.models_schema)
        yield {"load_features": LoadCVData(self.pipeline_task,
                                           json.dumps(features_dict),
                                           "features", self.cur_fold),
               "load_responses": LoadCVData(self.pipeline_task,
                                            json.dumps(responses_dict),
                                            "responses", self.cur_fold)}


    def run(self):
        """
        Run, evaluate, and load a model
        """
        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        with open(experiments_path) as experiments_file:
            experiments_dict = json.load(experiments_file)

        cur_exper = pg_sed.fill_experiment_defaults(
            experiments_dict[self.exper_id],
            self.conf,
            self.pipeline_task
        )

        features_dict = OrderedDict((
            ("subset_name", cur_exper["subset_name"]),
            ("features", cur_exper["features"]),
            ("preprocessing", cur_exper["preprocessing"])
        ))

        features_basename = self.conf.get(self.pipeline_task, "features_basename")
        features_str = features_basename + \
                       pg_sed.hash_if_needed("".join(features_dict.values()))

        responses_basename = self.conf.get(self.pipeline_task,
                                           "responses_basename")
        responses_str = responses_basename + \
                        pg_sed.hash_if_needed(features_dict["subset_name"])

        basenames = {"train_features": "%s_train_%s" % (features_str, self.cur_fold),
                     "train_responses": "%s_train_%s" % (responses_str, self.cur_fold),
                     "test_features": "%s_test_%s" % (features_str, self.cur_fold),
                     "test_responses": "%s_test_%s" % (responses_str, self.cur_fold)}

        # get model data
        data = model_funs.get_model_data(
            basenames,
            self.conf.get(self.pipeline_task, "model_data_schema")
        )

        model_dict = model_funs.get_model_dict(
            self.theta,
            self.cur_fold,
            cur_exper,
            self.conf.get(self.pipeline_task, "responses"),
            self.seed,
            basenames,
            self.conf.get(self.pipeline_task, "model_data_schema")
        )

        # fit the model
        start = time.time()
        model_fit = model_funs.fit_model(
            cur_exper["model"]["name"],
            ast.literal_eval(self.theta),
            data["train_features"],
            data["train_responses"]
        )
        model_dict["run_date"] = time.strftime('%Y-%m-%d %H:%M:%S',
                                               time.localtime(start))
        model_dict["time_to_run"] = time.time() - start

        # save model
        model_dict["binary_path"] = os.path.join(
            self.binaries_path,
            "%s.pkl" % model_dict["string"]
        )
        with open(model_dict["binary_path"], "wb") as file_obj:
            pickle.dump(model_fit, file_obj)

        # evaluate the model
        metrics_list = pg_sed.parse_cfg_string(cur_exper["metrics"])
        model_eval = model_funs.evaluate_model(
            model_fit,
            data,
            metrics_list
        )

        # load model
        model_funs.load_model_results(
            model_eval,
            model_dict,
            self.models_schema,
            self.conf.get(self.pipeline_task, "models_table")
        )

        # if successful, log it
        output_path = os.path.join(
            self.logging_path,
            "models",
            "model%s_%s.log" % (
                self.pipeline_task,
                model_dict["string"]
            )
        )
        open(output_path, "a").close()


    def output(self):
        """
        We output a log file specifying the model type
        """
        experiments_path = self.conf.get(self.pipeline_task, "experiments")
        with open(experiments_path) as experiments_file:
            experiments_dict = json.load(experiments_file)

        cur_exper = pg_sed.fill_experiment_defaults(
            experiments_dict[self.exper_id],
            self.conf,
            self.pipeline_task
        )

        features_dict = OrderedDict((
            ("subset_name", cur_exper["subset_name"]),
            ("features", cur_exper["features"]),
            ("preprocessing", cur_exper["preprocessing"])
        ))

        features_basename = self.conf.get(self.pipeline_task, "features_basename")
        features_str = features_basename + \
                       pg_sed.hash_if_needed("".join(features_dict.values()))

        responses_basename = self.conf.get(self.pipeline_task,
                                           "responses_basename")
        responses_str = responses_basename + \
                        pg_sed.hash_if_needed(cur_exper["subset_name"])
        basenames = {"train_features": "%s_train_%s" % (features_str, self.cur_fold),
                     "train_responses": "%s_train_%s" % (responses_str, self.cur_fold),
                     "test_features": "%s_test_%s" % (features_str, self.cur_fold),
                     "test_responses": "%s_test_%s" % (responses_str, self.cur_fold)}

        model_dict = model_funs.get_model_dict(
            self.theta,
            self.cur_fold,
            cur_exper,
            self.conf.get(self.pipeline_task, "responses"),
            self.seed,
            basenames,
            self.conf.get(self.pipeline_task, "model_data_schema")
        )


        # if successful, log it
        output_path = "%s/models/model%s_%s.log" % (
            self.logging_path,
            self.pipeline_task,
            model_dict["string"]
        )

        return luigi.LocalTarget(output_path)

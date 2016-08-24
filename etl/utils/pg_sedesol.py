"""
Utilities used throughout the sedesol luigi pipeline
"""

import logging
import logging.config
import json
import psycopg2
import pandas as pd
import os
import re
import hashlib
import string

import luigi
import luigi.postgres
from luigi import configuration
from luigi import six
from pandas.compat import range, lzip, map, zip
import datetime
import numpy as np

LOGGING_CONF = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")


def extract_tablename_from_filename(str_to_clean):
    """
        Helper function to extract table name from filename. Removes the
        leading 'clean_' and trailing '.sql' in filenames.

        Args:
            string to clean
        Returns:
            cleaned string
    """
    strs_to_strip = ['clean_', '.sql']
    for cur_str in strs_to_strip:
        str_to_clean = str_to_clean.replace(cur_str, '')
    return str_to_clean


def parse_query_strings(sql_file, sub_dict=None):
    """
    Parse a SQL file containing queries

    This function takes the path to a file containing SQL queries separated by
    semicolons and returns a list with each individual query string as an
    element.

    :param string sql_file The path to the file that we want to parse.
     We assume all queries in this file are separated by a semicolon.
    :param dict sub_dict A dictionary representing substitutions to make
     between the raw input file and the queries that are output. This is
     useful for writing parameterized SQL
    :return list of string A list of strings, each of which corresponds to one
     of the SQL queries in the original file
    """
    if sub_dict is None:
        sub_dict = {"\n": " "}

    queries = open(sql_file).read()
    for old_str, new_str in sub_dict.items():
        queries = queries.replace(old_str, new_str)

    queries = queries.split(";")
    return [q + ";" for q in queries if not q.isspace()]  # remove empty strings, and add semicolon


def db_connection():
    """
    Connect to the sedesol database

    This connects to the sedesol database using the parameters in the luigi.cfg
    file.

    :return connection The connection object resulting after connecting to the
     sedesol database with the appropriate parameters.
    """
    conf = configuration.get_config()

    # connect to the postgres database
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    host = db_profile["PGHOST"]
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]

    conn_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'" % (
        host, port, database, user, password
    )
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    return conn


class SedesolPostgres(luigi.postgres.PostgresQuery):
    """
    Subclass of PostgresQuery that provides default database information

    This reads the configuration file to log into the database.
    """
    query, table = None, None

    conf = configuration.get_config()
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    host = "%s:%s" % (db_profile["PGHOST"], port)
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]


class QueryString(SedesolPostgres):
    """
        Template task for querying the SEDESOL database

        Inherits from luigi.postgres.PostgresQuery and over-rides the `host`,
        `port`, `database`, `user`, and `password` members so that we can
        directly subclass from this class instead of having to over-ride each
        time

        Usage:
            Subclass and over-ride the `query` and `table` attributes.
            Call this by including a query, 
            like QueryString("select * from table;")
    """
    query = luigi.Parameter()
    table = luigi.Parameter(default=None)

    def run(self):
        """
        This is a slight variation on
        http://luigi.readthedocs.io/en/stable/_modules/luigi/postgres.html
        that runs a query from a file rather than a string parameter.
        """
        connection = self.output().connect()
        connection.autocommit = True
        cursor = connection.cursor()
        logger.info('Executing query from task: %s', self.__class__)
        cursor.execute(self.query)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


class QueryFile(SedesolPostgres):
    """
        Template task for querying the SEDESOL database

        Inherits from luigi.postgres.PostgresQuery and over-rides the `host`,
        `port`, `database`, `user`, and `password` members so that we can
        directly subclass from this class instead of having to over-ride each
        time

        Usage:
            Subclass and over-ride the `query` and `table` attributes
    """
    query_file = luigi.Parameter()
    table = luigi.Parameter(default=None)
    substitutions = luigi.Parameter(default="{}")
    query = None

    def run(self):
        """
        This is a slight variation on
        http://luigi.readthedocs.io/en/stable/_modules/luigi/postgres.html
        that runs a query from a file rather than a string parameter.
        """
        connection = self.output().connect()
        cursor = connection.cursor()

        # get query and make substitutions
        substitutions_dict = json.loads(self.substitutions)
        query = open(self.query_file).read().replace("\n", " ")
        for to_replace, sub in substitutions_dict.items():
            query = query.replace(to_replace, sub)

        logger.info('Executing query from task: %s', self.__class__)
        cursor.execute(query)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


class SedesolPostgresTarget(luigi.postgres.PostgresTarget):
    """
    Subclass of PostgresTarget that let's us avoid setting database parameters
    every time.

    This follows the construction of PostgresTarget defined in:
    http://luigi.readthedocs.io/en/stable/_modules/luigi/postgres.html
    """
    conf = configuration.get_config()
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]
    update_id = None

    def open(self):
        pass

    def __init__(self, table):
        self.table = table

    def output(self):
        """
        Returns a PostgresTarget representing the executed query.
        """
        return luigi.postgres.PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )


class CopyToTableWithTypes(luigi.postgres.CopyToTable):
    """
    Template task for inserting a data set into Postgres

    This is a variation of luigi.postgres.CopyToTable that expects as
    input a list of two paths,
        - The path to a csv file to upload to the database
        - The path to a csv file with two columns: the name of the corresponding
          column in the features file and its postgresql type

    Essentially everything is the same as in
    https://github.com/spotify/luigi/blob/master/luigi/postgres.py
    except it defines the columns variable during the copy method, by reading
    the path from the second input() element.

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table`.
    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.
    """
    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        tsv_file, _ = self.input()
        with tsv_file.open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def copy(self, cursor, file):
        self.columns = []

        # read schema from a file
        _, column_types_path = self.input()
        with column_types_path.open('r') as column_types_file:
            for line in column_types_file.readlines():
                self.columns.append(line.split(","))

        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))

        cursor.copy_from(file, self.table, null=r'\\N', 
            sep=self.column_separator, columns=column_names)


def process_filter_name(path, filter_condition=None, basename="features"):
    """
    Prepare a path name from a filter condition
    """
    if filter_condition is None:
        filter_condition = ""

    subset_id = filter_condition.lower()
    for replace_str in ["where cv_index", " "]:
        subset_id = subset_id.replace(replace_str, "")

    subset_id = subset_id.replace("=", "test_")
    subset_id = subset_id.replace("<>", "train_")
    subset_id = subset_id.replace(" ", "")

    return ("%s%s_%s") % (path, basename, subset_id)


def get_df_sqltypes(data):
    """
    Returns the sql datatypes corresponding to the dataframe columns

    :param pd.DataFrame of pd.Series dataframe
    :returns a list of tuples of the format (column name, sql datatype)
    :rtype  [list of tuples]
    """
    data = pd.DataFrame(data)
    lookup_type = lambda dtype: get_sqltype(dtype.type)
    column_names = [str(s).replace(" ", "_") for s in data.columns.values]
    column_types = lzip(column_names, map(lookup_type, data.dtypes))
    return column_types


def get_sqltype(pytype):
    """
    Returns the SQL data type corresponding to a python data type.
    This function is adapted from:
        https://gist.github.com/jorisvandenbossche/10841234

    :param  pytype python datatype
    :returns sql datatype
    :rtype  string
    """
    sqltype = 'VARCHAR (64)'

    if issubclass(pytype, np.floating):
        sqltype = 'double precision'

    if issubclass(pytype, np.integer):
        sqltype = 'integer'

    if issubclass(pytype, np.datetime64) or pytype is datetime:
        sqltype = 'timestamp'

    if pytype is datetime.date:
        sqltype = 'date'

    if issubclass(pytype, np.bool_):
        sqltype = 'boolean'

    return sqltype


def write_data_with_schema_wrapper(data, data_dir, filter_condition,
                                   basename):
    base_path = process_filter_name(data_dir, filter_condition, basename)
    data_path = base_path + ".csv"
    schema_path = base_path + "_schema.csv"

    logger.info("Writing data to %s", data_path)
    write_data_with_schema(data, data_path, schema_path)


def write_data_with_schema(data, data_path, schema_path):
    """
    Write a dataframe and its schema into separate csv files

    :param pd.DataFrame data The dataframe to write to disk
    :param string data_path File path for writing the dataframe
    :param string schema_path File path for writing the column schema
    """
    data.to_csv(data_path, index=False, sep="\t", header=False, na_rep=r"\N")

    # write schema to file
    with open(schema_path, "w") as schema_file:
        column_types = get_df_sqltypes(data)
        schema_file.write('\n'.join('%s,%s' % x for x in column_types))


def parse_grouping_cols(grouping_cols):
    if type(grouping_cols) == list:
        grouping_str = ",".join(grouping_cols)
    elif type(grouping_cols) == str:
        grouping_str = grouping_cols
    elif grouping_cols is None:
        grouping_str = ""
    else:
        raise ValueError("""'grouping_cols' must be string, list of strings,
        or None""")
    return grouping_str


def get_original_features(features, schema_name, subset_type,
                          filter_condition=None, grouping_cols=None,
                          aggregation_fun="DISTINCT ON"):
    """
    Get features directly from a database

    The original features are defined as those contained in the
    semantic.semantic database. These can be obtained quickly through a single
    postgres query.

    :param list of strings features A list of feature names that are columns in
     the semantic.semantic database. It can be a list of length 1.
    :param string subset_type The name of the semantic data subset to  extract.
     We assume there is a table named semantic.{subset_type} in the sedesol
     database.
    :param string filter_condition A string to post-pend to the database query
     specifing a subset of the data to filter to (e.g., WHERE cv_index == 3)
    :param string or list of strings grouping_cols The columns on which to
     calculate the aggregation. The result is also ordered by these columns.
    :param aggregation_fun A function used to perform the aggregation over the
     columns specified by the grouping cols. This only has an effect if
     grouping_cols is not None.
    :return pd.DataFrame A DataFrame corresponding to the input features queried
     from the database
    :rtype pd.DataFrame

    example
    -------
    features =  ["payment_amount", "estimated_income"]
    schema_name = "semantic"
    subset_type = "sifode_sample_100000"
    filter_condition = "where cv_index <> 0"
    grouping_cols = "person_id"
    aggregation_fun = "SUM"
    total_payments = get_original_features(
       features, schema_name, subset_type,
       filter_condition, grouping_cols, aggregation_fun
    )
    """
    # parse arguments
    if filter_condition is None:
        filter_condition = ""

    if type(features) == str:
        features = [features]

    grouping_str = parse_grouping_cols(grouping_cols)

    # consider alternative aggregation options
    query = "SELECT "
    grouping_query = ""
    if grouping_cols != None:

        # setup the distinct on to extract the first element in each group
        if aggregation_fun.lower() == "distinct on":
            query = query + "%s(%s)" % (
                aggregation_fun,
                grouping_str
            )
            grouping_query = "ORDER BY %s" % (grouping_str)

        # specify an aggregation over each feature
        else:
            aggregation_str = ["%s(%s) as sum_%s" % (aggregation_fun, 
                feature, feature) for feature in features]
            query = "%s %s" % (
                query,
                ",".join(aggregation_str)
            )
            features = "" # so we don't use the feature any more

            grouping_query = "GROUP BY %s ORDER BY %s" % (
                grouping_str,
                grouping_str
            )

    # perform the query
    query = "%s %s FROM %s.%s %s %s;" % (
        query,
        ",".join(features),
        schema_name,
        subset_type,
        filter_condition,
        grouping_query
    )

    logger.info("Executing: %s", query)
    conn = db_connection()
    X = pd.io.sql.read_sql(query, conn)
    x_dtypes = X.dtypes

    # booleans can't have nans, so convert to float64, see
    # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#na-type-promotions
    for j in range(X.shape[1]):
        if x_dtypes[j] == "bool":
            X.ix[:, j] = X.ix[:, j].astype("float64")
        elif x_dtypes[j] == "object":
            cur_unique = set(X.ix[:500, j])
            bool_set = set([True, False, None])
            if cur_unique.issubset(bool_set):
                X.ix[:, j] = X.ix[:, j].astype("float64")

    conn.close()

    return X


def parse_cfg_string(string):
    """
    Parse a comma separated string into a list
    """
    string = string.split(",")
    return [m.strip() for m in string]


def fill_experiment_defaults(experiment, conf, pipeline_task):
    if "subset_name" not in experiment.keys():
        experiment["subset_name"] = conf.get(pipeline_task, 
            "subset_name_default")
    if "subset_file" not in experiment.keys():
        experiment["subset_file"] = conf.get(pipeline_task, 
            "subset_file_default")
    if "features" not in experiment.keys():
        experiment["features"] = conf.get(pipeline_task, 
            "features_default")
    if "preprocessing" not in experiment.keys():
        experiment["preprocessing"] = conf.get(pipeline_task, 
            "preprocessing_default")
    return experiment


def unique_experiment_subsets(experiments_path, conf, pipeline_task):
    """
    Get the unique subsetting files specified by the experiment json
    """
    with open(experiments_path) as experiments_json:
        experiments = json.load(experiments_json)

    subsets = set()
    for _, experiment in experiments.items():

        experiment = fill_experiment_defaults(experiment, conf, pipeline_task)
        subset_tuple = (experiment["subset_name"],
                        experiment["subset_file"])
        subsets.add(subset_tuple)

    return subsets


def hash_name(string, max_chars=32):
    """
    Return the sha1 hash of a string
    """
    hash_obj = hashlib.sha1(string.encode())
    return hash_obj.hexdigest()[:max_chars]


def hash_if_needed(string, min_hash_req=55, max_chars=32):
    """
    Return the hash to a string if it is too long
    We keep the first five characters as a prefix though.
    """
    result_string = string
    if len(string) > min_hash_req:
        vowels = re.compile('[aeiou]', flags=re.I)
        result_string = vowels.sub('', string)
    if len(result_string) > min_hash_req:
        result_string = string[:5] + hash_name(string, max_chars - 5)
    return result_string


def basename_without_extension(file_path):
    """
    Extract the name of a file from it's path
    """
    file_path = os.path.splitext(file_path)[0]
    return os.path.basename(file_path)


def get_csv_names(basename, params_dict):
    """
    Paste names in a string, after extracting basenames and removing extensions
    """
    components = [basename] + list(params_dict.values())
    components = [basename_without_extension(s) for s in components]
    components = [strip_punct(s) for s in components]
    return "-".join(components)


def strip_punct(cur_string):
    """
    Remove punctuation from a string
    """
    exclude = set(string.punctuation)
    exclude.add(' ')
    exclude.remove('_')

    regex = '[' + re.escape(''.join(exclude)) + ']'
    return re.sub(regex, '', cur_string)

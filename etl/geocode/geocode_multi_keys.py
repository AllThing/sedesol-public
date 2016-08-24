"""

 This script geocodes addresses using the list of provided Google geocoding
 keys. Run multiple instances of this script in parallel, varying the
 configuration (config.py) for each run, in order to parallelize and scale up
 the geocoding process. Please see config_sample.py for an example configuration.
 
"""
from __future__ import print_function 
import googlemaps
from google_query import query_location, postprocess_geocode
import os
import psycopg2
import yaml
import logging
import json
import sys
from time import time
sys.path.append(os.getcwd())
# import config file from the directory code is being run
import config
import requests


def clean_json(json_str):
    """ Function to remove double quotes from json.dumps string

    :param json string to clean

    :return cleaned string
    """
    return json_str.replace('\\"',"")


def read_api_keys(FILEPATH):
    """ reads the list of API keys to validate from a file and returns the list
    of unique keys

    :param FILEPATH [string] Path to the file containing the list of API keys
    :return [list] A list of unique API keys
    :rtype: list
    :raises: googlemaps.exceptions.ApiError
    """
    api_keys = list() 
    with open(FILEPATH) as f:
        for line in f:
            api_key = line.strip()
            api_keys.append(api_key)

    return api_keys


def is_key_good(api_key):
    """ Does a quick sample query to see if the API key is invalid or over query limit
    
    :param api_key [string] api key to check
    
    :return True if key valid, else False
    :rtype Boolean
    """
    sample_query = "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=" + api_key
    try:
        res = requests.get(sample_query)
        if res.json()['status'] == "REQUEST_DENIED" or \
           res.json()['status'] == "OVER_QUERY_LIMIT":
            return False
        else:
            return True
    except:
        return False
    return False


def connect_to_database(CONN_YAML_FILEPATH, DB_YAML_FILEPATH):
    """ returns a connection to the database
    
    :param CONN_YAML_FILEPATH [string] Path to a YAML file that contains the
    configuration parameters for establishing a tunneling connection to the database.
    :param DB_YAML_FILEPATH [string] Path to a YAML file that contains the
    database connection configuration parameters.

    :return [connection]
    """
    with open(CONN_YAML_FILEPATH) as f:
        tnconf = yaml.load(f)

    with open(DB_YAML_FILEPATH) as f:
        dbconf = yaml.load(f)
    
    conn = psycopg2.connect(dbname = dbconf["PGDATABASE"], 
                            host = dbconf["PGHOST"], 
                            port = 8427, 
                            user = dbconf["PGUSER"],
                            password = dbconf["PGPASSWORD"])
    
    '''
    conn = psycopg2.connect(dbname = dbconf["PGDATABASE"], 
                            host = tnconf["CONNECTION_HOST"], 
                            port = tnconf["CONNECTION_PORT"], 
                            user = dbconf["PGUSER"],
                            password = dbconf["PGPASSWORD"])
    '''
    conn.autocommit = True
    return conn


def query_database(db_conn, start_index, end_index):
    """ queries the database to get the list of addresses that a given API_KEY
    will then query Google
    
    :param db_conn [connection] DB connection object
    :param start_index [int] starting row number of the addresses to be queried 
    :param end_index [int] ending row number of the addresses to be queried

    :return [list] A list of dictionary of addresses. An address dictionary has
     the keys keys called "id", "entidad_federativa", "municipio", "localidad",
     "localidad", and "asen", corresponding to a search id and various levels
     of granularity in mexican addresses, as well as "vial", "vial_1", and
     "vial_2", giving the names of the main road ("vial") and side streets
     ("vial_1" and "vial_2"). 
    :rtype: list of dictionaries
    """
    cur = db_conn.cursor()  ## open a cursor
    query = "%s %d %s %d;" %("SELECT \
                               address as address_id, \
                               ent as entidad_federativa, \
                               mun as municipio, \
                               loc as localidad, \
                               asen as asen, \
                               num_ext as num_ext, \
                               vial as vial, \
                               vial_1 as vial_1, \
                               vial_2 as vial_2 \
                               FROM raw.addresses \
                               where address between ",
                               start_index,
                               " and ",
                               end_index)

    cur.execute(query)
    result_set = cur.fetchall()
    logging.info("Queried database for addresses to geocode successfully")

    keys = ["id",
            "entidad_federativa",
             "municipio", 
            "localidad",
            "asen",
            "num_ext",
            "vial", 
            "vial_1", 
            "vial_2"]
    addresses_to_query = list()
        
    for row in result_set:
         addresses_to_query.append(dict(zip(keys, row)))    

    return addresses_to_query 


def write_to_database(db_conn, google_query_results, failed_writes_log):
    """ writes the results of google API querying to the database in JSON
    format

    :param db_conn [connection] DB connection object 
    :param google_query_results [list] a list of address_ids and corresponding
    google query result in json format 
    """
    cur = db_conn.cursor()
    # table raw.geocoding_results holds the same id as raw.addresses and stores
    # a json object that contains the geocoding info
    insert_query = "%s %s;" %("INSERT INTO raw.geocoding_results \
                           (address_id, google_query_result) \
                            VALUES", \
                            ",".join([str(x) for x in google_query_results]))
    try:
        cur.execute(insert_query)
        db_conn.commit()
        logging.info("Inserted %d records to database successfully", len(google_query_results))
    except: #(psycopg2.InternalError, psycopg2.ProgrammingError, psycopg2.DataError)
        logging.error("Error inserting query result to the database. \
                      Collecting in log %s", failed_writes_log)
        db_conn.rollback()
        with open(failed_writes_log, "a") as g:
            g.write("%s\n" %(insert_query))

    cur.close()

if __name__ == "__main__":
    """
    main handler code, loops over the API keys, and for each API key first
    fetches a list of addresses from database that should be queried using this
    key (the length of this list corresponds to the QUERIES_PER_API_KEY
    parameter in the config file). It then queries the Google geocoding API
    and stores the results back in database in json format. 
    """     
    START_INDEX = config.START_INDEX
    QUERIES_PER_API_KEY = config.QUERIES_PER_API_KEY 
    PATH_TO_YAML_FILES = config.PATH_TO_YAML_FILES 
    API_KEYS_PATH = config.API_KEYS_PATH 
    FIRST_API_KEY_INDEX = config.FIRST_API_KEY_INDEX 
    LAST_API_KEY_INDEX = config.LAST_API_KEY_INDEX
    END_INDEX = START_INDEX + QUERIES_PER_API_KEY - 1
    KEY_ERR_THRESHOLD = 5 # Threshold for number of errors a key is allowed
                          # after which we switch keys  
    GEOCODING_FAILED = {"query_results":[{"quality":{"geocoding_failed":True}}]}

    log_file_name = "google_querying_%d_%d.%d.log"  %(START_INDEX,
                                                     START_INDEX + (LAST_API_KEY_INDEX - FIRST_API_KEY_INDEX + 1)*QUERIES_PER_API_KEY,
                                                     int(time()))
    failed_writes_log = "failed_writes_%d_%d.%d.log" %(START_INDEX,
                                                       START_INDEX + (LAST_API_KEY_INDEX - FIRST_API_KEY_INDEX + 1)*QUERIES_PER_API_KEY,
                                                       int(time()))

     
    logging.basicConfig(filename = log_file_name, 
                        filemode = 'w', 
                        format = '%(asctime)s %(levelname)s:%(message)s', 
                        level = logging.INFO)

    db_conn = connect_to_database(os.path.join(PATH_TO_YAML_FILES, 
                                               "connection.yaml"), 
                                  os.path.join(PATH_TO_YAML_FILES,
                                               "db_profile.yaml"))

    logging.info("Connected to database successfully")
    api_keys = read_api_keys(API_KEYS_PATH)
    
    for key_no, api_key in enumerate(api_keys[FIRST_API_KEY_INDEX: LAST_API_KEY_INDEX + 1]):
        logging.info("Using key number %d:, %s", key_no, api_key)
        key_err_count = 0
        
        if not is_key_good(api_key):
            logging.error("Skipping unusable key: %s", api_key)
            continue
        
        END_INDEX = START_INDEX + QUERIES_PER_API_KEY - 1
        addresses_to_query = query_database(db_conn, START_INDEX, END_INDEX)
        google_query_results = list()
         
        for address_no, address in enumerate(addresses_to_query):
            if key_err_count > KEY_ERR_THRESHOLD:
                logging.error("Key %s is unusable further. Flagging all remaining addresses in batch as failed geocoding",
                             api_key)
                google_query_results.append((int(address["id"]), json.dumps(GEOCODING_FAILED)))
                continue
            
            try:
                query_result = query_location(address, api_key)
            except googlemaps.exceptions.ApiError:               
                logging.error("Querying error")
                google_query_results.append((int(address["id"]), json.dumps(GEOCODING_FAILED)))
                key_err_count += 1
            except googlemaps.exceptions.Timeout:
                logging.error("Timeout error")
                google_query_results.append((int(address["id"]), json.dumps(GEOCODING_FAILED)))
                key_err_count += 1
            except googlemaps.exceptions.TransportError:
                logging.error("Transport error")
                google_query_results.append((int(address["id"]), json.dumps(GEOCODING_FAILED)))
                key_err_count += 1
            except:
                logging.error("Unexpected error")
                google_query_results.append((int(address["id"]), json.dumps(GEOCODING_FAILED)))
            else:
                logging.info("Queried Google API successfully for address: %d", address_no)
                # postprocess_geocode returns a list containing the search id of
                # the address queried and a json of the google query result 
                google_query_results.append((int(address["id"]),
                                             clean_json(json.dumps(query_result,
                                                                  allow_nan=False))))
       
        write_to_database(db_conn, google_query_results, failed_writes_log)
        START_INDEX = END_INDEX + 1
    
    db_conn.close()
    logging.info("Closed database connection")
    logging.info("Done with querying. Last address_id queried: %d", END_INDEX)
    

# the address_id in the database from which to start querying.
# this is useful for splitting up querying workload across various runs
# and for resuming querying in case of crash. 

START_INDEX = 1

# number of API requests to make per API key. Google has a maximum b 

QUERIES_PER_API_KEY = 2499

# path from where the db_profile.yaml and connection_profile.yaml YAML files
# should be read. (Example YAML files are in the conf folder at the root
# directory of the repo).

PATH_TO_YAML_FILES = "./"

# path to a file containing the Google geocoding API keys. The keys should be
# separated by newlines

API_KEYS_PATH = "validated_API_keys"

# the first and last index of the keys to use from the API keys file.

FIRST_API_KEY_INDEX = 1 
LAST_API_KEY_INDEX = 20

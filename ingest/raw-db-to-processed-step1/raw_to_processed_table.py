import os
import sys
import yaml
import psycopg2

base_path = os.path.join("/mnt", "data", "sedesol", "db_creation","create_raw_to_processed")

conf = {}
with open("../../conf/db_profile.yaml", "r") as f:
    db_profile = yaml.load(f)
    conf["HOST"] = db_profile["PGHOST"]
    conf["USER"] = db_profile["PGUSER"]
    conf["PASSWORD"] = db_profile["PGPASSWORD"]
    conf["DATABASE"] = db_profile["PGDATABASE"]

with open("tables_columns_from_raw_to_processed_all.yaml","r") as f:
    all_possible_tables = yaml.load(f)
    conf["ALLTABLES"] = all_possible_tables

def check_if_table_raw_to_processed_map_exists_and_create_if_not():
    """ Check if raw_to_processed_map table exists in raw schema, create it if not and return db connection
    :param None
    :return dbconnection 
    :side-effects: Creates
    :rtype psycopg2 cursor """
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='raw' AND table_type='BASE TABLE' AND table_name = 'raw_to_processed_map'")
    res = cur.fetchone()
    if len(res) == 1:
         print "\traw_to_processed_map table found!"
    else:
         print "\traw_to_processed_map table not found!  create table in raw schema"    
         cur.execute("CREATE TABLE raw.raw_to_processed_map ( table_name VARCHAR(200), column_name VARCHAR(200), description varchar, included_in_processed integer, new_schema_name varchar, date_created timestamp DEFAULT current_timestamp )")
         conn.commit()
         print "\t raw_to_processed_map created."
         
         
    return conn

def connect_to_db():
    """ This function connects to the db based on the settings in db_profile.yaml
    :param None
    :return dbconnection 
    :rtype psycopg2 dbconnection"""
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (conf["HOST"],conf["DATABASE"],conf["USER"], conf["PASSWORD"])
    conn = psycopg2.connect(conn_string)
    return conn

def generate_table_columns(conn,table_name,columns_array,new_schema_name):
    """ Given a table_name and columns within the table, set those columns to be "included" in the processed db
    :param psycopg2 dbcon: psycopg2 db connection
    :param string table_name: The name of the table to process
    :param [string] columns_array:  An array of column names contained in the table which we want to include in the processed db
    :return None
    :side-effects: Creates rows for each column in table_name in the table raw_to_processed_map and sets the included value to 1 for those included in the columns_array
    :rtype None """
    cur = conn.cursor()
    default_included_val = 0;
    if columns_array == "*":
        default_included_val = 1;

    #this query creates the sql inserts to place the current tables columns into the raw_to_processed_map table
    sql_to_generate_inserts_needed_for_table = "SELECT CONCAT('INSERT INTO raw.raw_to_processed_map SELECT ''',table_name,''' as table_name, ''',column_name,''' as column_name,', '''desc'', %s, ''%s'';') FROM information_schema.columns WHERE table_schema='raw' AND table_name='%s' order by column_name;" % (default_included_val, new_schema_name, table_name)

    cur.execute(sql_to_generate_inserts_needed_for_table)
    insert_statements = cur.fetchall()
    for insert_query in insert_statements:
        cur.execute(insert_query[0])
        conn.commit() 

    #add updates to set columns_array columns to 1
    if default_included_val == 0 and columns_array != '':
        update_columns_query = "UPDATE raw.raw_to_processed_map SET included_in_processed = 1 WHERE table_name='%s' AND column_name in (%s)" % ( table_name, ", ".join(["'"+ a + "'" for a in columns_array.split(" ")]))
        cur.execute(update_columns_query)
        conn.commit() 


def load_table_columns_yaml(tables_columns_yaml_file):
    """Load a yaml file which is a list of tables each with a list of column names that we want to include in our processing step from the raw db to the processed one 
    :param string tables_columns_yaml_file:  File name of the yaml input file to process
    :return dictionary of tables with each containing a list of columns to include
    :rtype dict """
    with open(tables_columns_yaml_file,"r") as f:
        tables_columns_dict = yaml.load(f)

    #if all tables aren't represented fill in dict with table names from conf['ALLTABLES'] and no columns for them ( this is better for book keeping )
    sorted_tables_to_process = sorted(tables_columns_dict.keys())
    sorted_all_possible_tables = sorted(conf['ALLTABLES'].keys())
    if sorted_tables_to_process != sorted_all_possible_tables:
        print "List of tables in conf file not identical to all possible tables so add missing tables with no columns included to process"
        for table in sorted_all_possible_tables:
            if table not in sorted_tables_to_process:
               tables_columns_dict[table] = '' 

    return tables_columns_dict
    

def create_and_populate_raw_to_processed_map_table(tables_columns_yaml_file,new_schema_name):
    """Create the raw_to_processed_map table in the raw schema given a configuration file specifying which tables and columns to be included 
    :param string tables_columns_yaml_file:  File name of the yaml input file to process
    :return None
    :rtype None """
    print "Loading yaml conf"
    tables_columns_dict = load_table_columns_yaml(tables_columns_yaml_file)
    
    print "Check if table already exists"
    conn = check_if_table_raw_to_processed_map_exists_and_create_if_not()

    for table in tables_columns_dict:
        print "Processing table: ",table,"\n\tinclude columns: ",tables_columns_dict[table]
        generate_table_columns(conn,table,tables_columns_dict[table],new_schema_name)

    #generate nice output
    cur = conn.cursor()
    cur.execute("SELECT Count(*) from raw.raw_to_processed_map Where included_in_processed = 1;")
    print "Total number of tables processed: %s" % len(tables_columns_dict)
    print "Total number of columns: %s" % cur.fetchone()[0]


def create_new_schema_and_dbs_from_vals_in_raw_to_processed_map_table(new_schema_name):
    conn = connect_to_db()

    #create schema name and then copy of
    cur = conn.cursor()
    create_schema_sql = "CREATE SCHEMA %s" % new_schema_name
    cur.execute(create_schema_sql)
    conn.commit()

    #go through raw_to_processed map and get all table/columns to copy over to new schema
    cur.execute("SELECT table_name,column_name FROM raw.raw_to_processed_map WHERE included_in_processed = 1 ORDER BY table_name,column_name;")
    results = cur.fetchall()
    current_table = ''
    current_columns = '';

    for row in results:
        row_table_name = row[0]
        row_column_name = row[1]

        if row_table_name != current_table:
            if current_columns != '':
                new_table_in_new_schema_sql = "SELECT %s INTO %s.%s FROM raw.%s" % ( current_columns[:-1], new_schema_name, current_table, current_table)
                cur.execute(new_table_in_new_schema_sql)

            current_columns = ''
            current_table = row_table_name

        #add current_column to list
        current_columns += row_column_name + ','
    
    if current_columns != '':
        new_table_in_new_schema_sql = "SELECT %s INTO %s.%s FROM raw.%s" % ( current_columns[:-1], new_schema_name, current_table, current_table)
        cur.execute(new_table_in_new_schema_sql)

    conn.commit()
    

if __name__ == "__main__":
    #load yaml conf of tables and columns to include
    if len(sys.argv) == 1:
        print("Input error: expecting yaml_file of tables and columns to include")
    else:
        tables_columns_yaml_file = sys.argv[1]
        print "Processing file %s" % tables_columns_yaml_file
        new_schema_name = ''
        if len(sys.argv) == 3:
            new_schema_name = sys.argv[2]

        create_and_populate_raw_to_processed_map_table(tables_columns_yaml_file,new_schema_name)

        if new_schema_name != '':
            print "Creating new schema named: %s  with tables and columns provided" % new_schema_name
            create_new_schema_and_dbs_from_vals_in_raw_to_processed_map_table(new_schema_name)

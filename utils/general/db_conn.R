
#' @title Return a dplyr connection to a postgres database
#'
#' @description This function wraps some of the boilerplate associated
#' with logging into a postgres database. The main steps are
#'   (1) extract credentials from appropriately formatted YAML files
#'   (2) set up a tunnel to the server
#'   (3) connect to the actual postgres database
#' @param profile_yaml [character] The path to a YAML file describing
#'   how to log-in to the postgres database. See
#'   example_db_profile.yaml for an example.
#' @param connection_yaml [character] The path to a YAML file
#'   describing how to set up the tunnel. See
#'   example_connection_profile.yaml for an example.
#' @return A dplyr src_postgres object allowing the user to query the
#'   database without completely loading it into memory.
db_conn <- function(profile_yaml, connection_yaml) {
  db_info <- yaml.load_file(profile_yaml)
  conn_info <- yaml.load_file(connection_yaml)

  tunnel <- sprintf(
    "ssh -fNT -L %s:%s:%s -i %s %s@%s",
    db_info$PGPORT,
    conn_info$CONNECTION_HOST,
    conn_info$CONNECTION_PORT,
    conn_info$KEY,
    conn_info$TUNNEL_USER,
    conn_info$TUNNEL_HOST
  )
  
  system(tunnel)
  src_postgres(dbname = db_info$PGDATABASE,
               host = db_info$PGHOST,
               user = db_info$PGUSER,
               password = db_info$PGPASSWORD,
               port = db_info$PGPORT,
               options="-c search_path=raw")
}
  

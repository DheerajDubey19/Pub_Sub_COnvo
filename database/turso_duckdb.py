import os
import duckdb
import logging
import libsql_experimental as libsql
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Step 1: Connect to TursoDB (SQLite)
url = os.getenv("TURSO_DATABASE_URL")
auth_token = os.getenv("TURSO_AUTH_TOKEN")

def connect_to_turso():
    conn = libsql.connect("users.db", sync_url=url, auth_token=auth_token)
    conn.sync()
    logger.info("Successfully connected to the TursoDB database")
    return conn

def add_user(conn, name, age, address):
    query = "INSERT INTO users (name, age, address) VALUES (?, ?, ?)"
    conn.execute(query, (name, age, address))
    conn.commit()
    logger.info(f"Added new user: {name}")

def delete_user(conn, user_id):
    query = "DELETE FROM users WHERE ID = ?"
    result = conn.execute(query, (user_id,))
    conn.commit()
    if result.rows_affected == 0:
        logger.warning(f"No user found with ID: {user_id}")
    else:
        logger.info(f"Deleted user with ID: {user_id}")

def fetch_all_users(conn):
    query = "SELECT * FROM users"
    return conn.execute(query).fetchall()

def main():
    turso_conn = None
    duck_conn = None
    try:
        # Connect to TursoDB
        turso_conn = connect_to_turso()

        # Connect to DuckDB
        duck_conn = duckdb.connect()
        logger.info("Successfully connected to DuckDB")

        # Add a new user
        # add_user(turso_conn, "Jane Smith", 30, "456 Elm St")

        # Delete a user (assuming ID 1 exists)
        # delete_user(turso_conn, 1)

        # Fetch all users
        data = fetch_all_users(turso_conn)
        if not data:
            logger.warning("No data retrieved from TursoDB")
        else:
            logger.info(f"Retrieved {len(data)} rows from TursoDB")

        columns = [desc[0] for desc in turso_conn.execute("SELECT * FROM users").description]
        logger.info(f"Columns: {columns}")

        # Convert to DataFrame
        df = pd.DataFrame(data, columns=columns)
        logger.info(f"Created DataFrame with shape: {df.shape}")
        
        # Print out the DataFrame contents
        logger.info("DataFrame contents:")
        logger.info(df.to_string())

        # Create a table in DuckDB from the pandas DataFrame
        duck_conn.register('df_view', df)
        duck_conn.execute("CREATE OR REPLACE TABLE users AS SELECT * FROM df_view")
        logger.info("Created 'users' table in DuckDB")

        # Query the data within DuckDB
        duck_results = duck_conn.execute("SELECT * FROM users WHERE age > 20").fetchall()
        logger.info(f"Retrieved {len(duck_results)} rows from DuckDB query")

        # Display results
        logger.info("DuckDB query results:")
        for row in duck_results:
            logger.info(f"Row: {row}")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

    finally:
        # Clean up and close connections
        logger.info("Finishing up and closing connections")
        if duck_conn:
            duck_conn.close()
            logger.info("DuckDB connection closed")
        if turso_conn:
            # TursoDB connection doesn't have a close method, so we'll just log it
            logger.info("Finished using TursoDB connection")

    logger.info("Script execution completed.")

if __name__ == "__main__":
    main()
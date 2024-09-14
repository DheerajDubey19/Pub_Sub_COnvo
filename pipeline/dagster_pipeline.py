import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dagster import job, op, repository
from database.tursodb import insert_user, get_connection,delete_user

# Configure logging
logging.basicConfig(level=logging.INFO)

@op
def add_users_to_db(_):
    users = [
        {"name": "Amyth", "age": 20, "address": "NES"},
    ]
    for user in users:
        try:
            insert_user(user['name'], user['age'], user['address'])
            logging.info(f"User {user['name']} added successfully.")
        except Exception as e:
            logging.error(f"Error adding user {user['name']}: {e}")

# @op
# def update_user_in_db(_):
#     try:
#         update_user("Amyth", {"age": 25, "address": "HSR Layout"})
#         logging.info("User Amyth updated successfully.")
#     except Exception as e:
#         logging.error(f"Error updating user Amyth: {e}")

@op
def delete_user_from_db(_):
    try:
        delete_user("Amyth")
        logging.info("User Amyth deleted successfully.")
    except Exception as e:
        logging.error(f"Error deleting user Amyth: {e}")

@op
def query_users_from_db(_):
    try:
        conn = get_connection()
        results = conn.execute("SELECT * FROM users").fetchall()
        for row in results:
            logging.info(row)
    except Exception as e:
        logging.error(f"Error querying users: {e}")

@job
def final_pipeline():
    add_users_to_db()
    # update_user_in_db()
    delete_user_from_db()
    query_users_from_db()

@repository
def final_repository():
    return [final_pipeline]

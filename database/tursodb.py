import os
import libsql_experimental as libsql
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    try:
        conn = libsql.connect("users.db", sync_url=url, auth_token=auth_token)
        conn.sync()
        logger.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

def insert_user(name, age, address):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO users (name, age, address) VALUES (?, ?, ?)", (name, age, address))
        conn.commit()
        logger.info(f"Successfully inserted user: {name}, {age}, {address}")
    except Exception as e:
        logger.error(f"Error inserting user: {e}")
        conn.rollback()

def update_user(name, updated_data):
    conn = get_connection()
    try:
        set_clause = ", ".join([f"{key} = ?" for key in updated_data.keys()])
        values = list(updated_data.values()) + [name]
        query = f"UPDATE users SET {set_clause} WHERE name = ?"
        logger.debug(f"Executing query: {query} with values: {values}")
        conn.execute(query, tuple(values))
        conn.commit()
        logger.info(f"Successfully updated user: {name}")
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        conn.rollback()


def delete_user(name):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM users WHERE name = ?", (name,))
        conn.commit()
        logger.info(f"Successfully deleted user: {name}")
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        conn.rollback()

def get_users_from_db(limit=10):
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT id, name, age, address FROM users LIMIT ?", (limit,))
        users = [{"id": row[0], "name": row[1], "age": row[2], "address": row[3]} for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(users)} users from the database")
        print(users)
        return users
    except Exception as e:
        logger.error(f"Error fetching users from database: {e}")
        return []

def initialize_database():
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                address TEXT
            );
        """)
        conn.commit()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        conn.rollback()

def send_notification():
    try:
        logger.info("Sending notification to admin.")
        # Simulate sending notification
    except Exception as e:
        logger.error(f"Error sending notification: {e}")

def handle_transaction():
    try:
        logger.info("Handling transaction.")
        # Simulate transaction handling
    except Exception as e:
        logger.error(f"Error handling transaction: {e}")

if __name__ == "__main__":
    initialize_database()

    # Test inserting a user
    # insert_user("Aditya", 21, "Hebbal")

    # Test updating a user
    # update_user("Jane Smith", {"age": 24, "address": "HSR Layout"})

    # Test deleting a user
    # delete_user("Venkat Dheeraj")

    # Test retrieving users
    get_users_from_db()

    # Test sending notification
    send_notification()

    # Test handling transaction
    handle_transaction()




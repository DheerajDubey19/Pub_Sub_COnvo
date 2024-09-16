import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dagster import op, job, repository, Out, Output
from database.tursodb import insert_user, get_connection, delete_user, update_user
import modal

# Configure logging
logging.basicConfig(level=logging.INFO)

app = modal.App(
    image=modal.Image.debian_slim().pip_install("dagster", "libsql_experimental")
)

@app.function()
@modal.batched(max_batch_size=100, wait_ms=1000)
def process_batch(batch):
    results = []
    for row in batch:
        # Perform operations on the row
        print(row)
        results.append(row)  # Append the processed row to results
    return results  # Return the list of processed rows

@op
def add_users_to_db(_):
    users = [
        {"name": "Himanchal", "age": 20, "address": "Raipur"},
    ]
    for user in users:
        try:
            insert_user(user['name'], user['age'], user['address'])
            logging.info(f"User {user['name']} added successfully.")
        except Exception as e:
            logging.error(f"Error adding user {user['name']}: {e}")

@op
def update_user_in_db(_):
    # Define the user to update and the new data
    name = "Jane Smith"
    updated_data = {"age": 50, "address": "BTM Layout"}

    try:
        update_user(name, updated_data)
        logging.info(f"User {name} updated successfully.")
    except Exception as e:
        logging.error(f"Error updating user {name}: {e}")

@op
def delete_user_from_db(_):
    try:
        delete_user("Aditya")
        logging.info("User Aditya deleted successfully.")
    except Exception as e:
        logging.error(f"Error deleting user Harsh: {e}")

@op(out=Out(list))
def query_users_from_db(_):
    try:
        conn = get_connection()
        results = conn.execute("SELECT * FROM users").fetchall()

        # Create batches of a suitable size (e.g., 100)
        batch_size = 100
        batched_results = [results[i:i+batch_size] for i in range(0, len(results), batch_size)]

        return Output(batched_results)
    except Exception as e:
        logging.error(f"Error querying users: {e}")

@op
def process_batches(batched_results):
    # Create a list to store deferred tasks
    deferred_tasks = []

    for batch in batched_results:
        # Process each batch asynchronously using Modal Labs
        with app.run():
            batch_tasks = process_batch._call_function([batch], {})
            if isinstance(batch_tasks, tuple):
                batch_tasks = list(batch_tasks)  # Convert tuple to list if necessary
            deferred_tasks.extend(batch_tasks)  # Append all tasks to deferred_tasks

    # Wait for all deferred tasks to complete
    for task in deferred_tasks:
        if hasattr(task, 'result'):
            task.result()

@job
def final_pipeline():
    # add_users_to_db()
    # update_user_in_db()
    delete_user_from_db()
    batched_results = query_users_from_db()
    process_batches(batched_results)

@repository
def final_repository():
    return [final_pipeline]

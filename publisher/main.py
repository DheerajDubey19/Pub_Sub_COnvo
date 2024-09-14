import json
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dapr.clients import DaprClient
from pipeline.dagster_modal import add_users_to_db, final_pipeline, delete_user_from_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn")

# Initialize FastAPI app
app = FastAPI()

def publish_message(message: str, topic_name: str):
    payload = {"message": message}
    logger.info(f"Publishing to {topic_name} with payload: {payload}")
    
    with DaprClient() as client:
        client.publish_event(
            pubsub_name="pubsub",
            topic_name=topic_name,
            data=json.dumps(payload),
        )
    logger.info(f"Publish result: Success")
    
    return {"status": "Message published", "message": message}

class User(BaseModel):
    name: str
    age: int
    address: str

@app.post("/insert_user")
async def insert_user_endpoint(user: User):
    try:
        # Use the Dagster op to add the user
        add_users_to_db()  # This will add the hardcoded user
        
        message = f"User added: {user.name} with age {user.age} and {user.address}"
        result = publish_message(message, "create-user")
        
        # Run the Dagster job to process users
        final_pipeline.execute_in_process()
        
        return result
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")

@app.post("/delete_user")
async def delete_user_endpoint():
    try:
        # Use the Dagster op to delete the user
        delete_user_from_db()
        
        message = "User deleted"
        result = publish_message(message, "delete-user")
        
        return result
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete user")

@app.post("/process_users")
async def process_users():
    try:
        # Run the Dagster job to query and process users
        result = final_pipeline.execute_in_process()
        
        if result.success:
            return {"status": "Users processed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process users")
    except Exception as e:
        logger.error(f"Error processing users: {e}")
        raise HTTPException(status_code=500, detail="Failed to process users")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

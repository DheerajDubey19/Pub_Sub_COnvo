from fastapi import FastAPI, Request
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.post("/subscribe/")
async def subscribe_message(request: Request):
    body = await request.json()
    data = body.get("data")
    if data:
        try:
            # Parse the data string into a dictionary
            data_dict = json.loads(data)
            message = data_dict.get("message")
            topic = body.get("topic")
            if message is None or topic is None:
                logger.warning(
                    "Received message with missing topic or message")
            else:
                logger.info(f"Received message on {topic} topic: {message}")
                if topic == "create-user":
                    logger.info(f"Processing create-user message: {message}")
                elif topic == "get-users":
                    logger.info(f"Processing get-users message: {message}")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON data")
    else:
        logger.warning("Received message with missing data field")
    return {} 


@app.get("/dapr/subscribe")
async def dapr_subscribe():
    return [
        {"pubsubname": "pubsub", "topic": "create-user", "route": "/subscribe/"},
        {"pubsubname": "pubsub", "topic": "get-users", "route": "/subscribe/"}
    ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
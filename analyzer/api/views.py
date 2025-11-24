from fastapi import FastAPI
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import json
import os
import asyncio
from loguru import logger

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
TOPIC_NAME = 'sentiment_tasks'

class PredictRequest(BaseModel):
    text: str

class PredictResponse(BaseModel):
    status: str
    message: str

producer = None
@app.on_event("startup")
async def startup_event():
    global producer
    retry_count = 0
    max_retries = 25
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to Kafka ({retry_count + 1}/{max_retries})...")
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            await producer.start()
            logger.info("Producer connected successfully!")
            return
        except Exception as e:
            logger.warning(f"Connection failed: {e}")
            retry_count += 1
            await asyncio.sleep(5)
            
    raise Exception("Could not connect to Kafka after several retries")

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()
        logger.info("Kafka is down")

@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    message = json.dumps({"text": request.text}).encode("utf-8")

    await producer.send_and_wait(TOPIC_NAME, message)
    return {"status": "queued", "message": "Task sent to workers"}

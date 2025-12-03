from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from loguru import logger
from uuid import uuid4
import asyncio
import redis
import json
import os

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
TOPIC_NAME = 'sentiment_tasks'

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
num_partitions = 10 # так же, как и в compose файле
producer = None

class PredictRequest(BaseModel):
    text: str

class PredictResponse(BaseModel):
    task_id: str
    status: str

task_counter = 0

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
    global task_counter
    task_id = str(uuid4())
    redis_client.set(task_id, 'processing')

    message = json.dumps({"task_id": task_id, "text": request.text}).encode("utf-8")
    partition_id = task_counter % num_partitions
    task_counter += 1

    await producer.send_and_wait(TOPIC_NAME, message, partition=partition_id)
    logger.info(f'Send task {task_counter - 1} to partition {partition_id}')

    return PredictResponse(task_id=task_id, status='queued')

@app.get("task/{task_id}", response_model=PredictResponse)
async def get_result(task_id: str):
    status = redis_client.get(task_id)

    if not status:
        raise HTTPException(status_code=404, detail='Task is not found')
    
    return PredictResponse(task_id=task_id, status=status)

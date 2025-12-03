from aiokafka import AIOKafkaConsumer
from textblob import TextBlob
from loguru import logger
import asyncio
import socket
import redis
import json
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
TOPIC_NAME = 'sentiment_tasks'
GROUP_ID = os.getenv('GROUP_ID', 'my_group')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
WORKER_ID = socket.gethostname()
redis_client = redis.Redis(REDIS_HOST, port=6379, db=0, decode_responses=True)

async def consume():
    logger.info(f"Worker {WORKER_ID} starting...")
    
    consumer = None
    retry_count = 0
    max_retries = 25

    while retry_count < max_retries:
        try:
            logger.info(f"Worker {WORKER_ID} connecting to Kafka ({retry_count + 1}/{max_retries})...")
            
            consumer = AIOKafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_interval_ms=300000
            )

            await consumer.start()
            logger.info(f"Worker {WORKER_ID} connected!")
            break

        except Exception as e:
            logger.warning(f"Worker connection failed: {e}")
            retry_count += 1
            await asyncio.sleep(5)
    
    if not consumer:
        logger.critical("Critical: Could not connect to Kafka.")
        return
    
    try:
        logger.info(f"Consumer with id {WORKER_ID} ready")

        async for message in consumer:
            data = json.loads(message.value.decode('utf-8'))
            text = data.get('text', '')
            task_id = data.get('task_id')

            # мы имитируем долгую работу сервиса
            await asyncio.sleep(10)

            blob = TextBlob(text)
            sentiment = blob.sentiment.polarity
            result = "Happy happy happy" if sentiment > 0 else "Dead inside" if sentiment < 0 else "Niche tak"

            if task_id:
                redis_client.set(task_id, result)
            
            await consumer.commit()
            logger.info(f"SERVICE_TAG:WORKER_DONE:{WORKER_ID}")
        
    except Exception as e:
        logger.exception(f"Беда приключилась: {e}")
        asyncio.sleep(5)
    
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())

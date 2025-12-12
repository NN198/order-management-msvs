# producer.py
import asyncio
import json
import os
import time
import uuid
import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUE_NAME = "test_queue"

async def publish_message(body: dict):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(QUEUE_NAME, durable=True)
        message = aio_pika.Message(body=json.dumps(body).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
        print("[producer] Published:", body)

async def main():
    for i in range(5):
        msg = {
            "id": str(uuid.uuid4()),
            "seq": i + 1,
            "msg": f"hello {i+1}",
            "ts": time.time()
        }
        await publish_message(msg)
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())

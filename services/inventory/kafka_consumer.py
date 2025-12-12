# consumer.py
import asyncio
import json
import os
import aio_pika
from aio_pika import IncomingMessage

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE_NAME = "test_queue"

async def on_message(message: IncomingMessage):
    async with message.process():
        body = message.body.decode()
        try:
            payload = json.loads(body)
        except Exception:
            payload = body
        print("[consumer] Received:", payload)

async def main():
    print("[consumer] Connecting to", RABBITMQ_URL)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        # make sure queue exists
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.consume(on_message)
        print(f"[consumer] Waiting for messages on queue '{QUEUE_NAME}'. CTRL+C to quit.")
        # keep the consumer running
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")

import asyncio
import json
import aio_pika
import logging

logger = logging.getLogger(__name__)

LATEST_MARKET_DATA = {}

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"

async def consume_market_data():
    while True:
        try:
            logger.info("Attempting RabbitMQ connection...")

            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()

            exchange = await channel.declare_exchange(
                "market",
                aio_pika.ExchangeType.TOPIC
            )

            queue = await channel.declare_queue("", exclusive=True)
            await queue.bind(exchange, routing_key="market.prices")

            logger.info("RabbitMQ connected. Consuming messages.")

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        payload = json.loads(message.body)
                        symbol = payload["symbol"]
                        prices = payload["prices"]

                        LATEST_MARKET_DATA[symbol] = prices

        except Exception as e:
            logger.warning(f"Consumer error: {e}")
            logger.info("Retrying in 5 seconds...")
            await asyncio.sleep(5)

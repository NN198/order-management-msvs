import os
import json
import uuid
import asyncio
import logging
from fastapi import FastAPI
from pydantic import BaseModel
import aio_pika

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@rabbitmq:5672/"
)

app = FastAPI(title="Order Service")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Request model
# -------------------------
class Order(BaseModel):
    product_id: int
    quantity: int
    user_id: str
    amount: float
    payment_method: str

# -------------------------
# RabbitMQ publisher
# -------------------------
async def publish_event(message: dict, routing_key: str):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "events",
            aio_pika.ExchangeType.TOPIC
        )
        await exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=routing_key
        )


# -------------------------
# API endpoint
# -------------------------
@app.post("/orders/")
async def create_order(req: Order):
    order_id = str(uuid.uuid4())

    order = {
        "order_id": order_id,
        "product_id": req.product_id,
        "quantity": req.quantity,
        "user_id": req.user_id,
        "amount": req.amount,
        "payment_method": req.payment_method,
        "status": "created"
    }

    logger.info(f"Order created: {order}")

    asyncio.create_task(
        publish_event(order, routing_key="order.created")
    )

    logger.info(f"Order event published to RabbitMQ: {order_id}")

    return {"order_id": order_id, "status": "created"}

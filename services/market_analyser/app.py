import os
import json
import asyncio
import aio_pika
import pandas as pd
import numpy as np
from scipy.stats import zscore
from collections import deque
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
WINDOW = 15  # rolling window size

price_buffer = deque(maxlen=WINDOW)

def analyse(prices: list[float]):
    series = pd.Series(prices)
    returns = series.pct_change().dropna()

    if len(returns) < 5:
        return None

    z = zscore(returns)[-1]
    vol = returns.std()

    regime = "stress" if abs(z) > 2 else "normal"
    vol_level = "high" if vol > 0.015 else "medium" if vol > 0.005 else "low"

    return {
        "volatility": vol,
        "volatility_level": vol_level,
        "return_zscore": float(z),
        "regime": regime
    }

async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange("events", aio_pika.ExchangeType.TOPIC)
    queue = await channel.declare_queue("market.raw.queue", durable=True)
    await queue.bind(exchange, routing_key="market.price.raw")

    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process():
                event = json.loads(message.body)
                price_buffer.append(event["price"])

                analysis = analyse(list(price_buffer))
                if not analysis:
                    continue

                analysis_event = {
                    "event_type": "market.analysis",
                    "symbol": event["symbol"],
                    "price": event["price"],
                    "analysis": analysis,
                    "timestamp": event["timestamp"]
                }

                print("Market analysis:", analysis_event)

if __name__ == "__main__":
    asyncio.run(main())

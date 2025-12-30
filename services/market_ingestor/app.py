import os
import json
import uuid
import asyncio
import aiohttp
import aio_pika
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
SYMBOL = os.getenv("MARKET_SYMBOL", "TSLA")
INTERVAL = 60  # seconds

ALPHA_URL = (
    "https://www.alphavantage.co/query"
    "?function=TIME_SERIES_INTRADAY"
    "&symbol={IBM}"
    "&interval=1min"
    "&apikey={apikey}"
)

async def fetch_price(session):
    async with session.get(ALPHA_URL.format(symbol=SYMBOL, apikey=API_KEY), timeout=10) as resp:
        data = await resp.json()
        series = data.get("Time Series (1min)", {})

        if not series:
            raise RuntimeError("No market data returned")

        latest_ts = sorted(series.keys())[-1]
        price = float(series[latest_ts]["4. close"])
        return latest_ts, price

async def publish_event(event):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("events", aio_pika.ExchangeType.TOPIC)
        await exchange.publish(
            aio_pika.Message(body=json.dumps(event).encode()),
            routing_key="market.price.raw"
        )

async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ts, price = await fetch_price(session)

                event = {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "market.price.raw",
                    "symbol": SYMBOL,
                    "price": price,
                    "timestamp": ts,
                    "source": "alpha_vantage"
                }

                await publish_event(event)
                print("Published raw price:", event)

            except Exception as e:
                print("Market ingestion failed:", e)

            await asyncio.sleep(INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())

# from fastapi import FastAPI
# from agents.market_analyst import analyze_market
# from schemas import MarketInsight

# app = FastAPI(title="Market Insight Service")

# @app.get("/insights/market", response_model=MarketInsight)
# def get_market_insight():
#     # Step 1: retrieve documents via RAG (stubbed for now)
#     retrieved_events = []

#     # Step 2: agent analysis
#     insight = analyze_market(retrieved_events)

#     return insight

from fastapi import FastAPI
import asyncio
from agents.market_analyst import analyze_market
from consumers.market_consumer import LATEST_MARKET_DATA
from consumers.market_consumer import consume_market_data

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug")
def debug():
    return {"routes": [route.path for route in app.routes]}


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_market_data())

@app.get("/debug/cache")
def debug_cache():
    return LATEST_MARKET_DATA


@app.get("/insights/market")
def market_insight(symbol: str = "RELIANCE.BSE"):
    prices = LATEST_MARKET_DATA.get(symbol)

    if not prices:
        return {"status": "waiting for market data"}

    return analyze_market(symbol, prices)

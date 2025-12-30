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
from agents.market_analyst import analyze_market

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug")
def debug():
    return {"routes": [route.path for route in app.routes]}


@app.get("/insights/market")
def market_insight():
    """
    Temporary stub: we will replace this with real data ingestion next
    """

    # Temporary sample price history (will come from market_ingestor later)
    prices = [
        242.1, 242.5, 243.0, 242.9,
        243.4, 244.1, 244.8, 245.0, 246.2
    ]

    insight = analyze_market("RELIANCE.BSE", prices)
    return insight

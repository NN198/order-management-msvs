import pandas as pd
import numpy as np
from scipy.stats import zscore
from schemas import MarketInsight

# -------------------------
# Core analytics helpers
# -------------------------

def compute_returns(prices: pd.Series):
    return prices.pct_change().dropna()

def compute_volatility(returns: pd.Series, window=10):
    return returns.rolling(window).std()

def detect_regime(latest_return, z_thresh=2.0):
    if abs(latest_return) > z_thresh:
        return "stress"
    return "normal"

def classify_volatility(vol):
    if vol < 0.005:
        return "low"
    elif vol < 0.015:
        return "medium"
    return "high"

# -------------------------
# Market Analyst Agent
# -------------------------

def analyze_market(asset: str, price_history: list[float]) -> MarketInsight:
    """
    price_history: ordered list of recent prices (old → new)
    """

    prices = pd.Series(price_history)
    returns = compute_returns(prices)

    latest_return = returns.iloc[-1]
    return_z = zscore(returns)[-1]

    volatility_series = compute_volatility(returns)
    latest_vol = volatility_series.iloc[-1]

    regime = detect_regime(return_z)
    vol_level = classify_volatility(latest_vol)

    signal_strength = float(abs(return_z))
    confidence = min(0.95, 0.5 + signal_strength / 5)

    supporting_factors = [
        f"Latest return z-score: {return_z:.2f}",
        f"Rolling volatility: {latest_vol:.4f}",
        f"Volatility classified as {vol_level}"
    ]
    assessment = (
    "high short-term volatility"
    if regime == "stress"
    else "normal market conditions"
    )

    return MarketInsight(
         asset=asset,
        assessment=assessment,        # ✅ ADD THIS
        confidence=confidence,
        supporting_factors=supporting_factors
    )

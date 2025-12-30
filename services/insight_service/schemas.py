from pydantic import BaseModel
from typing import List

class MarketInsight(BaseModel):
    asset: str
    assessment: str
    confidence: float
    supporting_factors: List[str]

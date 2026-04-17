"""
AI Brain Client
==============
Connects to Groq API for Llama 3 70B processing
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class GroqClient:
    """Client for Groq API (Llama 3)."""

    BASE_URL = "https://api.groq.com/openai/v1/chat/completions"

    # Default system prompt for financial analysis
    SYSTEM_PROMPT = """You are a senior financial analyst with 20 years of experience at Goldman Sachs.
Your specialty is detecting signals in market data that retail investors miss.

For each piece of financial data I provide, respond with a JSON object containing:
{
    "importance_score": 1-10,
    "category": "Macro" | "Equity" | "Commodity" | "Crypto" | "Geopolitical" | "IPO" | "ICO",
    "summary": "One-line summary of the signal",
    "reason": "Why this scored what it did",
    "cross_signals": ["Any related signals in other asset classes"]
}

Scoring guidelines:
- 9-10: Major institutional buying (C-suite >$1M), Fed policy shift, geopolitical crisis
- 7-8: Large insider buys, unusual options flow, dark pool prints, rate changes
- 5-6: Regular filings, routine data
- 1-4: Noise, routine updates with no actionable signal"""

    def __init__(self, api_key: str = None, model: str = "llama-3-70b-8192"):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        self.model = model
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })

    def _call(self, messages: List[Dict[str, str]], temperature: float = 0.1) -> Optional[str]:
        """Make API call to Groq."""
        if not self.api_key:
            logger.error("GROQ_API_KEY not configured")
            return None

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 1024
        }

        try:
            response = self.session.post(
                self.BASE_URL,
                json=payload,
                timeout=60
            )
            response.raise_for_status()

            data = response.json()
            return data["choices"][0]["message"]["content"]

        except requests.RequestException as e:
            logger.error(f"Groq API error: {e}")
            return None
        except (KeyError, IndexError) as e:
            logger.error(f"Parse error: {e}")
            return None

    def analyze(self, content: str, context: str = "") -> Optional[Dict[str, Any]]:
        """Analyze a single piece of content."""
        user_msg = f"""Analyze this financial data:

{context}

Raw data:
{content}

Respond with ONLY valid JSON (no markdown code blocks)."""

        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": user_msg}
        ]

        response = self._call(messages)
        if not response:
            return None

        # Extract JSON from response
        try:
            # Find JSON block
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0:
                json_str = response[start:end]
                return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}, response: {response[:200]}")

        return None

    def batch_analyze(self, items: List[Dict[str, Any]], 
                    batch_size: int = 5) -> List[Dict[str, Any]]:
        """Analyze multiple items in batch (more efficient)."""
        if not items:
            return []

        # Combine into batch prompt for efficiency
        batch_content = "\n\n---\n\n".join([
            f"Item {i+1}: {item.get('raw_content', item.get('title', ''))}"
            for i, item in enumerate(items[:batch_size])
        ])

        user_msg = f"""Analyze these {min(batch_size, len(items))} financial data items:

{batch_content}

Respond with a JSON array, one object per item:
[
    {{"importance_score": X, "category": "...", "summary": "...", "reason": "..."}},
    ...
]

IMPORTANT: Respond with ONLY valid JSON array, no markdown."""

        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": user_msg}
        ]

        response = self._call(messages, temperature=0.2)
        if not response:
            returnitems = []
            for item in items[:batch_size]:
                returnitems.append({
                    "importance_score": 5,  # Default score on failure
                    "category": "Equity",
                    "summary": item.get("raw_content", "")[:100],
                    "reason": "Default score (API failed)"
                })
            return returnitems

        # Parse batch response
        try:
            start = response.find("[")
            end = response.rfind("]") + 1
            if start >= 0:
                json_str = response[start:end]
                results = json.loads(json_str)
                
                # Merge with original items
                for i, result in enumerate(results):
                    if i < len(items):
                        items[i].update(result)
                return items[:batch_size]

        except json.JSONDecodeError as e:
            logger.error(f"Batch parse error: {e}")

        # Return defaults on failure
        returnitems = []
        for item in items[:batch_size]:
            returnitems.append({
                "importance_score": 5,
                "category": "Equity", 
                "summary": str(item.get("raw_content", ""))[:100],
                "reason": "Default (parse failed)"
            })
        return returnitems

    def score_with_rules(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback rule-based scoring without AI (for speed/cost)."""
        content = str(item.get("raw_content", "")).lower()
        data_type = item.get("data_type", "")

        score = 5  # Default
        category = "Equity"
        reason = "Default score"

        # OpenInsider scoring
        if data_type == "form_4":
            value = item.get("value", 0)
            is_c_suite = item.get("is_c_suite", False)
            is_large = item.get("is_large_trade", False)

            if is_c_suite and value > 1000000:
                score = 10
                reason = "C-suite > $1M insider buy"
            elif is_c_suite:
                score = 7
                reason = "C-suite insider trade"
            elif is_large and value > 100000:
                score = 8
                reason = "Large insider buy"
            elif is_large:
                score = 6
                reason = "Moderate insider trade"
            category = "Equity"

        # Dark pool scoring  
        elif data_type == "dark_pool":
            pct = item.get("volume_pct", 0)
            if pct > 1000:
                score = 9
                reason = "Extreme dark pool activity"
            elif pct > 500:
                score = 8
                reason = "Very unusual dark pool"
            elif pct > 200:
                score = 7
                reason = "Unusual dark pool"
            category = "Equity"

        # Options flow
        elif data_type == "options_flow":
            sentiment = item.get("sentiment", "neutral")
            if sentiment == "bullish":
                score = 7
                reason = "Unusual call buying (bullish)"
            elif sentiment == "bearish":
                score = 6
                reason = "Unusual put buying (bearish)"
            category = "Equity"

        # Macro
        elif data_type == "macro":
            score = 6
            category = "Macro"
            reason = "Macro indicator update"

        return {
            "importance_score": score,
            "category": category,
            "reason": reason,
            "summary": item.get("raw_content", "")[:100]
        }


# Test if run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("Set GROQ_API_KEY environment variable first")
        exit(1)

    client = GroqClient(api_key)
    
    # Test with sample data
    test_item = {
        "raw_content": "NVDA: CEO Jensen Huang bought 100,000 shares at $450/share",
        "value": 45000000,
        "is_c_suite": True,
        "data_type": "form_4"
    }

    result = client.score_with_rules(test_item)
    print(f"Rule-based: {result}")

    # Try AI if API works
    if api_key:
        result = client.analyze(test_item["raw_content"])
        print(f"AI-based: {result}")
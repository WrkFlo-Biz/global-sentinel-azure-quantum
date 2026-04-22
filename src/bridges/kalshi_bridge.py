#!/usr/bin/env python3
"""Kalshi Bridge — prediction market probabilities for Fed, macro, and geopolitical events."""
import json
import datetime
import os
import urllib.request
import urllib.error

KALSHI_BASE = "https://api.elections.kalshi.com/v2"
KALSHI_TRADING_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def iso_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _get_trading_auth_headers(method, path):
    """Build RSA-PSS signed auth headers for Kalshi trading API."""
    try:
        import time
        import base64
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
        from cryptography.hazmat.primitives.hashes import SHA256
    except ImportError:
        return None

    key_id = os.getenv("KALSHI_API_KEY_ID", "")
    private_key_pem = os.getenv("KALSHI_RSA_PRIVATE_KEY", "")
    if not key_id or not private_key_pem:
        return None

    ts_ms = str(int(time.time() * 1000))
    pem_bytes = private_key_pem.replace("\\n", "\n").encode()
    private_key = load_pem_private_key(pem_bytes, password=None)
    msg = f"{ts_ms}{method}{path}".encode()
    signature = private_key.sign(msg, PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH), SHA256())
    sig_b64 = base64.b64encode(signature).decode()
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
    }


def _fetch_positions():
    """Fetch open Kalshi positions (requires API key)."""
    path = "/trade-api/v2/portfolio/positions"
    headers = _get_trading_auth_headers("GET", path)
    if not headers:
        return []
    try:
        req = urllib.request.Request(f"{KALSHI_TRADING_BASE}/portfolio/positions")
        for k, v in headers.items():
            req.add_header(k, v)
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        positions = data.get("market_positions", [])
        return [{
            "ticker": p.get("ticker", ""),
            "position": p.get("position", 0),
            "market_exposure": p.get("market_exposure", 0),
            "realized_pnl": p.get("realized_pnl", 0),
        } for p in positions if p.get("position", 0) != 0]
    except Exception:
        return []


def poll():
    results = {"timestamp": iso_now(), "markets": [], "fed_rate": {}, "macro": {}}
    for tag in ["fed-funds-rate", "gdp", "inflation", "recession"]:
        try:
            url = f"{KALSHI_BASE}/events?status=open&series_ticker={tag}&limit=10"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            for event in data.get("events", []):
                for market in event.get("markets", []):
                    entry = {
                        "title": market.get("title", ""),
                        "ticker": market.get("ticker", ""),
                        "yes_price": market.get("yes_bid", 0) / 100.0 if market.get("yes_bid") else 0,
                        "no_price": market.get("no_bid", 0) / 100.0 if market.get("no_bid") else 0,
                        "volume": market.get("volume", 0),
                        "category": tag,
                    }
                    results["markets"].append(entry)
                    if tag == "fed-funds-rate":
                        results["fed_rate"][market.get("title", "")] = entry["yes_price"]
                    else:
                        results["macro"][market.get("title", "")] = entry["yes_price"]
        except Exception as e:
            results[f"error_{tag}"] = str(e)[:200]

    results["positions"] = _fetch_positions()
    return results


if __name__ == "__main__":
    print(json.dumps(poll(), indent=2))

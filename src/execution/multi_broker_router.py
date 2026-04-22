#!/usr/bin/env python3
"""
Multi-Broker Smart Router v3.1 for Global Sentinel
===================================================
Routes trades across all 5 brokers with failover:
  - Alpaca    (paper + live, stocks, options, crypto)
  - TastyTrade (cash, unlimited day trades, options, crypto options, FX)
  - IBKR      (futures, forex, international, options via ib_async)
  - Coinbase  (crypto spot via Advanced Trade CDP API)
  - Kalshi    (event contracts / prediction markets via RSA-signed REST)

Routing logic — each trade type lists all capable brokers in priority order:
  - 0DTE / day-trade options  -> TastyTrade > Alpaca > IBKR
  - Weekly/monthly options    -> Alpaca > TastyTrade > IBKR
  - Crypto options            -> TastyTrade (exclusive)
  - Crypto spot               -> Coinbase > Alpaca > TastyTrade
  - Forex                     -> IBKR > TastyTrade
  - Futures                   -> IBKR
  - Stocks                    -> Alpaca > TastyTrade > IBKR
  - International             -> IBKR > Alpaca
  - Event contracts           -> Kalshi
  - Failover on broker down   -> next capable broker in chain
"""
from __future__ import annotations

import json
import logging
import os
import ssl
import time
import traceback
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------
REPO_ROOT = Path(os.getenv("GLOBAL_SENTINEL_REPO_ROOT", "/opt/global-sentinel"))
QUANTUM_DIR = REPO_ROOT / "data" / "quantum_feed"
QUANTUM_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = QUANTUM_DIR / "broker_routing.json"
ROUTING_LOG_PATH = QUANTUM_DIR / "broker_routing_log.jsonl"
HEALTH_LOG_PATH = QUANTUM_DIR / "broker_health_log.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] ROUTER: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("gs.multi_broker_router")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_env() -> Dict[str, str]:
    env: Dict[str, str] = {}
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
                os.environ.setdefault(k.strip(), v.strip())
    return env


ENV = _load_env()
IBKR_CP_BASE_URL = os.getenv("IBKR_CP_BASE_URL", "https://127.0.0.1:5000/v1/api")
IBKR_ACCOUNT = ENV.get("IBKR_ACCOUNT_1", "") or os.getenv("IBKR_ACCOUNT_1", "")
_IBKR_SSL_CONTEXT = ssl.create_default_context()
_IBKR_SSL_CONTEXT.check_hostname = False
_IBKR_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


def _ibkr_cp_request(path: str, method: str = "GET",
                     payload: Optional[Dict[str, Any]] = None,
                     timeout: int = 10) -> Any:
    url = f"{IBKR_CP_BASE_URL}{path}"
    data = None
    req = urllib.request.Request(url, method=method)
    if payload is not None:
        data = json.dumps(payload).encode()
        req.data = data
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout, context=_IBKR_SSL_CONTEXT) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode())


def _ibkr_resolve_stock_conid(symbol: str) -> int:
    """Resolve an IBKR stock symbol to conid via Client Portal search."""
    query = urllib.parse.urlencode({"symbol": symbol, "name": "true"})
    data = _ibkr_cp_request(f"/iserver/secdef/search?{query}", timeout=12)
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"no secdef results for {symbol}")

    sym_upper = symbol.upper()
    for item in data:
        if str(item.get("symbol", "")).upper() == sym_upper and item.get("conid"):
            return int(item["conid"])
    for item in data:
        if item.get("conid"):
            return int(item["conid"])
    raise RuntimeError(f"no conid found for {symbol}")

# ---------------------------------------------------------------------------
# Broker configuration
# ---------------------------------------------------------------------------

class BrokerConfig:
    def __init__(self, name: str, account_type: str, has_api: bool,
                 unlimited_day_trades: bool, options_commission: float,
                 stock_commission: float, best_for: List[str], priority: int):
        self.name = name
        self.account_type = account_type
        self.has_api = has_api
        self.unlimited_day_trades = unlimited_day_trades
        self.options_commission = options_commission
        self.stock_commission = stock_commission
        self.best_for = best_for
        self.priority = priority


BROKERS = {
    "tastytrade": BrokerConfig(
        name="tastytrade",
        account_type="cash",
        has_api=True,
        unlimited_day_trades=True,
        options_commission=1.00,
        stock_commission=0.00,
        best_for=["0dte_options", "day_trade_options", "options_spreads",
                   "options_any", "weekly_options", "monthly_options", "crypto"],
        priority=1,
    ),
    "alpaca": BrokerConfig(
        name="alpaca",
        account_type="margin_restricted",
        has_api=True,
        unlimited_day_trades=False,
        options_commission=0.00,
        stock_commission=0.00,
        best_for=["market_data", "paper_trading", "overnight_holds", "stocks",
                   "crypto", "us_equity"],
        priority=2,
    ),
    "ibkr": BrokerConfig(
        name="ibkr",
        account_type="cash",
        has_api=True,
        unlimited_day_trades=True,
        options_commission=0.65,
        stock_commission=0.005,
        best_for=["global_markets", "forex", "futures", "swing_trades",
                   "international", "options_any"],
        priority=3,
    ),
    "coinbase": BrokerConfig(
        name="coinbase",
        account_type="exchange",
        has_api=True,
        unlimited_day_trades=True,
        options_commission=0.00,
        stock_commission=0.00,
        best_for=["crypto", "crypto_spot"],
        priority=4,
    ),
    "kalshi": BrokerConfig(
        name="kalshi",
        account_type="exchange",
        has_api=True,
        unlimited_day_trades=True,
        options_commission=0.00,
        stock_commission=0.00,
        best_for=["event_contracts", "prediction_markets"],
        priority=5,
    ),
}

# ---------------------------------------------------------------------------
# Broker health cache
# ---------------------------------------------------------------------------

class BrokerHealth:
    """Cached health + balance state per broker."""
    def __init__(self):
        self.connected: bool = False
        self.error: str = ""
        self.buying_power: float = 0.0
        self.equity: float = 0.0
        self.day_trades_remaining: int = 0
        self.last_check: float = 0.0
        self.extra: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connected": self.connected,
            "error": self.error,
            "buying_power": self.buying_power,
            "equity": self.equity,
            "day_trades_remaining": self.day_trades_remaining,
            "last_check": self.last_check,
            "extra": self.extra,
        }


_broker_health: Dict[str, BrokerHealth] = {
    "alpaca": BrokerHealth(),
    "tastytrade": BrokerHealth(),
    "ibkr": BrokerHealth(),
    "coinbase": BrokerHealth(),
    "kalshi": BrokerHealth(),
}
# Tastytrade auth backoff state
_tastytrade_backoff_until = 0.0
_tastytrade_fail_count = 0


# ---------------------------------------------------------------------------
# Balance checkers
# ---------------------------------------------------------------------------

def _check_alpaca_health() -> BrokerHealth:
    """Check Alpaca live account balance via REST API."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        key = ENV.get("ALPACA_API_KEY_LIVE", "")
        secret = ENV.get("ALPACA_SECRET_KEY_LIVE", "")
        if not key or not secret:
            key = ENV.get("ALPACA_API_KEY", "")
            secret = ENV.get("ALPACA_SECRET_KEY", "")
        if not key:
            h.error = "no_credentials"
            return h

        req = urllib.request.Request("https://api.alpaca.markets/v2/account")
        req.add_header("APCA-API-KEY-ID", key)
        req.add_header("APCA-API-SECRET-KEY", secret)
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read())

        h.connected = True
        h.buying_power = float(d.get("buying_power", 0))
        h.equity = float(d.get("equity", 0))
        h.day_trades_remaining = max(0, 3 - int(d.get("daytrade_count", 0)))
        h.extra = {
            "account_number": d.get("account_number", ""),
            "status": d.get("status", ""),
            "pattern_day_trader": d.get("pattern_day_trader", False),
        }
    except Exception as e:
        h.error = str(e)[:200]
    return h


def _check_alpaca_paper_health(acct_label: str, key_env: str, secret_env: str) -> BrokerHealth:
    """Check a specific Alpaca paper account."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        key = ENV.get(key_env, "")
        secret = ENV.get(secret_env, "")
        if not key:
            h.error = f"no_credentials ({key_env})"
            return h
        req = urllib.request.Request("https://paper-api.alpaca.markets/v2/account")
        req.add_header("APCA-API-KEY-ID", key)
        req.add_header("APCA-API-SECRET-KEY", secret)
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read())
        h.connected = True
        h.buying_power = float(d.get("buying_power", 0))
        h.equity = float(d.get("equity", 0))
        # PDT accounts with equity > $25K have unlimited day trades
        is_pdt = d.get("pattern_day_trader", False)
        equity = h.equity
        if is_pdt and equity >= 25000:
            h.day_trades_remaining = 999  # unlimited
        else:
            h.day_trades_remaining = max(0, 3 - int(d.get("daytrade_count", 0)))
        h.extra = {
            "label": acct_label,
            "account_number": d.get("account_number", ""),
            "pattern_day_trader": is_pdt,
        }
    except Exception as e:
        h.error = str(e)[:200]
    return h


def _check_tastytrade_health() -> BrokerHealth:
    """Check Tastytrade balance via REST session login."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        username = ENV.get("TASTYTRADE_USERNAME", "") or os.getenv("TASTYTRADE_USERNAME", "")
        password = ENV.get("TASTYTRADE_PASSWORD", "") or os.getenv("TASTYTRADE_PASSWORD", "")
        remember_token = ENV.get("TASTYTRADE_REMEMBER_TOKEN", "") or os.getenv("TASTYTRADE_REMEMBER_TOKEN", "")
        if not username or (not password and not remember_token):
            h.error = "no_credentials"
            return h

        auth_attempts: List[tuple[str, Dict[str, Any]]] = []
        if remember_token:
            auth_attempts.append(("remember_token", {"login": username, "remember-token": remember_token}))
        if password:
            auth_attempts.append(("password", {"login": username, "password": password}))

        session_data: Dict[str, Any] = {}
        auth_method = ""
        last_error = ""
        for auth_method, auth_payload in auth_attempts:
            body = json.dumps(auth_payload).encode()
            req = urllib.request.Request(
                "https://api.tastyworks.com/sessions",
                data=body,
                method="POST",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    session_data = json.loads(resp.read())
                break
            except urllib.error.HTTPError as e:
                detail = e.read().decode() if e.fp else str(e)
                last_error = f"http_{e.code}:{detail[:120]}"
                if auth_method == "remember_token" and password and e.code in (400, 401, 403):
                    logger.warning("Tastytrade remember-token rejected; retrying password auth")
                    continue
                raise

        if not session_data:
            h.error = last_error or "tastytrade_auth_failed"
            return h

        token = (session_data.get("data") or {}).get("session-token", "")
        if not token:
            h.error = "no_session_token"
            return h

        acct_req = urllib.request.Request("https://api.tastyworks.com/customers/me/accounts")
        acct_req.add_header("Authorization", token)
        with urllib.request.urlopen(acct_req, timeout=15) as resp:
            account_data = json.loads(resp.read())

        accounts = (account_data.get("data") or {}).get("items", [])
        if not accounts:
            h.error = "no_accounts"
            return h

        acct = accounts[0].get("account", {})
        acct_num = acct.get("account-number", "")
        bal_req = urllib.request.Request(f"https://api.tastyworks.com/accounts/{acct_num}/balances")
        bal_req.add_header("Authorization", token)
        with urllib.request.urlopen(bal_req, timeout=15) as resp:
            balances = json.loads(resp.read())

        bal_data = balances.get("data") or {}
        h.connected = True
        h.buying_power = float(bal_data.get("derivative-buying-power", bal_data.get("buying-power", 0)) or 0)
        h.equity = float(bal_data.get("net-liquidating-value", 0) or 0)
        h.day_trades_remaining = 999
        h.extra = {
            "account_number": acct_num,
            "cash_balance": str(bal_data.get("cash-balance", 0)),
            "maintenance_excess": str(bal_data.get("maintenance-excess", "N/A")),
            "auth_method": auth_method,
            "execution_supported": True,
        }
    except Exception as e:
        h.error = str(e)[:200]
    return h


def _check_ibkr_health() -> BrokerHealth:
    """Check IBKR Client Portal gateway health via localhost:5000."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        auth = _ibkr_cp_request("/iserver/auth/status", timeout=8)
        authenticated = bool(auth.get("authenticated", False))
        if not authenticated:
            h.error = "client_portal_not_authenticated"
            h.extra = {
                "api_mode": "client_portal",
                "base_url": IBKR_CP_BASE_URL,
                "execution_supported": False,
                "execution_reason": "not_authenticated",
            }
            return h

        h.connected = True
        h.day_trades_remaining = 999
        h.extra = {
            "api_mode": "client_portal",
            "base_url": IBKR_CP_BASE_URL,
            "account": IBKR_ACCOUNT,
            "execution_supported": bool(IBKR_ACCOUNT),
        }
    except urllib.error.HTTPError as e:
        if e.code == 401:
            h.error = "client_portal_not_authenticated"
            h.extra = {
                "api_mode": "client_portal",
                "base_url": IBKR_CP_BASE_URL,
                "execution_supported": False,
                "execution_reason": "not_authenticated",
            }
        else:
            h.error = f"ibkr_http_{e.code}"
    except Exception as e:
        h.error = str(e)[:200]
    return h


def _get_coinbase_client():
    """Get a Coinbase REST client using the SDK."""
    try:
        from coinbase.rest import RESTClient
    except ImportError:
        return None

    key_name = os.getenv("COINBASE_API_KEY_NAME", "")
    private_key_pem = os.getenv("COINBASE_API_PRIVATE_KEY", "")
    if not key_name or not private_key_pem:
        return None

    pem = private_key_pem.replace("\\n", "\n")
    return RESTClient(api_key=key_name, api_secret=pem)


def _check_coinbase_health() -> BrokerHealth:
    """Check Coinbase Advanced Trade API connectivity."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        client = _get_coinbase_client()
        if not client:
            h.error = "no_credentials or coinbase SDK missing"
            return h

        data = client.get_accounts()
        accounts = data.get("accounts", []) if isinstance(data, dict) else []
        total_usd = 0.0
        for acct in accounts:
            if acct.get("available_balance", {}).get("currency") == "USD":
                total_usd += float(acct["available_balance"].get("value", 0) or 0)

        h.connected = True
        h.buying_power = total_usd
        h.equity = total_usd
        h.day_trades_remaining = 999
        h.extra = {
            "num_accounts": len(accounts),
            "execution_supported": True,
        }
    except Exception as e:
        h.error = str(e)[:200]
    return h


KALSHI_TRADING_API = "https://api.elections.kalshi.com/trade-api/v2"


def _kalshi_auth_headers(method: str, path: str) -> Dict[str, str]:
    """Build RSA-PSS signed auth headers for Kalshi trading API."""
    try:
        import base64
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
        from cryptography.hazmat.primitives.hashes import SHA256
    except ImportError:
        return {}

    key_id = os.getenv("KALSHI_API_KEY_ID", "")
    private_key_pem = os.getenv("KALSHI_RSA_PRIVATE_KEY", "")
    if not key_id or not private_key_pem:
        return {}

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


def _check_kalshi_health() -> BrokerHealth:
    """Check Kalshi trading API connectivity and balance."""
    h = BrokerHealth()
    h.last_check = time.time()
    try:
        key_id = os.getenv("KALSHI_API_KEY_ID", "")
        private_key_pem = os.getenv("KALSHI_RSA_PRIVATE_KEY", "")
        if not key_id or not private_key_pem:
            h.error = "no_credentials"
            return h

        path = "/trade-api/v2/portfolio/balance"
        headers = _kalshi_auth_headers("GET", path)
        if not headers:
            h.error = "auth_build_failed (cryptography pkg missing?)"
            return h

        req = urllib.request.Request(f"{KALSHI_TRADING_API}/portfolio/balance")
        for k, v in headers.items():
            req.add_header(k, v)
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        balance_cents = data.get("balance", 0)
        balance_usd = balance_cents / 100.0
        h.connected = True
        h.buying_power = balance_usd
        h.equity = balance_usd
        h.day_trades_remaining = 999
        h.extra = {
            "portfolio_value": data.get("portfolio_value", 0) / 100.0,
            "execution_supported": True,
        }
    except Exception as e:
        h.error = str(e)[:200]
    return h


def refresh_all_health(force: bool = False) -> Dict[str, Dict[str, Any]]:
    """Refresh health for all brokers. Skips if checked within 60s unless force."""
    now = time.time()
    cutoff = 0 if force else 60

    if now - _broker_health["alpaca"].last_check > cutoff:
        _broker_health["alpaca"] = _check_alpaca_health()
        logger.info("Alpaca: connected=%s equity=%.0f bp=%.0f dt_rem=%d %s",
                     _broker_health["alpaca"].connected,
                     _broker_health["alpaca"].equity,
                     _broker_health["alpaca"].buying_power,
                     _broker_health["alpaca"].day_trades_remaining,
                     _broker_health["alpaca"].error or "OK")

    global _tastytrade_backoff_until, _tastytrade_fail_count
    if now - _broker_health["tastytrade"].last_check > cutoff:
        if now < _tastytrade_backoff_until:
            logger.debug("Tastytrade: skipping health check (backoff until %.0f, now %.0f)", _tastytrade_backoff_until, now)
        else:
            _broker_health["tastytrade"] = _check_tastytrade_health()
            if _broker_health["tastytrade"].error and not _broker_health["tastytrade"].connected:
                _tastytrade_fail_count += 1
                # Exponential backoff: 1h, 4h, 8h, max 24h
                backoff_secs = min(3600 * (4 ** (_tastytrade_fail_count - 1)), 86400)
                _tastytrade_backoff_until = now + backoff_secs
                logger.warning("Tastytrade auth failed (%d consecutive), backing off %.0fh: %s",
                               _tastytrade_fail_count, backoff_secs / 3600,
                               _broker_health["tastytrade"].error)
            else:
                _tastytrade_fail_count = 0
                _tastytrade_backoff_until = 0.0
            logger.info("Tastytrade: connected=%s equity=%.0f bp=%.0f %s",
                         _broker_health["tastytrade"].connected,
                         _broker_health["tastytrade"].equity,
                         _broker_health["tastytrade"].buying_power,
                         _broker_health["tastytrade"].error or "OK")

    if now - _broker_health["ibkr"].last_check > cutoff:
        _broker_health["ibkr"] = _check_ibkr_health()
        logger.info("IBKR: connected=%s equity=%.0f bp=%.0f %s",
                     _broker_health["ibkr"].connected,
                     _broker_health["ibkr"].equity,
                     _broker_health["ibkr"].buying_power,
                     _broker_health["ibkr"].error or "OK")

    if now - _broker_health["coinbase"].last_check > cutoff:
        _broker_health["coinbase"] = _check_coinbase_health()
        logger.info("Coinbase: connected=%s equity=%.0f %s",
                     _broker_health["coinbase"].connected,
                     _broker_health["coinbase"].equity,
                     _broker_health["coinbase"].error or "OK")

    if now - _broker_health["kalshi"].last_check > cutoff:
        _broker_health["kalshi"] = _check_kalshi_health()
        logger.info("Kalshi: connected=%s equity=%.0f %s",
                     _broker_health["kalshi"].connected,
                     _broker_health["kalshi"].equity,
                     _broker_health["kalshi"].error or "OK")

    _broker_health.setdefault("alpaca_paper_dt", BrokerHealth())
    _broker_health.setdefault("alpaca_paper_ml", BrokerHealth())
    if now - _broker_health["alpaca_paper_dt"].last_check > cutoff:
        _broker_health["alpaca_paper_dt"] = _check_alpaca_paper_health(
            "daytrade", "ALPACA_API_KEY", "ALPACA_SECRET_KEY")
    if now - _broker_health["alpaca_paper_ml"].last_check > cutoff:
        _broker_health["alpaca_paper_ml"] = _check_alpaca_paper_health(
            "medlong", "ALPACA_API_KEY_MEDLONG", "ALPACA_SECRET_KEY_MEDLONG")

    entry = {
        "timestamp": iso_now(),
        "brokers": {k: v.to_dict() for k, v in _broker_health.items()},
    }
    try:
        with open(HEALTH_LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

    return {k: v.to_dict() for k, v in _broker_health.items()}


def get_broker_balances() -> Dict[str, Any]:
    """Legacy API -- returns balances dict keyed by broker name."""
    refresh_all_health()
    result = {}
    for name in ("alpaca", "tastytrade", "ibkr", "coinbase", "kalshi"):
        h = _broker_health[name]
        if h.connected:
            result[name] = {
                "buying_power": h.buying_power,
                "day_trades_remaining": h.day_trades_remaining,
                "equity": h.equity,
            }
        else:
            result[name] = {"error": h.error or "not connected"}
    return result


# ---------------------------------------------------------------------------
# Routing intelligence
# ---------------------------------------------------------------------------

def classify_trade(symbol: str, qty: int, side: str, order_type: str,
                   asset_class: str = "auto", is_day_trade: bool = False,
                   expiration: Optional[str] = None) -> str:
    """Classify a trade into a routing category."""
    sym_upper = symbol.upper()

    if asset_class == "auto":
        if any(sym_upper.endswith(sfx) for sfx in ("USD", "USDT", "BTC", "ETH")):
            asset_class = "crypto"
        elif "/" in sym_upper:
            asset_class = "forex"
        elif len(sym_upper) > 10 or (expiration is not None):
            asset_class = "us_option"
        else:
            asset_class = "us_equity"

    if asset_class == "crypto":
        return "crypto"
    if asset_class == "crypto_option":
        return "crypto_option"
    if asset_class == "forex":
        return "forex"
    if asset_class == "futures":
        return "futures"
    if asset_class in ("event_contract", "prediction"):
        return "event_contract"
    if asset_class in ("international", "global"):
        return "international"

    if asset_class == "us_option":
        if expiration:
            try:
                exp_date = datetime.strptime(expiration, "%Y-%m-%d").date()
                today = date.today()
                days_to_exp = (exp_date - today).days
                if days_to_exp <= 0:
                    return "0dte_option"
                elif days_to_exp <= 7:
                    return "weekly_option"
                else:
                    return "monthly_option"
            except Exception:
                pass
        if is_day_trade:
            return "day_trade_option"
        return "monthly_option"

    if is_day_trade:
        return "stock_daytrade"
    return "stock_overnight"


COMMISSION_MAP = {
    "tastytrade": {"option": 1.00, "stock": 0.00, "crypto": 0.00},
    "alpaca":     {"option": 0.00, "stock": 0.00, "crypto": 0.00},
    "ibkr":       {"option": 0.65, "stock": 0.005, "crypto": 0.00},
    "coinbase":   {"option": 0.00, "stock": 0.00, "crypto": 0.00},
    "kalshi":     {"option": 0.00, "stock": 0.00, "crypto": 0.00, "event": 0.00},
}


def estimate_commission(broker: str, trade_category: str, qty: int) -> float:
    """Estimate commission for a trade."""
    cmap = COMMISSION_MAP.get(broker, {})
    if "option" in trade_category:
        return cmap.get("option", 0) * abs(qty)
    elif "crypto" in trade_category:
        return cmap.get("crypto", 0)
    else:
        return cmap.get("stock", 0) * abs(qty)


ROUTING_TABLE = {
    "0dte_option":      ["tastytrade", "alpaca", "ibkr"],
    "day_trade_option": ["tastytrade", "alpaca", "ibkr"],
    "weekly_option":    ["alpaca", "tastytrade", "ibkr"],
    "monthly_option":   ["alpaca", "tastytrade", "ibkr"],
    "crypto_option":    ["tastytrade"],
    "stock_overnight":  ["alpaca", "tastytrade", "ibkr"],
    "stock_daytrade":   ["tastytrade", "alpaca", "ibkr"],
    "crypto":           ["coinbase", "alpaca", "tastytrade"],
    "forex":            ["ibkr", "tastytrade"],
    "futures":          ["ibkr"],
    "event_contract":   ["kalshi"],
    "international":    ["ibkr", "alpaca"],
}

BROKER_CAPABILITIES = {
    "alpaca":     {"stocks", "options", "crypto"},
    "tastytrade": {"stocks", "options", "crypto_options", "crypto", "forex"},
    "ibkr":       {"stocks", "options", "futures", "forex", "international", "bonds"},
    "coinbase":   {"crypto"},
    "kalshi":     {"event_contracts"},
}


def select_broker(symbol: str, qty: int, side: str, order_type: str,
                  asset_class: str = "auto", is_day_trade: bool = False,
                  expiration: Optional[str] = None,
                  min_buying_power: float = 0,
                  broker_override: Optional[str] = None,
                  alpaca_account: str = "paper_dt") -> Dict[str, Any]:
    """Select the best broker for a trade. Returns routing decision dict."""
    category = classify_trade(symbol, qty, side, order_type, asset_class,
                              is_day_trade, expiration)

    if broker_override and broker_override in BROKERS:
        h = _broker_health.get(broker_override, BrokerHealth())
        return {
            "broker": broker_override,
            "category": category,
            "reason": f"broker_override={broker_override}",
            "connected": h.connected,
            "buying_power": h.buying_power,
            "commission_est": estimate_commission(broker_override, category, qty),
            "failover": False,
        }

    preferred = ROUTING_TABLE.get(category, ["alpaca", "tastytrade", "ibkr"])

    for i, broker_name in enumerate(preferred):
        h = _broker_health.get(broker_name, BrokerHealth())
        # For Alpaca, use paper account health when trading on paper
        if broker_name == "alpaca" and alpaca_account.startswith("paper"):
            paper_key = "alpaca_paper_dt" if alpaca_account == "paper_dt" else "alpaca_paper_ml"
            paper_h = _broker_health.get(paper_key)
            if paper_h and paper_h.connected:
                h = paper_h  # Use paper account's bp/equity/day trades
        if not h.connected:
            continue
        if not h.extra.get("execution_supported", True):
            continue
        if min_buying_power > 0 and h.buying_power < min_buying_power:
            continue
        if is_day_trade and not BROKERS[broker_name].unlimited_day_trades:
            if h.day_trades_remaining <= 0:
                continue

        commission = estimate_commission(broker_name, category, qty)
        return {
            "broker": broker_name,
            "category": category,
            "reason": _build_reason(broker_name, category, i),
            "connected": True,
            "buying_power": h.buying_power,
            "commission_est": commission,
            "failover": i > 0,
            "failover_from": preferred[0] if i > 0 else None,
        }

    return {
        "broker": None,
        "category": category,
        "reason": "no_broker_available",
        "connected": False,
        "buying_power": 0,
        "commission_est": 0,
        "failover": False,
    }


def _build_reason(broker: str, category: str, idx: int) -> str:
    reasons = {
        ("tastytrade", "0dte_option"): "cash account, unlimited day trades for 0DTE",
        ("tastytrade", "day_trade_option"): "cash account, unlimited day trades",
        ("tastytrade", "stock_daytrade"): "cash account, unlimited day trades",
        ("tastytrade", "crypto_option"): "crypto options specialist (BTC/ETH)",
        ("tastytrade", "forex"): "TastyTrade FX account",
        ("tastytrade", "crypto"): "TastyTrade crypto support",
        ("alpaca", "stock_overnight"): "free commissions, already set up for overnight",
        ("alpaca", "weekly_option"): "$0 option commissions",
        ("alpaca", "monthly_option"): "$0 option commissions",
        ("alpaca", "crypto"): "native crypto trading, 15 pairs",
        ("coinbase", "crypto"): "Coinbase Advanced Trade, deep liquidity",
        ("ibkr", "forex"): "IDEALPRO, global FX access",
        ("ibkr", "futures"): "GLOBEX/NYMEX/COMEX futures access",
        ("ibkr", "international"): "global markets access",
        ("kalshi", "event_contract"): "Kalshi event contracts, prediction markets",
    }
    base = reasons.get((broker, category), f"{broker} selected for {category}")
    if idx > 0:
        base = f"FAILOVER: {base}"
    return base


def get_all_capable_brokers(asset_class: str) -> List[Dict[str, Any]]:
    """Return all brokers that support a given instrument type, with health status."""
    cap_map = {
        "stocks": ["alpaca", "tastytrade", "ibkr"],
        "options": ["tastytrade", "alpaca", "ibkr"],
        "crypto_options": ["tastytrade"],
        "crypto": ["coinbase", "alpaca", "tastytrade"],
        "forex": ["ibkr", "tastytrade"],
        "futures": ["ibkr"],
        "international": ["ibkr", "alpaca"],
        "bonds": ["ibkr"],
        "event_contracts": ["kalshi"],
    }
    brokers = cap_map.get(asset_class, [])
    result = []
    for b in brokers:
        h = _broker_health.get(b, BrokerHealth())
        result.append({
            "broker": b,
            "connected": h.connected,
            "buying_power": h.buying_power,
            "day_trades_remaining": h.day_trades_remaining,
            "supports": list(BROKER_CAPABILITIES.get(b, set())),
        })
    return result


# ---------------------------------------------------------------------------
# Routing log
# ---------------------------------------------------------------------------

def _log_routing_decision(decision: Dict[str, Any], trade_params: Dict[str, Any]):
    """Append routing decision to JSONL log."""
    entry = {
        "timestamp": iso_now(),
        "decision": decision,
        "trade": trade_params,
    }
    try:
        with open(ROUTING_LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.error("Failed to log routing decision: %s", e)


# ---------------------------------------------------------------------------
# Trade execution per broker
# ---------------------------------------------------------------------------

def _execute_alpaca(symbol: str, qty: int, side: str, order_type: str,
                    limit_price: Optional[float], account: str = "paper_dt",
                    time_in_force: str = "day", notional: Optional[float] = None) -> Dict[str, Any]:
    """Execute trade via Alpaca REST API."""
    if account == "paper_dt":
        key = ENV.get("ALPACA_API_KEY", "")
        secret = ENV.get("ALPACA_SECRET_KEY", "")
        base = "https://paper-api.alpaca.markets"
    elif account == "paper_ml":
        key = ENV.get("ALPACA_API_KEY_MEDLONG", "")
        secret = ENV.get("ALPACA_SECRET_KEY_MEDLONG", "")
        base = "https://paper-api.alpaca.markets"
    elif account == "live":
        key = ENV.get("ALPACA_API_KEY_LIVE", "")
        secret = ENV.get("ALPACA_SECRET_KEY_LIVE", "")
        base = "https://api.alpaca.markets"
    else:
        return {"error": f"unknown alpaca account: {account}"}

    order_data: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "time_in_force": time_in_force,
    }
    if notional and notional > 0:
        order_data["notional"] = str(round(notional, 2))
    else:
        order_data["qty"] = str(abs(qty))

    if order_type == "limit" and limit_price:
        order_data["limit_price"] = str(limit_price)

    body = json.dumps(order_data).encode()
    req = urllib.request.Request(f"{base}/v2/orders", data=body, method="POST")
    req.add_header("APCA-API-KEY-ID", key)
    req.add_header("APCA-API-SECRET-KEY", secret)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
        return {
            "status": "submitted",
            "broker": "alpaca",
            "account": account,
            "order_id": result.get("id", ""),
            "client_order_id": result.get("client_order_id", ""),
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "order_type": order_type,
        }
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else str(e)
        return {"error": f"alpaca_http_{e.code}", "details": err_body[:300]}
    except Exception as e:
        return {"error": str(e)[:300]}


def _execute_alpaca_flatten(symbol: str, account: str = "paper_dt") -> Dict[str, Any]:
    """Flatten (close) a position on Alpaca."""
    if account == "paper_dt":
        key = ENV.get("ALPACA_API_KEY", "")
        secret = ENV.get("ALPACA_SECRET_KEY", "")
        base = "https://paper-api.alpaca.markets"
    elif account == "paper_ml":
        key = ENV.get("ALPACA_API_KEY_MEDLONG", "")
        secret = ENV.get("ALPACA_SECRET_KEY_MEDLONG", "")
        base = "https://paper-api.alpaca.markets"
    else:
        key = ENV.get("ALPACA_API_KEY_LIVE", "")
        secret = ENV.get("ALPACA_SECRET_KEY_LIVE", "")
        base = "https://api.alpaca.markets"

    req = urllib.request.Request(f"{base}/v2/positions/{symbol}", method="DELETE")
    req.add_header("APCA-API-KEY-ID", key)
    req.add_header("APCA-API-SECRET-KEY", secret)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        return {"status": "flattened", "broker": "alpaca", "account": account, "symbol": symbol}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"status": "no_position", "broker": "alpaca", "symbol": symbol}
        return {"error": f"alpaca_flatten_{e.code}"}
    except Exception as e:
        return {"error": str(e)[:200]}


TASTYTRADE_CASH_ACCOUNT = "5WI54260"
TASTYTRADE_MARGIN_ACCOUNT = "5WI54194"


def _execute_tastytrade(symbol: str, qty: int, side: str, order_type: str,
                        limit_price: Optional[float], asset_class: str = "us_equity",
                        time_in_force: str = "day",
                        preferred_account: Optional[str] = None) -> Dict[str, Any]:
    """Execute trade via Tastytrade SDK."""
    try:
        from tastytrade import Session, Account
        from tastytrade.instruments import Equity, Option
        from tastytrade.order import (NewOrder, OrderAction, OrderTimeInForce,
                                       OrderType, PriceEffect)

        username = ENV.get("TASTYTRADE_USERNAME", "") or os.getenv("TASTYTRADE_USERNAME", "")
        password = ENV.get("TASTYTRADE_PASSWORD", "") or os.getenv("TASTYTRADE_PASSWORD", "")
        session = Session(username, password)
        accounts = Account.get_accounts(session)
        if not accounts:
            return {"error": "tastytrade_no_accounts"}

        target = preferred_account or TASTYTRADE_CASH_ACCOUNT
        acct = next((a for a in accounts if a.account_number == target), None)
        if not acct:
            logger.warning("Tastytrade account %s not found, using first", target)
            acct = accounts[0]

        if asset_class == "us_option":
            instrument = Option.get_option(session, symbol)
            action = OrderAction.BUY_TO_OPEN if side == "buy" else OrderAction.SELL_TO_CLOSE
        else:
            instrument = Equity.get_equity(session, symbol)
            action = OrderAction.BUY_TO_OPEN if side == "buy" else OrderAction.SELL_TO_CLOSE

        tif = OrderTimeInForce.DAY if time_in_force == "day" else OrderTimeInForce.GTC

        if order_type == "limit" and limit_price:
            order = NewOrder(
                time_in_force=tif,
                order_type=OrderType.LIMIT,
                price=float(limit_price),
                price_effect=PriceEffect.DEBIT if side == "buy" else PriceEffect.CREDIT,
                legs=[instrument.build_leg(abs(qty), action)],
            )
        else:
            order = NewOrder(
                time_in_force=tif,
                order_type=OrderType.MARKET,
                legs=[instrument.build_leg(abs(qty), action)],
            )

        response = acct.place_order(session, order)
        return {
            "status": "submitted",
            "broker": "tastytrade",
            "account": acct.account_number,
            "order_response": str(response)[:300],
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
        }
    except ImportError:
        return {"error": "tastytrade_sdk_not_installed"}
    except Exception as e:
        return {"error": f"tastytrade: {str(e)[:300]}"}


def _execute_ibkr(symbol: str, qty: int, side: str, order_type: str,
                  limit_price: Optional[float], asset_class: str = "us_equity",
                  exchange: str = "SMART") -> Dict[str, Any]:
    """Execute trade via IBKR Client Portal REST on localhost:5000."""
    try:
        if not IBKR_ACCOUNT:
            return {"error": "ibkr_account_missing"}
        if asset_class == "futures":
            return _execute_ibkr_futures(symbol, qty, side, order_type, limit_price, exchange)
        if asset_class == "forex":
            return _execute_ibkr_forex(symbol, qty, side, order_type, limit_price)
        if asset_class not in ("auto", "us_equity", "stock", "international"):
            return {"error": f"ibkr_client_portal_unsupported_asset_class:{asset_class}"}

        auth = _ibkr_cp_request("/iserver/auth/status", timeout=8)
        if not auth.get("authenticated", False):
            return {"error": "ibkr_client_portal_not_authenticated"}

        conid = _ibkr_resolve_stock_conid(symbol)
        order_payload: Dict[str, Any] = {
            "acctId": IBKR_ACCOUNT,
            "conid": conid,
            "orderType": "LMT" if order_type == "limit" and limit_price else "MKT",
            "side": "BUY" if side == "buy" else "SELL",
            "quantity": abs(int(qty)),
            "tif": "DAY",
            "outsideRTH": False,
        }
        if order_payload["orderType"] == "LMT":
            order_payload["price"] = float(limit_price)

        response = _ibkr_cp_request(
            f"/iserver/account/{IBKR_ACCOUNT}/orders",
            method="POST",
            payload={"orders": [order_payload]},
            timeout=15,
        )

        while isinstance(response, list) and response and response[0].get("id"):
            response = _ibkr_cp_request(
                f"/iserver/reply/{response[0]['id']}",
                method="POST",
                payload={"confirmed": True},
                timeout=15,
            )

        order_info: Dict[str, Any]
        if isinstance(response, list) and response:
            order_info = response[0]
        elif isinstance(response, dict):
            order_info = response
        else:
            order_info = {"raw_response": str(response)}

        return {
            "status": "submitted",
            "broker": "ibkr",
            "order_id": str(order_info.get("order_id", order_info.get("id", ""))),
            "order_status": order_info.get("order_status", order_info.get("status", "submitted")),
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "conid": str(conid),
        }
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return {"error": "ibkr_client_portal_not_authenticated"}
        return {"error": f"ibkr_http_{e.code}"}
    except Exception as e:
        return {"error": f"ibkr: {str(e)[:300]}"}


def _execute_ibkr_futures(symbol: str, qty: int, side: str, order_type: str,
                          limit_price: Optional[float], exchange: str = "GLOBEX") -> Dict[str, Any]:
    """Execute futures trade via IBKR ib_async (not Client Portal — it only handles stocks)."""
    try:
        from ib_async import IB, Contract, MarketOrder, LimitOrder
    except ImportError:
        return {"error": "ib_async_not_installed"}

    EXCHANGE_MAP = {
        "ES": "GLOBEX", "NQ": "GLOBEX", "RTY": "GLOBEX", "YM": "CBOT",
        "CL": "NYMEX", "NG": "NYMEX", "HO": "NYMEX", "RB": "NYMEX",
        "GC": "COMEX", "SI": "COMEX", "HG": "COMEX",
        "ZB": "CBOT", "ZN": "CBOT", "ZF": "CBOT",
    }
    exch = EXCHANGE_MAP.get(symbol, exchange)

    try:
        ib = IB()
        host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
        port = int(os.getenv("IB_GATEWAY_PORT", "4001"))
        ib.connect(host, port, clientId=int(os.getenv("IB_CLIENT_ID_FUTURES", "3")))

        contract = Contract(secType="FUT", symbol=symbol, exchange=exch, currency="USD")
        ib.qualifyContracts(contract)

        if order_type == "limit" and limit_price:
            order = LimitOrder("BUY" if side == "buy" else "SELL", abs(qty), float(limit_price))
        else:
            order = MarketOrder("BUY" if side == "buy" else "SELL", abs(qty))

        trade = ib.placeOrder(contract, order)
        ib.sleep(2)
        ib.disconnect()

        return {
            "status": "submitted",
            "broker": "ibkr_futures",
            "order_id": str(trade.order.orderId),
            "order_status": trade.orderStatus.status,
            "symbol": symbol,
            "sec_type": "FUT",
            "exchange": exch,
            "side": side,
            "qty": str(qty),
        }
    except Exception as e:
        return {"error": f"ibkr_futures: {str(e)[:300]}"}


def _execute_ibkr_forex(symbol: str, qty: int, side: str, order_type: str,
                        limit_price: Optional[float]) -> Dict[str, Any]:
    """Execute forex trade via IBKR ib_async on IDEALPRO."""
    try:
        from ib_async import IB, Contract, MarketOrder, LimitOrder
    except ImportError:
        return {"error": "ib_async_not_installed"}

    base = symbol[:3].upper()
    quote = symbol[3:].upper() if len(symbol) == 6 else "USD"

    try:
        ib = IB()
        host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
        port = int(os.getenv("IB_GATEWAY_PORT", "4001"))
        ib.connect(host, port, clientId=int(os.getenv("IB_CLIENT_ID_FOREX", "4")))

        contract = Contract(secType="CASH", symbol=base, exchange="IDEALPRO", currency=quote)
        ib.qualifyContracts(contract)

        if order_type == "limit" and limit_price:
            order = LimitOrder("BUY" if side == "buy" else "SELL", abs(qty), float(limit_price))
        else:
            order = MarketOrder("BUY" if side == "buy" else "SELL", abs(qty))

        trade = ib.placeOrder(contract, order)
        ib.sleep(2)
        ib.disconnect()

        return {
            "status": "submitted",
            "broker": "ibkr_forex",
            "order_id": str(trade.order.orderId),
            "order_status": trade.orderStatus.status,
            "symbol": symbol,
            "sec_type": "CASH",
            "pair": f"{base}/{quote}",
            "side": side,
            "qty": str(qty),
        }
    except Exception as e:
        return {"error": f"ibkr_forex: {str(e)[:300]}"}


def _execute_coinbase(symbol: str, qty: int, side: str, order_type: str,
                      limit_price: Optional[float], notional: Optional[float] = None) -> Dict[str, Any]:
    """Execute crypto trade via Coinbase Advanced Trade SDK."""
    import uuid

    client = _get_coinbase_client()
    if not client:
        return {"error": "coinbase_sdk_missing_or_no_credentials"}

    sym = symbol.upper().replace("/", "-")
    if not sym.endswith("-USD"):
        sym = f"{sym}-USD" if not sym.endswith("USD") else f"{sym[:-3]}-USD"

    client_order_id = str(uuid.uuid4())

    try:
        if order_type == "limit" and limit_price:
            result = client.limit_order_gtc(
                client_order_id=client_order_id,
                product_id=sym,
                side=side.upper(),
                base_size=str(abs(qty)) if qty else "0",
                limit_price=str(limit_price),
            )
        elif notional and notional > 0:
            result = client.market_order(
                client_order_id=client_order_id,
                product_id=sym,
                side=side.upper(),
                quote_size=str(round(notional, 2)),
            )
        else:
            result = client.market_order(
                client_order_id=client_order_id,
                product_id=sym,
                side=side.upper(),
                base_size=str(abs(qty)),
            )

        if isinstance(result, dict):
            success = result.get("success_response", result.get("success", {}))
            err = result.get("error_response", {})
            if success or result.get("success"):
                return {
                    "status": "submitted",
                    "broker": "coinbase",
                    "order_id": (success or {}).get("order_id", ""),
                    "product_id": sym,
                    "side": side,
                    "qty": str(qty),
                    "notional": str(notional) if notional else None,
                }
            elif err:
                return {"error": f"coinbase_rejected: {err.get('message', str(err)[:200])}"}

        return {
            "status": "submitted",
            "broker": "coinbase",
            "product_id": sym,
            "side": side,
            "qty": str(qty),
            "raw": str(result)[:300],
        }
    except Exception as e:
        return {"error": f"coinbase: {str(e)[:300]}"}


def _execute_tastytrade_forex(symbol: str, qty: int, side: str, order_type: str,
                              limit_price: Optional[float]) -> Dict[str, Any]:
    """Execute FX trade via TastyTrade FX account."""
    try:
        from tastytrade import Session, Account

        username = ENV.get("TASTYTRADE_USERNAME", "") or os.getenv("TASTYTRADE_USERNAME", "")
        password = ENV.get("TASTYTRADE_PASSWORD", "") or os.getenv("TASTYTRADE_PASSWORD", "")
        session = Session(username, password)
        accounts = Account.get_accounts(session)

        acct = next((a for a in accounts if a.account_number == TASTYTRADE_CASH_ACCOUNT), None)
        if not acct:
            acct = accounts[0] if accounts else None
        if not acct:
            return {"error": "tastytrade_no_accounts"}

        pair = symbol.upper()
        if len(pair) == 6:
            pair = f"{pair[:3]}/{pair[3:]}"

        order_payload = {
            "time-in-force": "Day",
            "order-type": "Limit" if order_type == "limit" and limit_price else "Market",
            "legs": [{
                "instrument-type": "Forex",
                "symbol": pair,
                "action": "Buy to Open" if side == "buy" else "Sell to Close",
                "quantity": str(abs(qty)),
            }],
        }
        if order_type == "limit" and limit_price:
            order_payload["price"] = str(limit_price)

        resp = acct.place_order(session, order_payload)
        return {
            "status": "submitted",
            "broker": "tastytrade_fx",
            "account": acct.account_number,
            "symbol": pair,
            "side": side,
            "qty": str(qty),
            "order_response": str(resp)[:300],
        }
    except ImportError:
        return {"error": "tastytrade_sdk_not_installed"}
    except Exception as e:
        return {"error": f"tastytrade_fx: {str(e)[:300]}"}


def _execute_tastytrade_crypto_option(symbol: str, qty: int, side: str, order_type: str,
                                      limit_price: Optional[float]) -> Dict[str, Any]:
    """Execute crypto option trade via TastyTrade (BTC/ETH options)."""
    try:
        from tastytrade import Session, Account
        from tastytrade.order import (NewOrder, OrderAction, OrderTimeInForce,
                                       OrderType, PriceEffect)

        username = ENV.get("TASTYTRADE_USERNAME", "") or os.getenv("TASTYTRADE_USERNAME", "")
        password = ENV.get("TASTYTRADE_PASSWORD", "") or os.getenv("TASTYTRADE_PASSWORD", "")
        session = Session(username, password)
        accounts = Account.get_accounts(session)

        acct = next((a for a in accounts if a.account_number == TASTYTRADE_CASH_ACCOUNT), None)
        if not acct:
            acct = accounts[0] if accounts else None
        if not acct:
            return {"error": "tastytrade_no_accounts"}

        action = OrderAction.BUY_TO_OPEN if side == "buy" else OrderAction.SELL_TO_CLOSE

        from tastytrade.instruments import CryptocurrencyOption
        instrument = CryptocurrencyOption.get_cryptocurrency_option(session, symbol)

        if order_type == "limit" and limit_price:
            order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.LIMIT,
                price=float(limit_price),
                price_effect=PriceEffect.DEBIT if side == "buy" else PriceEffect.CREDIT,
                legs=[instrument.build_leg(abs(qty), action)],
            )
        else:
            order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.MARKET,
                legs=[instrument.build_leg(abs(qty), action)],
            )

        response = acct.place_order(session, order)
        return {
            "status": "submitted",
            "broker": "tastytrade_crypto_option",
            "account": acct.account_number,
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "order_response": str(response)[:300],
        }
    except ImportError as ie:
        return {"error": f"tastytrade_sdk: {str(ie)[:200]}"}
    except Exception as e:
        return {"error": f"tastytrade_crypto_option: {str(e)[:300]}"}


def _execute_kalshi(symbol: str, qty: int, side: str, order_type: str,
                    limit_price: Optional[float]) -> Dict[str, Any]:
    """Execute event contract trade on Kalshi. Symbol is the market ticker."""
    try:
        path = "/trade-api/v2/portfolio/orders"
        headers = _kalshi_auth_headers("POST", path)
        if not headers:
            return {"error": "kalshi_auth_failed"}

        order_payload: Dict[str, Any] = {
            "ticker": symbol,
            "action": "buy" if side == "buy" else "sell",
            "side": "yes",
            "count": abs(qty),
            "type": "limit" if order_type == "limit" and limit_price else "market",
        }
        if order_type == "limit" and limit_price:
            order_payload["yes_price"] = int(limit_price * 100)

        body = json.dumps(order_payload).encode()
        req = urllib.request.Request(
            f"{KALSHI_TRADING_API}/portfolio/orders",
            data=body, method="POST")
        for k, v in headers.items():
            req.add_header(k, v)
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())

        order = result.get("order", {})
        return {
            "status": "submitted",
            "broker": "kalshi",
            "order_id": order.get("order_id", ""),
            "ticker": symbol,
            "side": side,
            "count": str(qty),
            "order_status": order.get("status", ""),
        }
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else str(e)
        return {"error": f"kalshi_http_{e.code}", "details": err_body[:300]}
    except Exception as e:
        return {"error": f"kalshi: {str(e)[:300]}"}


# ---------------------------------------------------------------------------
# Main entry point: route_and_execute
# ---------------------------------------------------------------------------

def route_and_execute(symbol: str, qty: int, side: str, order_type: str = "market",
                      limit_price: Optional[float] = None,
                      broker_override: Optional[str] = None,
                      asset_class: str = "auto",
                      is_day_trade: bool = False,
                      expiration: Optional[str] = None,
                      alpaca_account: str = "paper_dt",
                      time_in_force: str = "day",
                      notional: Optional[float] = None) -> Dict[str, Any]:
    """
    Main entry point: route a trade to the best broker and execute it.

    Parameters:
        symbol: Ticker or OCC option symbol
        qty: Number of shares/contracts
        side: "buy" or "sell"
        order_type: "market" or "limit"
        limit_price: Required for limit orders
        broker_override: Force a specific broker (skips routing logic)
        asset_class: "auto", "us_equity", "us_option", "crypto", "forex", etc.
        is_day_trade: True if planning to close same day
        expiration: Option expiration date (YYYY-MM-DD)
        alpaca_account: "paper_dt", "paper_ml", or "live"
        time_in_force: "day", "gtc"
        notional: Dollar amount (alternative to qty for Alpaca)

    Returns: dict with status, broker, order details, routing reason
    """
    refresh_all_health()

    bp_needed = 0
    if notional:
        bp_needed = notional
    elif limit_price:
        bp_needed = abs(qty) * limit_price

    decision = select_broker(
        symbol=symbol, qty=qty, side=side, order_type=order_type,
        asset_class=asset_class, is_day_trade=is_day_trade,
        expiration=expiration, min_buying_power=bp_needed,
        broker_override=broker_override, alpaca_account=alpaca_account,
    )

    trade_params = {
        "symbol": symbol, "qty": qty, "side": side, "order_type": order_type,
        "limit_price": limit_price, "asset_class": asset_class,
        "is_day_trade": is_day_trade, "expiration": expiration,
        "alpaca_account": alpaca_account, "notional": notional,
    }

    _log_routing_decision(decision, trade_params)

    broker = decision.get("broker")
    if not broker:
        logger.warning("NO BROKER AVAILABLE for %s %s %s", side, qty, symbol)
        return {**decision, "execution": {"error": "no_broker_available"}}

    logger.info("ROUTING %s %d %s -> %s (%s)", side, qty, symbol, broker, decision["reason"])

    exec_result: Dict[str, Any] = {}
    try:
        if broker == "alpaca":
            exec_result = _execute_alpaca(
                symbol, qty, side, order_type, limit_price,
                account=alpaca_account, time_in_force=time_in_force,
                notional=notional)
        elif broker == "tastytrade":
            cat = classify_trade(symbol, qty, side, order_type, asset_class,
                                 is_day_trade, expiration)
            if asset_class == "forex" or cat == "forex":
                exec_result = _execute_tastytrade_forex(
                    symbol, qty, side, order_type, limit_price)
            elif asset_class == "crypto_option" or cat == "crypto_option":
                exec_result = _execute_tastytrade_crypto_option(
                    symbol, qty, side, order_type, limit_price)
            else:
                ac = "us_option" if "option" in cat else "us_equity"
                tt_account = TASTYTRADE_CASH_ACCOUNT if is_day_trade else TASTYTRADE_MARGIN_ACCOUNT
                exec_result = _execute_tastytrade(
                    symbol, qty, side, order_type, limit_price,
                    asset_class=ac, time_in_force=time_in_force,
                    preferred_account=tt_account)
        elif broker == "ibkr":
            exec_result = _execute_ibkr(
                symbol, qty, side, order_type, limit_price,
                asset_class=asset_class)
        elif broker == "coinbase":
            exec_result = _execute_coinbase(
                symbol, qty, side, order_type, limit_price,
                notional=notional)
        elif broker == "kalshi":
            exec_result = _execute_kalshi(
                symbol, qty, side, order_type, limit_price)
        else:
            exec_result = {"error": f"unknown_broker: {broker}"}
    except Exception as e:
        exec_result = {"error": f"execution_exception: {str(e)[:300]}"}

    # Failover if primary execution failed
    if exec_result.get("error") and not broker_override:
        category = decision["category"]
        preferred = ROUTING_TABLE.get(category, [])
        for fallback in preferred:
            if fallback == broker:
                continue
            h = _broker_health.get(fallback, BrokerHealth())
            if not h.connected:
                continue
            logger.warning("FAILOVER from %s to %s for %s %s", broker, fallback, side, symbol)
            try:
                if fallback == "alpaca":
                    exec_result = _execute_alpaca(
                        symbol, qty, side, order_type, limit_price,
                        account=alpaca_account, time_in_force=time_in_force,
                        notional=notional)
                elif fallback == "tastytrade":
                    cat = classify_trade(symbol, qty, side, order_type, asset_class,
                                         is_day_trade, expiration)
                    if asset_class == "forex" or cat == "forex":
                        exec_result = _execute_tastytrade_forex(
                            symbol, qty, side, order_type, limit_price)
                    elif asset_class == "crypto_option" or cat == "crypto_option":
                        exec_result = _execute_tastytrade_crypto_option(
                            symbol, qty, side, order_type, limit_price)
                    else:
                        ac = "us_option" if "option" in cat else "us_equity"
                        exec_result = _execute_tastytrade(
                            symbol, qty, side, order_type, limit_price,
                            asset_class=ac, time_in_force=time_in_force)
                elif fallback == "ibkr":
                    exec_result = _execute_ibkr(
                        symbol, qty, side, order_type, limit_price,
                        asset_class=asset_class)
                elif fallback == "coinbase":
                    exec_result = _execute_coinbase(
                        symbol, qty, side, order_type, limit_price,
                        notional=notional)
                elif fallback == "kalshi":
                    exec_result = _execute_kalshi(
                        symbol, qty, side, order_type, limit_price)

                if not exec_result.get("error"):
                    decision["failover"] = True
                    decision["failover_from"] = broker
                    decision["broker"] = fallback
                    decision["reason"] = f"FAILOVER from {broker}: {decision['reason']}"
                    _log_routing_decision(decision, trade_params)
                    break
            except Exception:
                continue

    result = {**decision, "execution": exec_result}
    logger.info("RESULT: %s -> %s", broker, exec_result.get("status", exec_result.get("error", "unknown")))
    return result


# ---------------------------------------------------------------------------
# Convenience wrappers for paper_trade_mirror / conditional_order_engine
# ---------------------------------------------------------------------------

def route_alpaca_order(base: str, key: str, secret: str, method: str, path: str,
                       data: Optional[Dict] = None, account_label: str = "paper_dt") -> Optional[Dict]:
    """
    Drop-in replacement for direct alpaca_request() calls in paper_trade_mirror.
    Routes through smart router when placing orders, passes through for data calls.
    """
    if method == "POST" and "/v2/orders" in path and data:
        symbol = data.get("symbol", "")
        side = data.get("side", "buy")
        qty_str = data.get("qty", "0")
        notional_str = data.get("notional", "")
        otype = data.get("type", "market")
        limit_px = float(data["limit_price"]) if data.get("limit_price") else None
        tif = data.get("time_in_force", "day")

        qty = int(qty_str) if qty_str else 0
        notional_val = float(notional_str) if notional_str else None

        acct = "paper_dt"
        if key == ENV.get("ALPACA_API_KEY_MEDLONG", ""):
            acct = "paper_ml"
        elif key == ENV.get("ALPACA_API_KEY_LIVE", ""):
            acct = "live"

        result = route_and_execute(
            symbol=symbol, qty=qty, side=side, order_type=otype,
            limit_price=limit_px, alpaca_account=acct,
            time_in_force=tif, notional=notional_val,
        )

        if result.get("execution", {}).get("status") == "submitted":
            return result.get("execution", {})
        elif result.get("execution", {}).get("error"):
            logger.error("Routed order failed: %s", result["execution"]["error"])
            return None
        return None

    # Non-order requests pass through directly
    url = f"{base}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("APCA-API-KEY-ID", key)
    req.add_header("APCA-API-SECRET-KEY", secret)
    if body:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode() if e.fp else str(e)
        logger.warning("API error %d: %s", e.code, err[:200])
        return None
    except Exception as e:
        logger.warning("Request error: %s", e)
        return None


def route_conditional_order(order: Dict[str, Any], equity: float) -> bool:
    """
    Drop-in for conditional_order_engine._submit_order().
    Routes flatten/buy/sell through the smart router.
    """
    symbol = order.get("symbol", "")
    action = order.get("action", "")
    account = order.get("account", "daytrade")
    notional_pct = order.get("notional_pct", 5.0)

    live_allowed = str(ENV.get("ALPACA_ALLOW_LIVE", "")).strip().lower() in {"1", "true", "yes", "on"}
    acct = "live" if (account == "daytrade" and live_allowed) else ("paper_dt" if account == "daytrade" else "paper_ml")

    if action == "flatten":
        result = _execute_alpaca_flatten(symbol, account=acct)
        ok = result.get("status") in ("flattened", "no_position")
        if ok:
            logger.info("CONDITIONAL FLATTEN %s on %s", symbol, acct)
        return ok

    elif action in ("buy", "sell_short"):
        raw_notional = round(max(equity, 0) * max(float(notional_pct), 0.0) / 100.0, 2)
        notional_floor = 25.0
        notional_cap = round(max(equity * 0.95, notional_floor), 2) if equity > 0 else notional_floor
        notional_val = round(min(max(raw_notional, notional_floor), notional_cap), 2)
        side = "buy" if action == "buy" else "sell"

        result = route_and_execute(
            symbol=symbol, qty=0, side=side, order_type="market",
            alpaca_account=acct, notional=notional_val,
            is_day_trade=(account == "daytrade"),
        )
        ok = result.get("execution", {}).get("status") == "submitted"
        if ok:
            logger.info("CONDITIONAL %s %s $%.0f on %s via %s",
                        action.upper(), symbol, notional_val, acct,
                        result.get("broker", "?"))
        else:
            logger.error("CONDITIONAL ORDER FAILED: %s %s -- %s",
                         action, symbol, result.get("execution", {}).get("error", "?"))
        return ok

    return False


# ---------------------------------------------------------------------------
# Daily routing report
# ---------------------------------------------------------------------------

def generate_daily_report() -> Dict[str, Any]:
    """Generate daily routing summary from the JSONL log."""
    today_str = date.today().isoformat()
    broker_counts: Dict[str, int] = {}
    failover_events: List[Dict] = []
    total_commission: float = 0
    total_commission_saved: float = 0
    entries_today: int = 0

    try:
        if ROUTING_LOG_PATH.exists():
            for line in ROUTING_LOG_PATH.read_text().splitlines():
                try:
                    entry = json.loads(line)
                    ts = entry.get("timestamp", "")
                    if not ts.startswith(today_str):
                        continue
                    entries_today += 1
                    dec = entry.get("decision", {})
                    broker = dec.get("broker", "unknown")
                    broker_counts[broker] = broker_counts.get(broker, 0) + 1

                    comm = dec.get("commission_est", 0)
                    total_commission += comm

                    cat = dec.get("category", "")
                    qty = entry.get("trade", {}).get("qty", 1)
                    worst_comm = max(
                        estimate_commission(b, cat, qty)
                        for b in BROKERS
                    )
                    total_commission_saved += (worst_comm - comm)

                    if dec.get("failover"):
                        failover_events.append({
                            "timestamp": ts,
                            "from": dec.get("failover_from"),
                            "to": broker,
                            "symbol": entry.get("trade", {}).get("symbol", ""),
                        })
                except (json.JSONDecodeError, KeyError):
                    continue
    except Exception as e:
        logger.error("Failed to read routing log: %s", e)

    report = {
        "report_date": today_str,
        "generated_at": iso_now(),
        "total_trades_routed": entries_today,
        "broker_distribution": broker_counts,
        "total_commission_est": round(total_commission, 2),
        "commission_savings_est": round(total_commission_saved, 2),
        "failover_events": failover_events,
        "failover_count": len(failover_events),
        "broker_health": {k: v.to_dict() for k, v in _broker_health.items()},
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2))
    logger.info("Daily report: %d trades, %s distribution, $%.2f commission, $%.2f saved, %d failovers",
                entries_today, broker_counts, total_commission, total_commission_saved, len(failover_events))
    return report


# ---------------------------------------------------------------------------
# Legacy API compatibility
# ---------------------------------------------------------------------------

def route_trade(trade_type: str, asset_class: str, is_day_trade: bool,
                size_usd: float) -> Dict[str, Any]:
    """Legacy route_trade() API -- kept for backward compatibility."""
    refresh_all_health()
    balances = get_broker_balances()
    candidates = []

    for name, config in BROKERS.items():
        bal = balances.get(name, {})
        if "error" in bal:
            continue
        if is_day_trade and not config.unlimited_day_trades:
            if bal.get("day_trades_remaining", 0) <= 0:
                continue
        if bal.get("buying_power", 0) < size_usd:
            continue

        score = config.priority * -1
        if trade_type.replace("buy_", "").replace("sell_", "") + "s" in config.best_for:
            score += 10
        if is_day_trade and "day_trade_options" in config.best_for:
            score += 20
        if "0dte" in str(config.best_for) and is_day_trade:
            score += 15
        commission = config.options_commission if "option" in trade_type else config.stock_commission
        score -= commission

        candidates.append({
            "broker": name,
            "score": score,
            "buying_power": bal.get("buying_power", 0),
            "day_trades": bal.get("day_trades_remaining", 0),
            "commission": commission,
            "unlimited_day_trades": config.unlimited_day_trades,
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    result = {
        "timestamp": iso_now(),
        "trade_request": {
            "type": trade_type,
            "asset_class": asset_class,
            "is_day_trade": is_day_trade,
            "size_usd": size_usd,
        },
        "recommended_broker": candidates[0] if candidates else None,
        "all_candidates": candidates,
        "routing_reason": "",
    }
    if candidates:
        best = candidates[0]
        if best["unlimited_day_trades"] and is_day_trade:
            result["routing_reason"] = f"{best['broker']} selected: unlimited day trades (cash account)"
        elif best["score"] > 0:
            result["routing_reason"] = f"{best['broker']} selected: best score ({best['score']})"
        else:
            result["routing_reason"] = f"{best['broker']} selected: only available option"
    else:
        result["routing_reason"] = "No broker available for this trade"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, indent=2))
    return result


# ---------------------------------------------------------------------------
# Daemon mode
# ---------------------------------------------------------------------------

def daemon_loop(interval_sec: int = 300):
    """Run as daemon: health-check all brokers every interval_sec."""
    logger.info("=== Multi-Broker Router Daemon Starting (interval=%ds) ===", interval_sec)
    last_report_date = ""

    while True:
        try:
            health = refresh_all_health(force=True)
            connected = sum(1 for k, v in health.items()
                           if v.get("connected") and k in ("alpaca", "tastytrade", "ibkr"))
            logger.info("Health check: %d/3 brokers connected", connected)

            now_utc = datetime.now(timezone.utc)
            et_offset = timedelta(hours=-4)
            now_et = now_utc + et_offset
            today_str = now_et.strftime("%Y-%m-%d")

            if now_et.hour >= 16 and now_et.minute >= 30 and today_str != last_report_date:
                generate_daily_report()
                last_report_date = today_str

        except Exception:
            logger.error("Daemon error:\n%s", traceback.format_exc())

        time.sleep(interval_sec)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Multi-Broker Smart Router")
    parser.add_argument("command", nargs="?", default="health",
                        choices=["health", "route", "report", "daemon", "test"],
                        help="Command to run")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--side", default="buy")
    parser.add_argument("--order-type", default="market")
    parser.add_argument("--limit-price", type=float, default=None)
    parser.add_argument("--broker", default=None, help="Force specific broker")
    parser.add_argument("--day-trade", action="store_true")
    parser.add_argument("--asset-class", default="auto")
    parser.add_argument("--expiration", default=None)
    parser.add_argument("--interval", type=int, default=300, help="Daemon interval seconds")
    args = parser.parse_args()

    if args.command == "health":
        print("Checking all broker connections...")
        health = refresh_all_health(force=True)
        print(json.dumps(health, indent=2, default=str))

    elif args.command == "route":
        print(f"Routing: {args.side} {args.qty} {args.symbol}")
        result = route_and_execute(
            symbol=args.symbol, qty=args.qty, side=args.side,
            order_type=args.order_type, limit_price=args.limit_price,
            broker_override=args.broker, asset_class=args.asset_class,
            is_day_trade=args.day_trade, expiration=args.expiration,
        )
        print(json.dumps(result, indent=2, default=str))

    elif args.command == "report":
        report = generate_daily_report()
        print(json.dumps(report, indent=2))

    elif args.command == "daemon":
        daemon_loop(interval_sec=args.interval)

    elif args.command == "test":
        print("=== Smart Router Test Suite ===\n")

        print("1. Health check...")
        health = refresh_all_health(force=True)
        for name, h in health.items():
            status = "OK" if h.get("connected") else f"DOWN ({h.get('error', '?')})"
            print(f"   {name}: {status} (equity={h.get('equity', 0):.0f}, bp={h.get('buying_power', 0):.0f})")

        print("\n2. Route tests...")
        tests = [
            {"label": "0DTE SPY put", "symbol": "SPY", "qty": 1, "side": "buy",
             "asset_class": "us_option", "is_day_trade": True, "expiration": date.today().isoformat()},
            {"label": "Weekly NVDA call", "symbol": "NVDA", "qty": 2, "side": "buy",
             "asset_class": "us_option", "expiration": (date.today() + timedelta(days=5)).isoformat()},
            {"label": "Overnight AAPL stock", "symbol": "AAPL", "qty": 10, "side": "buy"},
            {"label": "Crypto BTC/USD", "symbol": "BTC/USD", "qty": 1, "side": "buy", "asset_class": "crypto"},
            {"label": "Forex EUR/USD", "symbol": "EURUSD", "qty": 10000, "side": "buy", "asset_class": "forex"},
            {"label": "Futures ES", "symbol": "ES", "qty": 1, "side": "buy", "asset_class": "futures"},
            {"label": "Crypto option BTC", "symbol": "BTC 260425C100000", "qty": 1, "side": "buy", "asset_class": "crypto_option"},
            {"label": "Event contract FED", "symbol": "FED-26APR-T4.50", "qty": 10, "side": "buy", "asset_class": "event_contract"},
        ]
        for t in tests:
            label = t.pop("label")
            decision = select_broker(order_type="market", **t)
            print(f"   {label} -> {decision['broker']} ({decision['reason'][:60]})")

        print("\n3. Daily report...")
        report = generate_daily_report()
        print(f"   Trades today: {report['total_trades_routed']}")
        print(f"   Distribution: {report['broker_distribution']}")
        print(f"   Commission: ${report['total_commission_est']:.2f}")
        print(f"   Savings: ${report['commission_savings_est']:.2f}")
        print(f"   Failovers: {report['failover_count']}")

        print("\n=== Done ===")

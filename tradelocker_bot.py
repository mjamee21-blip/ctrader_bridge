#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TradeLocker Telegram Bot + Dashboard (Fully Enhanced)
# ALL ORIGINAL FEATURES PRESERVED + ADDITIONAL HELPFUL FEATURES
# 
# Features:
#   ✅ Connection status for TradeLocker AND Telegram
#   ✅ Login authentication (username/password from GitHub secrets)
#   ✅ Heartbeat & Cron job status monitoring (running, completed, failed, idle)
#   ✅ Backend process logs with detailed error information
#   ✅ Real-time dashboard with auto-refresh every 60 seconds
#   ✅ Shows SL/TP and P&L results for each trade from live TradeLocker data
#   ✅ Places MARKET orders immediately — ignores any price/REF in signal
#   ✅ Accurate SL/TP placement from signal
#   ✅ SL update handling (adjusts existing position SL)
#   ✅ Instrument ID resolution (pair name → numeric tradableInstrumentId)
#   ✅ Dashboard shows: balance, equity, margin, open positions, closed trades, orders
#   ✅ Statistics: Win rate, total P&L, open positions count
#   ✅ Risk metrics: Margin level, free margin, margin usage %
#   ✅ Trade performance chart data
#   ✅ Error tracking and alerting
#   ✅ "Fetch Now" button to manually trigger bot/dashboard
#   ✅ GitHub Actions workflow status tracking
#   ✅ Performance history (last 7 days)
#   ✅ Environmental health status checks
#   ✅ Cache-busting to prevent stale dashboard data

import os, json, re, urllib.request, urllib.parse, sys, hashlib, hmac, base64
from urllib.error import HTTPError
from datetime import datetime, timezone, timedelta
import ssl

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# =====================================================================
# CONFIG FROM GITHUB SECRETS
# =====================================================================
TL_EMAIL = os.environ.get("TL_EMAIL", "")
TL_PASSWORD = os.environ.get("TL_PASSWORD", "")
TL_SERVER = os.environ.get("TL_SERVER", "TradeLocker-Demo")
TL_ACCOUNT_ID = int(os.environ.get("TL_ACCOUNT_ID", "0")) if os.environ.get("TL_ACCOUNT_ID", "0").strip() else 0
TL_ACC_NUM = int(os.environ.get("TL_ACC_NUM", "1")) if os.environ.get("TL_ACC_NUM", "1").strip() else 1
TL_ENV = os.environ.get("TL_ENV", "demo")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")
TL_PAIR_MAP_JSON = os.environ.get("TL_PAIR_MAP", "{}")
DEFAULT_QTY = float(os.environ.get("TL_DEFAULT_QTY", "1.0").strip()) if os.environ.get("TL_DEFAULT_QTY", "").strip() else 1.0
MODE = os.environ.get("MODE", "bot")  # "bot" or "dashboard"

# GitHub info for the "Fetch Now" button
GH_OWNER = os.environ.get("GH_OWNER", "")
GH_REPO = os.environ.get("GH_REPO", "")
GH_WORKFLOW = os.environ.get("GH_WORKFLOW", "tradelocker.yml")
GH_TOKEN = os.environ.get("GH_TOKEN", "")

# Dashboard Login Credentials (from GitHub secrets)
DASHBOARD_USERNAME = os.environ.get("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "changeme")

try:
    PAIR_MAP = json.loads(TL_PAIR_MAP_JSON)
except:
    PAIR_MAP = {}

# Common pair aliases — maps signal names to TradeLocker instrument names
PAIR_ALIASES = {
    "GOLD": "XAUUSD", "XAU": "XAUUSD",
    "SILVER": "XAGUSD", "XAG": "XAGUSD",
    "OIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL",
    "BRENT": "UKOIL",
    "NAS100": "NAS100", "NASDAQ": "NAS100", "US100": "NAS100", "NQ100": "NAS100",
    "US30": "US30", "DOW": "US30", "DJ30": "US30",
    "SPX500": "SPX500", "SP500": "SPX500", "US500": "SPX500",
    "GER40": "GER40", "DAX": "GER40", "DE40": "GER40",
    "FRA40": "FRA40", "CAC": "FRA40",
    "UK100": "UK100", "FTSE": "UK100",
    "JPN225": "JPN225", "NIKKEI": "JPN225",
    "HK50": "HK50", "HSI": "HK50",
    "AUS200": "AUS200", "ASX": "AUS200",
}

TL_BASE = "https://live.tradelocker.com" if TL_ENV.lower() == "live" else "https://demo.tradelocker.com"
_last_update_id = 0
_instruments = {}
_process_logs = []  # Store process logs for dashboard
_heartbeat_log = {}  # Store heartbeat info
_statistics = {}  # Store statistics
_alerts = []  # Store alerts/errors

# Unique build version for cache-busting (prevents stale dashboard)
_BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

# =====================================================================
# PROCESS LOGGING, HEARTBEAT & STATISTICS SYSTEM
# =====================================================================

def log_process(level, message):
    """Log process events for dashboard display."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = {
        "timestamp": timestamp,
        "level": level,  # "info", "success", "error", "warning"
        "message": message
    }
    _process_logs.append(log_entry)
    print(f"[{level.upper()}] {timestamp} - {message}")
    
    # Keep only last 150 logs
    if len(_process_logs) > 150:
        _process_logs.pop(0)
    
    # Add errors and warnings to alerts
    if level in ["error", "warning"]:
        _alerts.append({
            "timestamp": timestamp,
            "level": level,
            "message": message
        })
        # Keep only last 50 alerts
        if len(_alerts) > 50:
            _alerts.pop(0)

def save_heartbeat(job_name, status, details=""):
    """Save heartbeat for cron job status."""
    timestamp = datetime.now(timezone.utc).isoformat()
    _heartbeat_log[job_name] = {
        "status": status,  # "running", "completed", "failed", "idle"
        "timestamp": timestamp,
        "details": details
    }
    
    # Save to file for persistence across runs
    os.makedirs("docs", exist_ok=True)
    heartbeat_file = os.path.join("docs", "heartbeat.json")
    try:
        with open(heartbeat_file, "w") as f:
            json.dump(_heartbeat_log, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not save heartbeat: {e}")

def load_heartbeat():
    """Load heartbeat data from previous runs."""
    global _heartbeat_log
    heartbeat_file = os.path.join("docs", "heartbeat.json")
    try:
        if os.path.exists(heartbeat_file):
            with open(heartbeat_file, "r") as f:
                _heartbeat_log = json.load(f)
    except Exception as e:
        print(f"[WARNING] Could not load heartbeat: {e}")

def save_statistics(stats_data):
    """Save statistics for performance tracking."""
    os.makedirs("docs", exist_ok=True)
    stats_file = os.path.join("docs", "statistics.json")
    try:
        with open(stats_file, "w") as f:
            json.dump(stats_data, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not save statistics: {e}")

def load_statistics():
    """Load statistics from previous runs."""
    stats_file = os.path.join("docs", "statistics.json")
    try:
        if os.path.exists(stats_file):
            with open(stats_file, "r") as f:
                return json.load(f)
    except:
        pass
    return {"trades": 0, "wins": 0, "losses": 0, "total_pnl": 0, "win_rate": 0}

def get_job_status(job_name):
    """Get current job status with time since last run."""
    if job_name not in _heartbeat_log:
        return {"status": "idle", "message": "No data", "time_ago": "never", "raw_status": "idle"}
    
    hb = _heartbeat_log[job_name]
    status = hb.get("status", "unknown")
    timestamp = hb.get("timestamp", "")
    details = hb.get("details", "")
    
    # Calculate time since last run
    time_ago = "unknown"
    if timestamp:
        try:
            last_run = datetime.fromisoformat(timestamp)
            now = datetime.now(timezone.utc)
            delta = now - last_run.replace(tzinfo=timezone.utc)
            
            if delta.total_seconds() < 60:
                time_ago = f"{int(delta.total_seconds())}s ago"
            elif delta.total_seconds() < 3600:
                time_ago = f"{int(delta.total_seconds() / 60)}m ago"
            elif delta.total_seconds() < 86400:
                time_ago = f"{int(delta.total_seconds() / 3600)}h ago"
            else:
                time_ago = f"{int(delta.total_seconds() / 86400)}d ago"
        except:
            time_ago = timestamp
    
    message = details if details else ("No errors" if status == "completed" else "Processing...")
    
    return {
        "status": status,
        "message": message,
        "time_ago": time_ago,
        "timestamp": timestamp,
        "raw_status": status
    }

# =====================================================================
# AUTHENTICATION HELPER
# =====================================================================

def hash_password(password):
    """Hash password using SHA-256 for secure storage."""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_credentials(username, password):
    """Verify login credentials against secrets."""
    return (username == DASHBOARD_USERNAME and 
            password == DASHBOARD_PASSWORD)

# =====================================================================
# HELPER: Extract list or dict from API response
# =====================================================================

def _extract_data(result, *keys):
    """
    Robustly extract data from TradeLocker API responses.
    Tries multiple common key paths to find the actual data.
    """
    # Direct list/dict
    if isinstance(result, list):
        return result
    if isinstance(result, dict) and result and not keys:
        return result

    # Try each key in order
    for key in keys:
        if key == "list" or key == "items" or key == "positions" or key == "trades" or key == "orders":
            val = result.get(key)
            if isinstance(val, list):
                return val
        else:
            val = result.get(key)
            if val is not None:
                if isinstance(val, (dict, list)):
                    return val

    # Try the "d" wrapper key (common in TL API)
    d = result.get("d")
    if d is not None:
        if isinstance(d, (list, dict)):
            return d
        # "d" might contain nested data
        if isinstance(d, dict):
            for key in keys:
                val = d.get(key)
                if isinstance(val, (list, dict)):
                    return val
            # Return d itself if no sub-key matched
            return d

    # Try "data" key
    data = result.get("data")
    if data is not None:
        if isinstance(data, (list, dict)):
            return data

    # Try "result" key
    res = result.get("result")
    if res is not None:
        if isinstance(res, (list, dict)):
            return res

    # Try "body" key
    body = result.get("body")
    if body is not None:
        if isinstance(body, str):
            try:
                parsed = json.loads(body)
                if isinstance(parsed, (list, dict)):
                    return parsed
            except:
                pass
        elif isinstance(body, (list, dict)):
            return body

    return {}

def _extract_list(result, *keys):
    """Extract a list from an API response, trying multiple paths."""
    data = _extract_data(result, *keys)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Try to find a list inside the dict
        for k, v in data.items():
            if isinstance(v, list):
                return v
    return []

def _get_nested(d, *keys, default=None):
    """Safely get a nested value from a dict."""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current if current is not None else default

# =====================================================================
# TRADELOCKER API CLIENT
# =====================================================================

class TradeLockerClient:
    def __init__(self):
        self.base_url = f"{TL_BASE}/backend-api"
        self.token = None
        self.authenticated = False

    def _req(self, method, path, body=None, headers_extra=None, timeout=20):
        url = f"{self.base_url}{path}"
        headers = {"User-Agent": "TLBot/2.0", "Accept": "application/json"}
        if body:
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if headers_extra:
            headers.update(headers_extra)

        try:
            data = json.dumps(body).encode() if body else None
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content = resp.read().decode().strip()
                return json.loads(content) if content else {}
        except HTTPError as e:
            try:
                err_body = e.read().decode().strip()
                log_process("warning", f"HTTP {e.code} on {method} {path}: {err_body[:200]}")
                return {"error": "http_error", "status": e.code, "body": err_body}
            except:
                log_process("warning", f"HTTP {e.code} on {method} {path}: no body")
                return {"error": "http_error", "status": e.code}
        except Exception as ex:
            log_process("warning", f"Request failed on {method} {path}: {str(ex)}")
            return {"error": "request_failed", "details": str(ex)}

    def auth(self):
        """Authenticate with TradeLocker and store the JWT token."""
        if not TL_EMAIL or not TL_PASSWORD:
            log_process("error", "TL_EMAIL or TL_PASSWORD not set")
            return False
            
        payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}
        result = self._req("POST", "/auth/jwt/token", body=payload)
        if result.get("error"):
            log_process("error", f"TradeLocker auth failed: {result}")
            return False
        self.token = result.get("accessToken") or result.get("access_token") or result.get("token")
        self.authenticated = bool(self.token)
        if self.authenticated:
            log_process("success", f"TradeLocker authenticated on {TL_SERVER}")
        return self.authenticated

    def load_instruments(self):
        """
        Load ALL instruments from TradeLocker and build a name→ID mapping.
        """
        global _instruments
        _instruments = {}

        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/instruments",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            log_process("warning", f"Failed to load instruments: {result}")
            return False

        instruments = _extract_list(result, "instruments", "data", "items")

        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = (inst.get("name") or inst.get("symbol") or "").upper().strip()
            inst_id = inst.get("tradableInstrumentId") or inst.get("id")
            route_id = None
            for route in (inst.get("routes") or []):
                if route.get("type") == "TRADE":
                    route_id = route.get("id")
                    break
            if name and inst_id is not None:
                _instruments[name] = {"id": inst_id, "route_id": route_id}

        log_process("info", f"Loaded {len(_instruments)} instruments from TradeLocker")
        return len(_instruments) > 0

    def find_instrument(self, pair_name):
        """Resolve pair name to numeric instrument ID."""
        if not pair_name:
            return None

        normalized = pair_name.replace("/", "").replace(" ", "").upper().strip()

        # 1. Check user-defined PAIR_MAP first
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in _instruments:
            return _instruments[mapped]

        # 2. Check common aliases
        alias = PAIR_ALIASES.get(normalized, "")
        if alias and alias in _instruments:
            return _instruments[alias]

        # 3. Exact match
        if normalized in _instruments:
            return _instruments[normalized]

        # 4. Fuzzy match (contains)
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                return info

        log_process("warning", f"Could not find instrument for '{pair_name}'")
        return None

    def get_quote(self, inst_id):
        """Get current bid/ask quote for an instrument."""
        qs = f"?tradableInstrumentId={inst_id}"
        result = self._req("GET", f"/trade/quotes{qs}", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return None
        d = _extract_data(result, "quotes")
        if isinstance(d, list) and d:
            q = d[0]
        elif isinstance(d, dict):
            q = d
        else:
            q = _extract_data(result, "d")
            if isinstance(q, dict):
                pass
            elif isinstance(q, list) and q:
                q = q[0]
            else:
                return None
        
        bp = q.get("bp") or q.get("bid")
        ap = q.get("ap") or q.get("ask")
        return {"bp": float(bp), "ap": float(ap)} if bp and ap else None

    def place_order(self, pair, direction, sl, tp, qty=None):
        """Place a MARKET order immediately."""
        if not qty:
            qty = DEFAULT_QTY

        log_process("info", f"Placing {direction} market order for {pair} | Qty: {qty} | SL: {sl} | TP: {tp}")

        instrument = self.find_instrument(pair)
        if not instrument:
            log_process("info", "Instrument not found, reloading instruments...")
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            log_process("error", f"Cannot resolve instrument ID for '{pair}'")
            return False

        inst_id = instrument["id"]
        route_id = instrument.get("route_id")
        side = "buy" if direction.upper() == "BUY" else "sell"

        payload = {
            "tradableInstrumentId": inst_id,
            "type": "market",
            "validity": "IOC",
            "side": side,
            "qty": qty,
        }
        if sl is not None:
            payload["stopLoss"] = float(sl)
        if tp is not None:
            payload["takeProfit"] = float(tp)
        if route_id:
            payload["routeId"] = str(route_id)

        result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders",
                           body=payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)

        if result.get("error") or result.get("s") == "error":
            errmsg = result.get("errmsg") or result.get("body") or result.get("error") or ""
            log_process("error", f"Market order failed: {errmsg}")

            if "forbidden" in str(errmsg).lower() and "route" in str(errmsg).lower():
                log_process("info", "Route forbidden — trying limit order near market price...")
                quote = self.get_quote(inst_id)
                if quote:
                    mid = (quote["bp"] + quote["ap"]) / 2.0
                    offset = max(mid * 0.0015, 0.5)
                    limit_price = round(quote["ap"] + offset, 5) if side == "buy" else round(quote["bp"] - offset, 5)
                    lim_payload = {
                        "tradableInstrumentId": inst_id,
                        "type": "limit",
                        "side": side,
                        "qty": qty,
                        "price": limit_price,
                    }
                    if sl is not None:
                        lim_payload["stopLoss"] = float(sl)
                    if tp is not None:
                        lim_payload["takeProfit"] = float(tp)
                    if route_id:
                        lim_payload["routeId"] = str(route_id)

                    result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders",
                                       body=lim_payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)

                    if result.get("error") or result.get("s") == "error":
                        log_process("error", f"Limit order also failed: {result}")
                        return False
                    log_process("success", f"Limit order placed at {limit_price}")
                    return True
            return False

        log_process("success", f"Market order placed successfully!")
        return True

    def get_open_positions(self):
        """Get all open positions with their SL/TP and P&L."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/positions",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            log_process("warning", f"get_open_positions error: {result.get('error')}")
            return []
        
        log_process("info", f"get_open_positions: result_type={type(result).__name__}, result_keys={list(result.keys()) if isinstance(result, dict) else 'N/A'}")
        
        # Get the data wrapper
        data = result.get("d", result)
        if isinstance(data, dict):
            log_process("info", f"get_open_positions: d_keys={list(data.keys())}")
            
            # Positions might be a dict (positionId -> data)
            pos_data = data.get("positions") or data.get("data") or data.get("items")
            
            if isinstance(pos_data, dict):
                # Positions returned as dict with position IDs as keys
                positions = []
                for pos_id, pos_obj in pos_data.items():
                    if isinstance(pos_obj, dict):
                        # Add positionId to the object if not present
                        if "positionId" not in pos_obj and "id" not in pos_obj:
                            pos_obj["positionId"] = pos_id
                        positions.append(pos_obj)
                    elif isinstance(pos_obj, str) and pos_obj.strip().startswith("{"):
                        try:
                            parsed = json.loads(pos_obj)
                            if isinstance(parsed, dict):
                                if "positionId" not in parsed:
                                    parsed["positionId"] = pos_id
                                positions.append(parsed)
                        except:
                            pass
                log_process("info", f"get_open_positions: found {len(positions)} positions from dict")
                return positions
            
            elif isinstance(pos_data, list):
                return self._parse_position_list(pos_data)
        
        # If data is a list directly
        if isinstance(data, list):
            return self._parse_position_list(data)
        
        return []
    
    def _parse_position_list(self, items):
        """Parse a list of position items that might be in various formats."""
        positions = []
        log_process("info", f"_parse_position_list: {len(items)} items")
        
        for item in items:
            if isinstance(item, dict):
                positions.append(item)
            elif isinstance(item, str):
                # Try to parse as JSON
                try:
                    parsed = json.loads(item)
                    if isinstance(parsed, dict):
                        positions.append(parsed)
                    elif isinstance(parsed, list):
                        positions.extend(parsed)
                except:
                    # Could be a position ID - try to get full data
                    log_process("info", f"  position string (not JSON): {item[:100]}")
            elif item is not None:
                log_process("info", f"  position item type={type(item).__name__}: {str(item)[:100]}")
        
        if positions:
            log_process("info", f"get_open_positions: parsed {len(positions)} positions, first keys={list(positions[0].keys())}")
            log_process("info", f"get_open_positions: first pos sample={json.dumps(positions[0], default=str)[:400]}")
        else:
            log_process("warning", f"get_open_positions: no valid positions found from {len(items)} items")
        
        return positions

    def close_position(self, pos_id):
        """Close an open position by ID."""
        result = self._req("DELETE", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        success = not result.get("error")
        log_process("success" if success else "error", f"Close position {pos_id}: {'Success' if success else 'Failed'}")
        return success

    def modify_position(self, pos_id, pos, new_sl):
        """Update the SL of an existing position."""
        log_process("info", f"Updating SL for position {pos_id} → new SL: {new_sl}")

        payload = {"stopLoss": float(new_sl)}
        if isinstance(pos, dict):
            qty = pos.get("qty") or pos.get("quantity") or pos.get("size") or 1.0
            tp = pos.get("takeProfit") or pos.get("tp")
            try:
                payload["qty"] = float(qty)
            except:
                payload["qty"] = qty
            if tp:
                try:
                    payload["takeProfit"] = float(tp)
                except:
                    payload["takeProfit"] = tp

        result = self._req("PUT", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}",
                           body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            result = self._req("PATCH", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}",
                               body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})

        success = not result.get("error")
        log_process("success" if success else "error", f"SL update: {'Success' if success else 'Failed'}")
        return success

    def get_account_state(self):
        """Get account balance, equity, margin, etc."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/state",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        
        if result.get("error"):
            log_process("warning", f"get_account_state error: {result.get('error')}")
            return {}
        
        # The API may nest data like: {"d": {"accountDetailsData": {balance, equity, ...}}}
        # or: {"d": {"state": {balance, ...}}}
        # We need to find the innermost dict with actual account data
        
        # Step 1: Get the first level of data
        data = _extract_data(result, "d", "data", "result")
        if not isinstance(data, dict):
            data = result
        
        log_process("info", f"get_account_state: level1_keys={list(data.keys())}")
        
        # Step 2: Try to go one level deeper to find actual account data
        for key in ["accountDetailsData", "state", "account", "accountState", "details"]:
            if key in data and isinstance(data[key], dict):
                inner = data[key]
                log_process("info", f"get_account_state: found '{key}' with keys={list(inner.keys())}")
                return inner
        
        # If no nested dict found, use the level1 data itself
        # (but if it only has 'accountDetailsData' as a key, extract that)
        if len(data) == 1:
            single_key = list(data.keys())[0]
            if isinstance(data[single_key], dict):
                log_process("info", f"get_account_state: extracted single key '{single_key}' with keys={list(data[single_key].keys())}")
                return data[single_key]
        
        log_process("info", f"get_account_state: using top-level data with keys={list(data.keys())}")
        return data

    def get_orders(self):
        """Get recent order history (up to 20)."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/orders?limit=50",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        
        if result.get("error"):
            log_process("warning", f"get_orders error: {result.get('error')}")
            return []
        
        orders = _extract_list(result, "orders", "data", "items")
        log_process("info", f"get_orders: got {len(orders)} orders")
        return orders[:20]

    def get_trade_history(self):
        """Get closed trade history for SL/TP results."""
        # Try trades endpoint first, fall back to closedPositions
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/trades?limit=50",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            log_process("info", f"trades endpoint not available ({result.get('error')}), trying closedPositions...")
            # Try alternative endpoint for closed positions
            result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/closedPositions?limit=50",
                               headers_extra={"accNum": str(TL_ACC_NUM)})
            if result.get("error"):
                log_process("warning", f"get_trade_history error: {result.get('error')}")
                return []
        
        trades = _extract_list(result, "trades", "closedPositions", "data", "items")
        log_process("info", f"get_trade_history: got {len(trades)} trades")
        return trades

# =====================================================================
# TELEGRAM
# =====================================================================

def test_telegram_connection():
    """Test if the Telegram bot is reachable."""
    if not TG_TOKEN:
        return False, {"error": "No TG_TOKEN set"}
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                bot_info = result.get("result", {})
                return True, {
                    "username": bot_info.get("username", "unknown"),
                    "name": bot_info.get("first_name", "unknown"),
                    "id": bot_info.get("id", "unknown")
                }
            return False, {"error": "API returned not ok"}
    except Exception as e:
        return False, {"error": str(e)}

def tg_get_messages(offset=0):
    """Fetch new messages from Telegram."""
    global _last_update_id
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset+1}&timeout=3&limit=100"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            if not result.get("ok"):
                return []
            messages = []
            for upd in result.get("result", []):
                uid = upd.get("update_id", 0)
                if uid > _last_update_id:
                    _last_update_id = uid
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat = msg.get("chat", {})
                chat_id = str(chat.get("id", ""))
                chat_uname = chat.get("username", "")
                target = str(TG_CHAT).lstrip("@")
                if TG_CHAT != "ANY" and target != chat_uname and target != chat_id:
                    continue
                text = msg.get("text") or msg.get("caption") or ""
                if text:
                    messages.append({"text": text})
            return messages
    except Exception as ex:
        log_process("warning", f"Error fetching Telegram messages: {ex}")
        return []

# =====================================================================
# SIGNAL PARSER
# =====================================================================

def looks_like_signal(text):
    """Check if text looks like a trading signal."""
    if not text:
        return False
    t = text.upper()
    return ("BUY" in t or "SELL" in t or "TP HIT" in t or
            "SL HIT" in t or "SL_UPDATE" in t or "SL UPDATE" in t)

def parse_signal(text):
    """Parse a trading signal from Telegram."""
    if not text:
        return None
    lines = text.strip().split("\n")
    first = lines[0].strip().upper()

    # --- Handle TP/SL Hit signals ---
    if "TP HIT" in first or "SL HIT" in first:
        pair = re.search(r"(TP|SL)\s*HIT\s*[-–:]\s*(\S+)", first, re.IGNORECASE)
        pair_str = pair.group(2) if pair else ""
        if not pair_str:
            for line in lines[1:]:
                m = re.search(r"([A-Z]{3,8}[/]?[A-Z]{0,8})", line.strip())
                if m:
                    pair_str = m.group(1)
                    break
        result = "TP" if "TP HIT" in first else "SL"
        log_process("info", f"TPSL_HIT: {result} on {pair_str}")
        return {"type": "TPSL_HIT", "result": result, "pair": pair_str}

    # --- Handle SL Update signals ---
    if "#SL_UPDATE" in text.upper() or "SL UPDATE" in text.upper():
        pair, new_sl = None, None
        for line in lines:
            if "PAIR" in line.upper() and ":" in line:
                pair = line.split(":", 1)[1].strip()
            m = re.search(r"(?:New\s*)?SL\s*[:=]\s*([\d.]+)", line, re.IGNORECASE)
            if m:
                try:
                    new_sl = float(m.group(1))
                except:
                    pass
        if pair and new_sl:
            log_process("info", f"SL_UPDATE: {pair} → new SL {new_sl}")
            return {"type": "SL_UPDATE", "pair": pair, "new_sl": new_sl}
        return None

    # --- Handle BUY/SELL/CLOSE signals ---
    sig = re.search(r"\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)", first, re.IGNORECASE)
    if not sig:
        return None

    direction = sig.group(1).upper()
    pair = sig.group(2).upper()
    sl = tp = None

    for line in lines:
        cl = re.sub(r"<[^>]+>", "", line).strip()
        m = re.search(r"(?<![A-Za-z])SL\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and sl is None:
            try:
                sl = float(m.group(1))
            except:
                pass
        m = re.search(r"(?<![A-Za-z])TP\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and tp is None:
            try:
                tp = float(m.group(1))
            except:
                pass

    log_process("info", f"SIGNAL: {direction} {pair} | SL: {sl} | TP: {tp}")
    return {"type": "SIGNAL", "direction": direction, "pair": pair, "sl": sl, "tp": tp}

# =====================================================================
# DASHBOARD GENERATOR (COMPLETE WITH ALL FEATURES)
# =====================================================================

def create_login_with_verification():
    """Generate login page with credential verification."""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>TradeLocker Dashboard - Login</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
            background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
            color: #c9d1d9; 
            font-size: 14px;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            padding: 20px;
        }}
        .login-container {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 40px;
            max-width: 400px;
            width: 100%;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }}
        .login-header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .login-header h1 {{
            font-size: 28px;
            margin-bottom: 8px;
            color: #58a6ff;
        }}
        .login-header p {{
            color: #8b949e;
            font-size: 12px;
        }}
        .form-group {{
            margin-bottom: 20px;
        }}
        .form-group label {{
            display: block;
            font-size: 12px;
            color: #8b949e;
            margin-bottom: 8px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .form-group input {{
            width: 100%;
            padding: 12px;
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 6px;
            color: #c9d1d9;
            font-size: 14px;
            transition: border-color 0.2s;
        }}
        .form-group input:focus {{
            outline: none;
            border-color: #58a6ff;
        }}
        .form-group input::placeholder {{
            color: #6e7681;
        }}
        .login-btn {{
            width: 100%;
            padding: 12px;
            background: #238636;
            color: #fff;
            border: none;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }}
        .login-btn:hover {{
            background: #2ea043;
        }}
        .login-btn:active {{
            background: #238636;
        }}
        .login-btn:disabled {{
            background: #6e7681;
            cursor: not-allowed;
        }}
        .error-message {{
            display: none;
            background: #f85149;
            color: #fff;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-size: 12px;
            animation: slideDown 0.3s ease-out;
        }}
        .error-message.show {{
            display: block;
        }}
        .success-message {{
            display: none;
            background: #3fb950;
            color: #fff;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-size: 12px;
            animation: slideDown 0.3s ease-out;
        }}
        .success-message.show {{
            display: block;
        }}
        @keyframes slideDown {{
            from {{
                opacity: 0;
                transform: translateY(-10px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        .login-footer {{
            margin-top: 20px;
            text-align: center;
            color: #6e7681;
            font-size: 11px;
        }}
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1>🚀 Dashboard</h1>
            <p>TradeLocker Bot Control Panel</p>
        </div>

        <div class="error-message" id="errorMsg"></div>
        <div class="success-message" id="successMsg"></div>

        <form onsubmit="handleLogin(event)">
            <div class="form-group">
                <label>Username</label>
                <input type="text" id="username" placeholder="Enter username" required autofocus>
            </div>

            <div class="form-group">
                <label>Password</label>
                <input type="password" id="password" placeholder="Enter password" required>
            </div>

            <button type="submit" class="login-btn" id="loginBtn">Sign In</button>
        </form>

        <div class="login-footer">
            <p>🔒 Credentials verified against GitHub Secrets</p>
            <p style="margin-top: 10px; color: #8b949e;">Default: admin / changeme</p>
        </div>
    </div>

    <script>
        const CORRECT_USERNAME = "{DASHBOARD_USERNAME}";
        const CORRECT_PASSWORD = "{DASHBOARD_PASSWORD}";

        function handleLogin(event) {{
            event.preventDefault();
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const errorMsg = document.getElementById('errorMsg');
            const successMsg = document.getElementById('successMsg');
            const loginBtn = document.getElementById('loginBtn');

            errorMsg.classList.remove('show');
            successMsg.classList.remove('show');

            if (username === '' || password === '') {{
                errorMsg.textContent = '❌ Please enter both username and password';
                errorMsg.classList.add('show');
                return;
            }}

            // Verify credentials
            if (username === CORRECT_USERNAME && password === CORRECT_PASSWORD) {{
                loginBtn.disabled = true;
                loginBtn.textContent = 'Signing in...';
                
                successMsg.textContent = '✅ Login successful! Redirecting...';
                successMsg.classList.add('show');

                // Store authentication in sessionStorage (session-based, more secure)
                sessionStorage.setItem('dashboard_authenticated', 'true');
                sessionStorage.setItem('dashboard_username', username);
                sessionStorage.setItem('dashboard_login_time', new Date().toISOString());

                // Redirect to dashboard after a brief delay
                setTimeout(function() {{
                    window.location.href = 'index.html?v={_BUILD_VERSION}';
                }}, 1000);
            }} else {{
                errorMsg.textContent = '❌ Invalid username or password';
                errorMsg.classList.add('show');
                document.getElementById('password').value = '';
                document.getElementById('password').focus();
            }}
        }}

        // Check if already authenticated
        window.addEventListener('load', function() {{
            const auth = sessionStorage.getItem('dashboard_authenticated');
            if (auth === 'true') {{
                window.location.href = 'index.html?v={_BUILD_VERSION}';
            }}
            document.getElementById('username').focus();
        }});

        // Auto-clear error message after 5 seconds
        document.getElementById('errorMsg').addEventListener('DOMNodeInserted', function() {{
            setTimeout(() => {{
                this.classList.remove('show');
            }}, 5000);
        }});
    </script>
</body>
</html>"""
    return html

def generate_login_html():
    """Generate the login page HTML."""
    html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>TradeLocker Dashboard - Login</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
            background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
            color: #c9d1d9; 
            font-size: 14px;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            padding: 20px;
        }
        .login-container {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 40px;
            max-width: 400px;
            width: 100%;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }
        .login-header {
            text-align: center;
            margin-bottom: 30px;
        }
        .login-header h1 {
            font-size: 28px;
            margin-bottom: 8px;
            color: #58a6ff;
        }
        .login-header p {
            color: #8b949e;
            font-size: 12px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            font-size: 12px;
            color: #8b949e;
            margin-bottom: 8px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .form-group input {
            width: 100%;
            padding: 12px;
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 6px;
            color: #c9d1d9;
            font-size: 14px;
            transition: border-color 0.2s;
        }
        .form-group input:focus {
            outline: none;
            border-color: #58a6ff;
        }
        .form-group input::placeholder {
            color: #6e7681;
        }
        .login-btn {
            width: 100%;
            padding: 12px;
            background: #238636;
            color: #fff;
            border: none;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }
        .login-btn:hover {
            background: #2ea043;
        }
        .login-btn:active {
            background: #238636;
        }
        .error-message {
            display: none;
            background: #f85149;
            color: #fff;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-size: 12px;
            animation: slideDown 0.3s ease-out;
        }
        .error-message.show {
            display: block;
        }
        @keyframes slideDown {
            from {
                opacity: 0;
                transform: translateY(-10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        .login-footer {
            margin-top: 20px;
            text-align: center;
            color: #6e7681;
            font-size: 11px;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1>🚀 Dashboard</h1>
            <p>TradeLocker Bot Control Panel</p>
        </div>

        <div class="error-message" id="errorMsg"></div>

        <form onsubmit="handleLogin(event)">
            <div class="form-group">
                <label>Username</label>
                <input type="text" id="username" placeholder="Enter username" required autofocus>
            </div>

            <div class="form-group">
                <label>Password</label>
                <input type="password" id="password" placeholder="Enter password" required>
            </div>

            <button type="submit" class="login-btn">Sign In</button>
        </form>

        <div class="login-footer">
            <p>🔒 Credentials are verified against GitHub Secrets</p>
        </div>
    </div>

    <script>
        function handleLogin(event) {
            event.preventDefault();
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const errorMsg = document.getElementById('errorMsg');

            if (username === '' || password === '') {
                errorMsg.textContent = '❌ Please enter both username and password';
                errorMsg.classList.add('show');
                return;
            }

            sessionStorage.setItem('dashboard_auth_attempt', JSON.stringify({
                username: username,
                timestamp: new Date().toISOString()
            }));

            location.href = '#dashboard?token=' + btoa(username + ':' + password);
            location.reload();
        }

        window.onload = function() {
            const auth = sessionStorage.getItem('dashboard_authenticated');
            if (auth === 'true') {
                location.href = '?authenticated=true';
            }
        };
    </script>
</body>
</html>"""
    return html

# =====================================================================
# FLEXIBLE FIELD RESOLVER FOR TRADELOCKER API
# =====================================================================

def _resolve_field(data, *candidates, default="N/A"):
    """
    Try multiple possible field names to extract a value from the API response.
    Returns the first non-None value found.
    """
    if not isinstance(data, dict):
        return default
    for key in candidates:
        val = data.get(key)
        if val is not None and val != "":
            return val
    return default

def _safe_float(val, default=0):
    """Safely convert a value to float."""
    if val is None or val == "N/A":
        return default
    try:
        return float(str(val).replace('$', '').replace(',', ''))
    except:
        return default

def _safe_currency(val):
    """Format a value as currency."""
    try:
        f = float(str(val).replace('$', '').replace(',', ''))
        return f"${f:,.2f}"
    except:
        return f"${val}"

# =====================================================================
# DASHBOARD GENERATOR
# =====================================================================

def generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info):
    """Generate the full dashboard HTML with all features."""

    state = client.get_account_state() if tl_connected else {}
    positions = client.get_open_positions() if tl_connected else []
    orders = client.get_orders() if tl_connected else []
    trades = client.get_trade_history() if tl_connected else []

    log_process("info", f"Dashboard data: state_keys={list(state.keys()) if state else 'EMPTY'}, "
                         f"positions={len(positions)}, orders={len(orders)}, trades={len(trades)}")
    
    # Debug: dump first position keys for debugging
    if positions and isinstance(positions[0], dict):
        log_process("info", f"First position keys: {list(positions[0].keys())}")
        log_process("info", f"First position: {json.dumps(positions[0], default=str)[:500]}")

    # --- Account state with flexible field resolution ---
    # Dump all state keys/values for debugging
    log_process("info", f"Account state raw: {json.dumps(state, default=str)[:500] if state else 'EMPTY'}")
    
    balance_raw = _resolve_field(state, "balance", "accountBalance", "Balance", "totalBalance", "equity", 
                                  "totalBalanceInAccountCurrency", "accountBalanceInAccountCurrency", 
                                  "balanceInAccountCurrency", "cashBalance", "availableBalance")
    equity_raw = _resolve_field(state, "equity", "Equity", "accountEquity", "equityValue", 
                                 "totalEquity", "equityInAccountCurrency", "currentEquity")
    margin_raw = _resolve_field(state, "usedMargin", "margin", "used_margin", "Margin", 
                                 "marginInAccountCurrency", "usedMarginInAccountCurrency", "initialMargin")
    free_margin_raw = _resolve_field(state, "freeMargin", "free_margin", "FreeMargin", "availableMargin", 
                                      "freeMarginInAccountCurrency", "availableFunds", "freeEquity")
    margin_level_raw = _resolve_field(state, "marginLevel", "margin_level", "MarginLevel", 
                                       "marginLevelPercent", "marginCallLevel")
    daypl_raw = _resolve_field(state, "dayPL", "dayPl", "dailyPnL", "dailyPL", "day_pl", "pnl", 
                                "dailyProfitLoss", "todayPL", "todayPnL", "unrealizedPL", "unrealizedPnL",
                                "floatingPL", "floatingPnL")
    currency_raw = _resolve_field(state, "currency", "accountCurrency", "Currency", 
                                   "accountCurrencyCode", "baseCurrency", "settlementCurrency", default="USD")

    # If balance is still N/A but we have some numeric-looking keys, try to find them
    if balance_raw == "N/A" and isinstance(state, dict):
        for k, v in state.items():
            if k.lower() in ("balance", "accountbalance", "totalbalance", "totalbalanceinaccountcurrency") and v is not None:
                balance_raw = v
                break

    account_state = {
        'balance': _safe_currency(balance_raw) if balance_raw != "N/A" else "N/A",
        'equity': _safe_currency(equity_raw) if equity_raw != "N/A" else "N/A",
        'margin': _safe_currency(margin_raw) if margin_raw != "N/A" else "N/A",
        'free_margin': _safe_currency(free_margin_raw) if free_margin_raw != "N/A" else "N/A",
        'margin_level': f"{margin_level_raw}%" if margin_level_raw != "N/A" else "N/A%",
        'currency': str(currency_raw),
        'daypl': _safe_currency(daypl_raw) if daypl_raw != "N/A" else "$0",
        'account_id': TL_ACCOUNT_ID,
        'server': TL_SERVER,
    }

    # --- Calculate margin usage percentage ---
    try:
        margin_usage = 0
        used = _safe_float(margin_raw)
        free = _safe_float(free_margin_raw)
        total = used + free
        if total > 0:
            margin_usage = (used / total) * 100
    except:
        margin_usage = 0

    # --- Open positions with SL/TP and P&L ---
    positions_data = []
    total_pnl = 0

    # Debug: log raw positions data structure
    if positions:
        first_type = type(positions[0]).__name__
        log_process("info", f"Positions: {len(positions)} items, first item type={first_type}")
        if isinstance(positions[0], dict):
            log_process("info", f"First position keys: {list(positions[0].keys())}")
            log_process("info", f"First position sample: {json.dumps(positions[0], default=str)[:500]}")
        elif isinstance(positions[0], list):
            # Positions might be nested: [[pos1], [pos2], ...]
            log_process("info", f"Positions appear to be nested lists, flattening...")
            flat = []
            for item in positions:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            positions = flat
            log_process("info", f"Flattened to {len(positions)} positions")
            if positions and isinstance(positions[0], dict):
                log_process("info", f"First position keys: {list(positions[0].keys())}")
                log_process("info", f"First position sample: {json.dumps(positions[0], default=str)[:500]}")
        elif isinstance(positions[0], str):
            # Positions might be JSON strings
            log_process("info", f"Positions appear to be JSON strings, parsing...")
            flat = []
            for item in positions:
                if isinstance(item, str):
                    try:
                        parsed = json.loads(item)
                        if isinstance(parsed, list):
                            flat.extend(parsed)
                        elif isinstance(parsed, dict):
                            flat.append(parsed)
                    except:
                        pass
                else:
                    flat.append(item)
            positions = flat
            log_process("info", f"Parsed to {len(positions)} positions")

    for pos in positions:
        if not isinstance(pos, dict):
            log_process("warning", f"Skipping non-dict position item: type={type(pos).__name__}")
            continue
        try:

            pos_time = pos.get('openTime') or pos.get('open_time') or pos.get('created') or ''
            if isinstance(pos_time, (int, float)):
                pos_time = datetime.fromtimestamp(pos_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            
            # Flexible field resolution for P&L
            pnl_val = _safe_float(pos.get('pnl') or pos.get('profit') or pos.get('floatingPL') or 
                                  pos.get('floatingPnL') or pos.get('pl') or pos.get('currentPL') or 
                                  pos.get('current_pnl') or pos.get('unrealizedPL') or pos.get('unrealizedPnL'))
            total_pnl += pnl_val
            
            positions_data.append({
                'pair': pos.get('instrumentName') or pos.get('symbol') or pos.get('name') or pos.get('instrument_name') or 'N/A',
                'side': str(pos.get('side') or pos.get('direction') or '').upper(),
                'qty': pos.get('qty') or pos.get('quantity') or pos.get('size') or pos.get('volume') or 'N/A',
                'price': pos.get('price') or pos.get('openPrice') or pos.get('open_price') or pos.get('avgPrice') or 'N/A',
                'sl': pos.get('stopLoss') or pos.get('sl') or pos.get('stop_loss') or '—',
                'tp': pos.get('takeProfit') or pos.get('tp') or pos.get('take_profit') or '—',
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
                'time': pos_time
            })
        except Exception as e:
            log_process("warning", f"Error parsing position: {e}")
            continue

    # --- Closed trades (results) ---
    trades_data = []
    wins = 0
    losses = 0
    for tr in trades:
        if not isinstance(tr, dict):
            continue
        try:
            close_time = tr.get('closeTime') or tr.get('close_time') or tr.get('closedAt') or tr.get('closed_at') or tr.get('closedTime') or ''
            if isinstance(close_time, (int, float)):
                close_time = datetime.fromtimestamp(close_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            
            pnl_val = _safe_float(tr.get('pnl') or tr.get('profit') or tr.get('realizedPL') or 
                                  tr.get('realizedPnL') or tr.get('pl') or tr.get('realized_pnl'))
            if pnl_val > 0:
                wins += 1
            elif pnl_val < 0:
                losses += 1
            
            entry = _safe_float(tr.get('openPrice') or tr.get('open_price') or tr.get('price') or tr.get('avgOpenPrice'))
            exit_p = _safe_float(tr.get('closePrice') or tr.get('close_price') or tr.get('exitPrice') or tr.get('avgClosePrice'))
            sl_val = tr.get('stopLoss') or tr.get('sl')
            tp_val = tr.get('takeProfit') or tr.get('tp')
            result_label = "Closed"
            if sl_val is not None and abs(exit_p - _safe_float(sl_val)) < 0.01:
                result_label = "🔴 SL Hit"
            elif tp_val is not None and abs(exit_p - _safe_float(tp_val)) < 0.01:
                result_label = "🟢 TP Hit"
            trades_data.append({
                'pair': tr.get('instrumentName') or tr.get('symbol') or tr.get('name') or 'N/A',
                'side': str(tr.get('side') or tr.get('direction') or '').upper(),
                'entry': entry,
                'exit': exit_p,
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
                'result': result_label,
                'time': close_time
            })
        except Exception as e:
            log_process("warning", f"Error parsing trade: {e}")
            continue

    # --- Calculate statistics ---
    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

    # --- Recent orders ---
    orders_data = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        try:
            order_time = order.get('modifiedAt') or order.get('modified_at') or order.get('placedAt') or order.get('placed_at') or order.get('created') or order.get('createdAt') or ''
            if isinstance(order_time, (int, float)):
                order_time = datetime.fromtimestamp(order_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            orders_data.append({
                'id': str(order.get('orderId') or order.get('order_id') or order.get('id') or '')[:12],
                'pair': order.get('instrumentName') or order.get('symbol') or order.get('name') or 'N/A',
                'side': str(order.get('side') or order.get('direction') or '').upper(),
                'type': order.get('type') or order.get('orderType') or 'N/A',
                'qty': order.get('qty') or order.get('quantity') or order.get('size') or 'N/A',
                'price': order.get('price') or order.get('avgPrice') or order.get('avg_price') or 'N/A',
                'status': order.get('status') or order.get('state') or 'N/A',
                'time': order_time
            })
        except Exception as e:
            log_process("warning", f"Error parsing order: {e}")
            continue

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # --- Connection status ---
    tl_status_color = "#3fb950" if tl_connected else "#f85149"
    tl_status_icon = "✓" if tl_connected else "✗"
    tl_status_text = "Connected" if tl_connected else "Disconnected"
    tl_detail = f"Server: {TL_SERVER} | Env: {TL_ENV}" if tl_connected else (tl_error or "Auth failed")

    tg_status_color = "#3fb950" if tg_connected else "#f85149"
    tg_status_icon = "✓" if tg_connected else "✗"
    tg_status_text = "Connected" if tg_connected else "Disconnected"
    tg_detail = f"Bot: @{tg_info.get('username', 'N/A')}" if tg_connected else tg_info.get('error', 'Unknown error')

    # --- Get cron job status ---
    bot_status = get_job_status("bot")
    dashboard_status = get_job_status("dashboard")

    # --- Health indicators ---
    health_checks = {
        "tl_auth": {"ok": tl_connected, "label": "TradeLocker Auth", "icon": "🔐"},
        "tg_bot": {"ok": tg_connected, "label": "Telegram Bot", "icon": "📱"},
        "instruments": {"ok": len(_instruments) > 0, "label": "Instruments Loaded", "icon": "📊"},
        "margin_safe": {"ok": margin_usage < 80, "label": f"Margin Safe ({margin_usage:.1f}%)", "icon": "⚠️"}
    }

    # --- Build tables ---
    if positions_data:
        positions_table = """<table>
<thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry Price</th><th>Stop Loss</th><th>Take Profit</th><th>P&L</th><th>Open Time</th></tr></thead>
<tbody>"""
        for p in positions_data:
            side_class = "buy" if "buy" in p["side"].lower() else "sell"
            pnl_color = "#3fb950" if p["pnl_value"] >= 0 else "#f85149"
            positions_table += f"""<tr>
<td class="pair">{p['pair']}</td>
<td class="{side_class}">{p['side']}</td>
<td>{p['qty']}</td>
<td>{p['price']}</td>
<td class="sl">{p['sl']}</td>
<td class="tp">{p['tp']}</td>
<td style="color:{pnl_color};font-weight:600;">{p['pnl']}</td>
<td class="time">{p['time']}</td>
</tr>"""
        positions_table += "</tbody></table>"
    else:
        positions_table = '<div class="empty">No open positions</div>'

    if trades_data:
        trades_table = """<table>
<thead><tr><th>Pair</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Result</th><th>Close Time</th></tr></thead>
<tbody>"""
        for t in trades_data:
            side_class = "buy" if "buy" in t["side"].lower() else "sell"
            pnl_color = "#3fb950" if t["pnl_value"] >= 0 else "#f85149"
            trades_table += f"""<tr>
<td class="pair">{t['pair']}</td>
<td class="{side_class}">{t['side']}</td>
<td>{t['entry']}</td>
<td>{t['exit']}</td>
<td style="color:{pnl_color};font-weight:600;">{t['pnl']}</td>
<td>{t['result']}</td>
<td class="time">{t['time']}</td>
</tr>"""
        trades_table += "</tbody></table>"
    else:
        trades_table = '<div class="empty">No closed trades yet</div>'

    if orders_data:
        orders_table = """<table>
<thead><tr><th>Order ID</th><th>Pair</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th><th>Time</th></tr></thead>
<tbody>"""
        for o in orders_data:
            side_class = "buy" if "buy" in o["side"].lower() else "sell"
            orders_table += f"""<tr>
<td class="oid">{o['id']}</td>
<td class="pair">{o['pair']}</td>
<td class="{side_class}">{o['side']}</td>
<td>{o['type']}</td>
<td>{o['qty']}</td>
<td>{o['price']}</td>
<td>{o['status']}</td>
<td class="time">{o['time']}</td>
</tr>"""
        orders_table += "</tbody></table>"
    else:
        orders_table = '<div class="empty">No recent orders</div>'

    # --- Build process logs table ---
    logs_table = ""
    if _process_logs:
        logs_table = """<table>
<thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead>
<tbody>"""
        for log in _process_logs[-30:]:
            level = log.get("level", "info").upper()
            level_color = {
                "INFO": "#58a6ff",
                "SUCCESS": "#3fb950",
                "ERROR": "#f85149",
                "WARNING": "#d29922"
            }.get(level, "#c9d1d9")
            logs_table += f"""<tr>
<td class="time">{log['timestamp']}</td>
<td style="color:{level_color};font-weight:600;">{level}</td>
<td>{log['message']}</td>
</tr>"""
        logs_table += "</tbody></table>"
    else:
        logs_table = '<div class="empty">No logs yet</div>'

    # --- Build alerts ---
    alerts_html = ""
    if _alerts:
        for alert in _alerts[-10:]:
            alert_color = "#f85149" if alert["level"] == "error" else "#d29922"
            alerts_html += f'<div style="padding:8px;margin:5px 0;background:{alert_color}20;border-left:3px solid {alert_color};border-radius:4px;font-size:11px;"><strong>{alert["level"].upper()}</strong> {alert["timestamp"]}: {alert["message"]}</div>'
    else:
        alerts_html = '<div class="empty">No alerts</div>'

    # --- Health check HTML ---
    health_html = ""
    for check_key, check in health_checks.items():
        status_color = "#3fb950" if check["ok"] else "#f85149"
        status_text = "✓ OK" if check["ok"] else "✗ FAILED"
        health_html += f'<div class="health-item"><span class="health-icon">{check["icon"]}</span><span class="health-label">{check["label"]}</span><span class="health-status" style="color:{status_color};">{status_text}</span></div>'

    # --- Cron job status indicators ---
    bot_color = "#3fb950" if bot_status["raw_status"] == "completed" else ("#f85149" if bot_status["raw_status"] == "failed" else "#d29922")
    dashboard_color = "#3fb950" if dashboard_status["raw_status"] == "completed" else ("#f85149" if dashboard_status["raw_status"] == "failed" else "#d29922")

    # --- GitHub settings defaults (use secrets if available) ---
    gh_owner_default = GH_OWNER if GH_OWNER else ""
    gh_repo_default = GH_REPO if GH_REPO else ""
    gh_workflow_default = GH_WORKFLOW if GH_WORKFLOW else "tradelocker.yml"

    # --- JavaScript ---
    js_code = """
<script>
function getGH() {
    return {
        token: localStorage.getItem('gh_token') || '',
        owner: localStorage.getItem('gh_owner') || '""" + gh_owner_default + """',
        repo: localStorage.getItem('gh_repo') || '""" + gh_repo_default + """',
        workflow: localStorage.getItem('gh_workflow') || '""" + gh_workflow_default + """'
    };
}
function openSettings() {
    var s = getGH();
    document.getElementById('gh_token').value = s.token;
    document.getElementById('gh_owner').value = s.owner;
    document.getElementById('gh_repo').value = s.repo;
    document.getElementById('gh_workflow').value = s.workflow;
    document.getElementById('settingsModal').style.display = 'block';
}
function closeSettings() {
    document.getElementById('settingsModal').style.display = 'none';
}
function saveSettings() {
    localStorage.setItem('gh_token', document.getElementById('gh_token').value);
    localStorage.setItem('gh_owner', document.getElementById('gh_owner').value);
    localStorage.setItem('gh_repo', document.getElementById('gh_repo').value);
    localStorage.setItem('gh_workflow', document.getElementById('gh_workflow').value);
    closeSettings();
    alert('Settings saved! Click "Fetch Now" to trigger a refresh.');
}
function fetchNow() {
    var s = getGH();
    if (!s.token || !s.owner || !s.repo) {
        alert('Please configure GitHub settings first.\\nClick the gear icon to set your GitHub token and repo info.');
        openSettings();
        return;
    }
    var btn = document.getElementById('fetchBtn');
    btn.disabled = true;
    btn.innerHTML = '⏳ Triggering...';
    fetch('https://api.github.com/repos/' + s.owner + '/' + s.repo + '/actions/workflows/' + s.workflow + '/dispatches', {
        method: 'POST',
        headers: {
            'Authorization': 'Bearer ' + s.token,
            'Accept': 'application/vnd.github+json',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ ref: 'main' })
    }).then(function(response) {
        if (response.status === 204) {
            btn.innerHTML = '✅ Triggered! Reloading in 30s...';
            setTimeout(function() { location.reload(); }, 30000);
        } else {
            return response.json().then(function(data) {
                throw new Error(data.message || 'Failed (status ' + response.status + ')');
            });
        }
    }).catch(function(error) {
        btn.disabled = false;
        btn.innerHTML = '🔄 Fetch Now';
        alert('Error: ' + error.message);
    });
}
function logout() {
    if (confirm('Are you sure you want to logout?')) {
        sessionStorage.removeItem('dashboard_authenticated');
        sessionStorage.removeItem('dashboard_username');
        sessionStorage.removeItem('dashboard_login_time');
        sessionStorage.clear();
        localStorage.removeItem('gh_token');
        localStorage.removeItem('gh_owner');
        localStorage.removeItem('gh_repo');
        localStorage.removeItem('gh_workflow');
        window.location.href = 'login.html';
    }
}
function checkAuth() {
    const auth = sessionStorage.getItem('dashboard_authenticated');
    if (auth !== 'true') {
        window.location.href = 'login.html';
    }
}

function autoRefresh() {
    setInterval(function() {
        const auth = sessionStorage.getItem('dashboard_authenticated');
        if (auth === 'true') {
            location.reload();
        } else {
            window.location.href = 'login.html';
        }
    }, 60000);
}

document.addEventListener('DOMContentLoaded', function() {
    checkAuth();
    autoRefresh();
    setInterval(function() {
        const auth = sessionStorage.getItem('dashboard_authenticated');
        if (auth !== 'true') {
            window.location.href = 'login.html';
        }
    }, 10000);
});
</script>
"""

    # --- Full HTML with Login Check ---
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="Pragma" content="no-cache">
    <meta http-equiv="Expires" content="0">
    <title>TradeLocker Dashboard</title>
    <script>
        (function() {{
            const auth = sessionStorage.getItem('dashboard_authenticated');
            if (auth !== 'true') {{
                location.href = 'login.html';
            }}
        }})();
    </script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; font-size: 13px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 20px; }}
        .header h1 {{ font-size: 16px; font-weight: 700; }}
        .header-actions {{ display: flex; gap: 8px; align-items: center; }}
        .btn {{ padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; transition: opacity 0.2s; }}
        .btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
        .btn-fetch {{ background: #238636; color: #fff; }}
        .btn-fetch:hover:not(:disabled) {{ background: #2ea043; }}
        .btn-settings {{ background: #30363d; color: #c9d1d9; }}
        .btn-settings:hover {{ background: #484f58; }}
        .btn-logout {{ background: #da3633; color: #fff; }}
        .btn-logout:hover {{ background: #f85149; }}
        .heartbeat {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; animation: pulse 1.5s ease-in-out infinite; }}
        .heartbeat.running {{ background: #d29922; animation: pulse 0.8s ease-in-out infinite; }}
        .heartbeat.completed {{ background: #3fb950; animation: none; }}
        .heartbeat.failed {{ background: #f85149; animation: none; }}
        .heartbeat.idle {{ background: #6e7681; animation: none; }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}
        .conn-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .conn-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }}
        .conn-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .conn-title {{ font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }}
        .conn-badge {{ font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 12px; }}
        .conn-detail {{ font-size: 11px; color: #8b949e; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin-bottom: 15px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 15px; }}
        .stat {{ text-align: center; padding: 15px; }}
        .stat-value {{ font-size: 20px; font-weight: 800; color: #58a6ff; }}
        .stat-label {{ font-size: 10px; color: #8b949e; margin-top: 5px; text-transform: uppercase; letter-spacing: 0.5px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ padding: 10px; background: #0d1117; color: #8b949e; font-size: 10px; text-transform: uppercase; text-align: left; border-bottom: 1px solid #30363d; }}
        td {{ padding: 10px; border-bottom: 1px solid #30363d; }}
        tr:hover td {{ background: #1c2128; }}
        .pair {{ font-weight: 600; }}
        .buy {{ color: #3fb950; }}
        .sell {{ color: #f85149; }}
        .sl {{ color: #f85149; font-weight: 500; }}
        .tp {{ color: #3fb950; font-weight: 500; }}
        .time {{ font-size: 11px; color: #8b949e; }}
        .oid {{ font-size: 10px; color: #8b949e; }}
        .section-title {{ font-size: 11px; color: #8b949e; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin: 20px 0 10px 0; }}
        .empty {{ text-align: center; color: #8b949e; padding: 20px; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 1000; }}
        .modal-content {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; max-width: 500px; margin: 100px auto; }}
        .modal-content h3 {{ margin-bottom: 15px; }}
        .modal-content label {{ display: block; font-size: 11px; color: #8b949e; margin-bottom: 4px; margin-top: 10px; }}
        .modal-content input {{ width: 100%; padding: 8px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-size: 12px; }}
        .modal-actions {{ display: flex; gap: 10px; margin-top: 20px; justify-content: flex-end; }}
        .last-update {{ color: #8b949e; font-size: 11px; text-align: center; margin-top: 20px; }}
        .account-info {{ background: #0d1117; padding: 10px; border-radius: 6px; font-size: 11px; color: #8b949e; margin-bottom: 15px; }}
        .cron-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .cron-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }}
        .cron-title {{ font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 8px; }}
        .cron-status {{ font-size: 10px; color: #8b949e; }}
        .cron-time {{ font-size: 10px; color: #6e7681; margin-top: 4px; }}
        .health-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 15px; }}
        .health-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px; display: flex; align-items: center; gap: 10px; font-size: 11px; }}
        .health-icon {{ font-size: 16px; }}
        .health-label {{ flex: 1; }}
        .health-status {{ font-weight: 600; }}
        .build-version {{ font-size: 9px; color: #6e7681; }}
        .api-debug {{ font-size: 9px; color: #6e7681; background: #0d1117; padding: 8px; border-radius: 4px; font-family: monospace; margin-top: 10px; word-break: break-all; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 TradeLocker Dashboard</h1>
            <div class="header-actions">
                <button class="btn btn-settings" onclick="openSettings()" title="GitHub Settings">⚙️</button>
                <button id="fetchBtn" class="btn btn-fetch" onclick="fetchNow()">🔄 Fetch Now</button>
                <button class="btn btn-logout" onclick="logout()">🚪 Logout</button>
            </div>
        </div>

        <div class="account-info">
            📊 Account ID: {account_state['account_id']} | Server: {account_state['server']} | Currency: {account_state['currency']} | Env: {TL_ENV.upper()}
            <span class="build-version">| Build: {_BUILD_VERSION}</span>
        </div>

        <!-- System Health Status -->
        <div class="section-title">🩺 System Health</div>
        <div class="health-grid">
            {health_html}
        </div>

        <!-- Connection Status -->
        <div class="section-title">Connection Status</div>
        <div class="conn-grid">
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">📊 TradeLocker</span>
                    <span class="conn-badge" style="background:{tl_status_color}20;color:{tl_status_color};">{tl_status_icon} {tl_status_text}</span>
                </div>
                <div class="conn-detail">{tl_detail}</div>
            </div>
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">✈️ Telegram</span>
                    <span class="conn-badge" style="background:{tg_status_color}20;color:{tg_status_color};">{tg_status_icon} {tg_status_text}</span>
                </div>
                <div class="conn-detail">{tg_detail}</div>
            </div>
        </div>

        <!-- Cron Job Status -->
        <div class="section-title">🔄 Cron Job Status</div>
        <div class="cron-grid">
            <div class="cron-card">
                <div class="cron-title">
                    <span class="heartbeat {bot_status['raw_status']}"></span>Trading Bot
                </div>
                <div class="cron-status">{bot_status['raw_status'].upper()}</div>
                <div class="cron-time">{bot_status['time_ago']}</div>
                <div style="font-size:10px;color:#8b949e;margin-top:6px;">{bot_status['message']}</div>
            </div>
            <div class="cron-card">
                <div class="cron-title">
                    <span class="heartbeat {dashboard_status['raw_status']}"></span>Dashboard
                </div>
                <div class="cron-status">{dashboard_status['raw_status'].upper()}</div>
                <div class="cron-time">{dashboard_status['time_ago']}</div>
                <div style="font-size:10px;color:#8b949e;margin-top:6px;">{dashboard_status['message']}</div>
            </div>
        </div>

        <!-- Account Overview -->
        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{account_state['balance']}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">{account_state['equity']}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin']}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['free_margin']}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin_level']}</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">{account_state['daypl']}</div><div class="stat-label">Day P&L</div></div>
        </div>

        <!-- Trade Statistics -->
        <div class="section-title">📈 Trade Statistics</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{total_trades}</div><div class="stat-label">Total Trades</div></div>
            <div class="card stat"><div class="stat-value">{wins}</div><div class="stat-label">Wins</div></div>
            <div class="card stat"><div class="stat-value">{losses}</div><div class="stat-label">Losses</div></div>
            <div class="card stat"><div class="stat-value">{win_rate:.1f}%</div><div class="stat-label">Win Rate</div></div>
            <div class="card stat"><div class="stat-value">${total_pnl:+.2f}</div><div class="stat-label">Open P&L</div></div>
            <div class="card stat"><div class="stat-value">{len(positions_data)}</div><div class="stat-label">Open Positions</div></div>
        </div>

        <!-- Recent Alerts -->
        <div class="section-title">⚠️ Recent Alerts & Errors</div>
        <div class="card">
            {alerts_html}
        </div>

        <!-- Backend Process Logs -->
        <div class="section-title">📋 Backend Process Logs (Last 30)</div>
        <div class="card">{logs_table}</div>

        <!-- Open Positions -->
        <div class="section-title">Open Positions ({len(positions_data)}) — Live SL/TP & P&L</div>
        <div class="card">{positions_table}</div>

        <!-- Closed Trades (Results) -->
        <div class="section-title">Closed Trades ({len(trades_data)}) — Results</div>
        <div class="card">{trades_table}</div>

        <!-- Recent Orders -->
        <div class="section-title">Recent Orders (Last 20)</div>
        <div class="card">{orders_table}</div>

        <!-- API Debug Panel -->
        <div class="section-title">🔍 API Debug (Raw Data)</div>
        <div class="card" style="font-family:monospace;font-size:10px;max-height:300px;overflow:auto;white-space:pre-wrap;word-break:break-all;">
<strong>State keys:</strong> {list(state.keys()) if state else 'EMPTY'}
<strong>State data:</strong> {json.dumps(state, default=str)[:1000] if state else 'EMPTY'}
<strong>Positions:</strong> {len(positions)} items
<strong>First position sample:</strong> {json.dumps(positions[0], default=str)[:500] if positions else 'NONE'}
<strong>Orders:</strong> {len(orders)} items
<strong>Trades:</strong> {len(trades)} items
<strong>Balance resolved:</strong> {balance_raw}
<strong>Equity resolved:</strong> {equity_raw}
        </div>

        <div class="last-update">Last updated: {last_update} | Auto-refresh every 60s | {len(_process_logs)} Total Logs | {len(_alerts)} Alerts | Build: {_BUILD_VERSION}</div>
    </div>

    <!-- Settings Modal -->
    <div id="settingsModal" class="modal">
        <div class="modal-content">
            <h3>⚙️ GitHub API Settings</h3>
            <p style="font-size:11px;color:#8b949e;margin-bottom:10px;">
                Configure these settings to enable the "Fetch Now" button.
                Your token is stored locally in your browser (localStorage) and never sent to our server.
            </p>
            <label>GitHub Personal Access Token (with <code>workflow</code> scope)</label>
            <input type="password" id="gh_token" placeholder="ghp_xxxxxxxxxxxx">
            <label>GitHub Username / Owner</label>
            <input type="text" id="gh_owner" placeholder="mjamee21-blip">
            <label>Repository Name</label>
            <input type="text" id="gh_repo" placeholder="tradelocker_bridge">
            <label>Workflow Filename</label>
            <input type="text" id="gh_workflow" value="tradelocker.yml">
            <div class="modal-actions">
                <button class="btn btn-settings" onclick="closeSettings()">Cancel</button>
                <button class="btn btn-fetch" onclick="saveSettings()">Save</button>
            </div>
        </div>
    </div>

    {js_code}
</body>
</html>"""
    return html

# =====================================================================
# BOT MODE
# =====================================================================

def run_bot():
    """Run the trading bot: fetch Telegram signals → place TradeLocker orders."""
    try:
        save_heartbeat("bot", "running", "Initializing bot...")
        log_process("info", "=== BOT CYCLE STARTED ===")
        
        if not (TL_EMAIL and TL_PASSWORD and TG_TOKEN and TG_CHAT):
            log_process("error", "Missing required secrets (TL_EMAIL, TL_PASSWORD, TG_TOKEN, TG_CHAT)")
            save_heartbeat("bot", "failed", "Missing credentials")
            return False

        client = TradeLockerClient()
        if not client.auth():
            log_process("error", "TradeLocker authentication failed")
            save_heartbeat("bot", "failed", "Auth failed")
            return False

        client.load_instruments()

        messages = tg_get_messages(offset=_last_update_id)
        log_process("info", f"Fetched {len(messages)} new messages from Telegram")

        signal_count = 0
        for msg in messages:
            text = (msg.get("text") or "").strip()
            if not looks_like_signal(text):
                continue

            signal_count += 1
            log_process("info", "=== Processing Signal ===")
            log_process("info", f"Signal: {text[:200]}")

            parsed = parse_signal(text)
            if not parsed:
                log_process("warning", "Could not parse signal, skipping")
                continue

            if parsed["type"] == "SIGNAL":
                client.place_order(
                    pair=parsed["pair"],
                    direction=parsed["direction"],
                    sl=parsed["sl"],
                    tp=parsed["tp"]
                )

            elif parsed["type"] == "TPSL_HIT":
                pair = parsed["pair"]
                result = parsed["result"]
                log_process("info", f"{result} HIT on {pair} — closing position")
                positions = client.get_open_positions()
                for pos in positions:
                    if not isinstance(pos, dict):
                        continue
                    pos_sym = pos.get('instrumentName') or pos.get('symbol') or pos.get('name') or ""
                    if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                        pos_id = pos.get('positionId') or pos.get('id')
                        if pos_id:
                            client.close_position(pos_id)

            elif parsed["type"] == "SL_UPDATE":
                pair = parsed["pair"]
                new_sl = parsed["new_sl"]
                log_process("info", f"SL UPDATE on {pair} → {new_sl}")
                positions = client.get_open_positions()
                for pos in positions:
                    if not isinstance(pos, dict):
                        continue
                    pos_sym = pos.get('instrumentName') or pos.get('symbol') or pos.get('name') or ""
                    if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                        pos_id = pos.get('positionId') or pos.get('id')
                        if pos_id:
                            client.modify_position(pos_id, pos, new_sl)

        log_process("info", f"=== BOT CYCLE COMPLETE === ({signal_count} signals processed)")
        save_heartbeat("bot", "completed", f"Processed {signal_count} signals")
        return True
    except Exception as e:
        log_process("error", f"Bot cycle failed: {str(e)}")
        save_heartbeat("bot", "failed", str(e)[:100])
        import traceback
        traceback.print_exc()
        return False

# =====================================================================
# DASHBOARD MODE
# =====================================================================

def generate_dashboard():
    """Generate the dashboard HTML with connection status and deploy to GitHub Pages."""
    try:
        load_heartbeat()
        save_heartbeat("dashboard", "running", "Generating dashboard...")
        log_process("info", "=== DASHBOARD GENERATION STARTED ===")
        
        print(f"[Dashboard] Starting at {datetime.now(timezone.utc).isoformat()}")
        print(f"[Dashboard] TL_ENV={TL_ENV}, TL_SERVER={TL_SERVER}, TL_ACCOUNT_ID={TL_ACCOUNT_ID}")

        client = TradeLockerClient()

        tl_connected = client.auth()
        tl_error = None
        if not tl_connected:
            tl_error = "Failed to authenticate. Check TL_EMAIL, TL_PASSWORD, TL_SERVER secrets."
            log_process("warning", f"TradeLocker: DISCONNECTED — {tl_error}")
        else:
            log_process("success", "TradeLocker: CONNECTED")
            client.load_instruments()

        tg_connected, tg_info = test_telegram_connection()
        if tg_connected:
            log_process("success", f"Telegram: CONNECTED (@{tg_info.get('username')})")
        else:
            log_process("warning", f"Telegram: DISCONNECTED — {tg_info.get('error')}")

        html = generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info)

        os.makedirs("docs", exist_ok=True)
        output_path = os.path.join("docs", "index.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        # Also create a login page
        login_path = os.path.join("docs", "login.html")
        with open(login_path, "w", encoding="utf-8") as f:
            f.write(create_login_with_verification())

        log_process("success", f"Dashboard written to {output_path}")
        print(f"[Dashboard] Written to {output_path} ({len(html)} bytes)")
        
        log_process("info", "=== DASHBOARD GENERATION COMPLETE ===")
        save_heartbeat("dashboard", "completed", "No errors")
        return True
    except Exception as e:
        log_process("error", f"Dashboard generation failed: {str(e)}")
        save_heartbeat("dashboard", "failed", str(e)[:100])
        import traceback
        traceback.print_exc()
        
        try:
            os.makedirs("docs", exist_ok=True)
            with open("docs/index.html", "w", encoding="utf-8") as f:
                f.write(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>TradeLocker Dashboard</title></head>
<body style="font-family:sans-serif;background:#0d1117;color:#f85149;padding:40px;">
<h1>⚠️ Dashboard Error</h1>
<p>The dashboard generator encountered a fatal error:</p>
<pre style="background:#161b22;padding:15px;border-radius:6px;overflow-x:auto;">{str(e)}</pre>
<p style="color:#8b949e;margin-top:20px;">Check the GitHub Actions logs for details.</p>
</body></html>""")
        except:
            pass
        return False

# =====================================================================
# MAIN
# =====================================================================

if __name__ == "__main__":
    try:
        if MODE == "dashboard":
            generate_dashboard()
        else:
            run_bot()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
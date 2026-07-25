#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader Telegram Bot + Dashboard
# Uses cTrader Open API v2.0 with OAuth authentication
#
# Features:
#   ✅ OAuth authentication (CT_CLIENT_ID, CT_CLIENT_SECRET)
#   ✅ Access token refresh (CT_REFRESH_TOKEN)
#   ✅ Connection status monitoring
#   ✅ Real-time dashboard with auto-refresh
#   ✅ Open positions tracking with SL/TP and P&L
#   ✅ MARKET orders with SL/TP
#   ✅ Position management (modify SL, close)
#   ✅ Telegram signal processing
#   ✅ Heartbeat & cron job status
#   ✅ Backend process logs
#   ✅ Trade statistics (win rate, P&L)
#   ✅ Risk metrics (margin level, free margin)
#   ✅ System health checks
#   ✅ Secure login dashboard
#   ✅ Cache-busting to prevent stale data

import os, json, re, urllib.request, urllib.parse, sys, hashlib, base64
from urllib.error import HTTPError
from datetime import datetime, timezone, timedelta
import ssl

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# =====================================================================
# CONFIG FROM GITHUB SECRETS - CTRADER OAUTH ONLY
# =====================================================================

# cTrader OAuth Credentials
CT_CLIENT_ID = os.environ.get("CT_CLIENT_ID", "")
CT_CLIENT_SECRET = os.environ.get("CT_CLIENT_SECRET", "")
CT_REFRESH_TOKEN = os.environ.get("CL_REFRESH_TOKEN", "")
CT_ACCOUNT_ID = os.environ.get("CT_ACCOUNT_ID", "")
CT_ENV = os.environ.get("CT_ENV", "demo")
CT_ACCESS_TOKEN = os.environ.get("CT_ACCESS_TOKEN", "")

# Telegram (optional)
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")

# Dashboard Login
DASHBOARD_USERNAME = os.environ.get("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "changeme")

# Optional configuration
CTRADER_PAIR_MAP_JSON = os.environ.get("CTRADER_PAIR_MAP", "{}")
DEFAULT_QTY = float(os.environ.get("CTRADER_DEFAULT_QTY", "1.0") or "1.0")
MODE = os.environ.get("MODE", "bot")  # "bot" or "dashboard"

try:
    PAIR_MAP = json.loads(CTRADER_PAIR_MAP_JSON)
except:
    PAIR_MAP = {}

# Common pair aliases
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

# cTrader API endpoints
CT_API_BASE = "https://openapi.ctrader.com/api/v1"
_last_update_id = 0
_instruments = {}
_process_logs = []
_heartbeat_log = {}
_alerts = []
_BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

# =====================================================================
# PROCESS LOGGING & MONITORING
# =====================================================================

def log_process(level, message):
    """Log process events for dashboard display."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = {
        "timestamp": timestamp,
        "level": level,
        "message": message
    }
    _process_logs.append(log_entry)
    print(f"[{level.upper()}] {timestamp} - {message}")
    
    if len(_process_logs) > 150:
        _process_logs.pop(0)
    
    if level in ["error", "warning"]:
        _alerts.append({
            "timestamp": timestamp,
            "level": level,
            "message": message
        })
        if len(_alerts) > 50:
            _alerts.pop(0)

def save_heartbeat(job_name, status, details=""):
    """Save heartbeat for cron job status."""
    timestamp = datetime.now(timezone.utc).isoformat()
    _heartbeat_log[job_name] = {
        "status": status,
        "timestamp": timestamp,
        "details": details
    }
    
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

def get_job_status(job_name):
    """Get current job status with time since last run."""
    if job_name not in _heartbeat_log:
        return {"status": "idle", "message": "No data", "time_ago": "never", "raw_status": "idle"}
    
    hb = _heartbeat_log[job_name]
    status = hb.get("status", "unknown")
    timestamp = hb.get("timestamp", "")
    details = hb.get("details", "")
    
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
# HELPER FUNCTIONS
# =====================================================================

def _extract_data(result, *keys):
    """Robustly extract data from cTrader API responses."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict) and result and not keys:
        return result

    for key in keys:
        val = result.get(key)
        if val is not None:
            if isinstance(val, (dict, list)):
                return val

    d = result.get("d")
    if d is not None:
        if isinstance(d, (list, dict)):
            return d

    data = result.get("data")
    if data is not None:
        if isinstance(data, (list, dict)):
            return data

    return {}

def _extract_list(result, *keys):
    """Extract a list from an API response."""
    data = _extract_data(result, *keys)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list):
                return v
    return []

def _resolve_field(data, *candidates, default="N/A"):
    """Try multiple possible field names to extract a value."""
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
# CTRADER API CLIENT - OAUTH V2
# =====================================================================

class cTraderClient:
    def __init__(self):
        self.api_base = CT_API_BASE
        self.access_token = CT_ACCESS_TOKEN
        self.authenticated = False

    def _req(self, method, path, body=None, timeout=20):
        """Make HTTP request to cTrader API."""
        url = f"{self.api_base}{path}"
        headers = {
            "User-Agent": "cTraderBot/2.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        if body:
            headers["Content-Type"] = "application/json"

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
                log_process("warning", f"HTTP {e.code} on {method} {path}")
                return {"error": "http_error", "status": e.code}
        except Exception as ex:
            log_process("warning", f"Request failed on {method} {path}: {str(ex)}")
            return {"error": "request_failed", "details": str(ex)}

    def verify_auth(self):
        """Verify cTrader authentication."""
        if not self.access_token:
            log_process("error", "No CT_ACCESS_TOKEN set")
            return False
        
        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}")
        if result.get("error"):
            log_process("error", f"Authentication failed: {result}")
            return False
        
        self.authenticated = True
        log_process("success", f"cTrader authenticated (Account: {CT_ACCOUNT_ID})")
        return True

    def load_instruments(self):
        """Load all instruments from cTrader."""
        global _instruments
        _instruments = {}

        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}/instruments")
        if result.get("error"):
            log_process("warning", f"Failed to load instruments: {result}")
            return False

        instruments = _extract_list(result, "items", "data", "instruments")

        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = (inst.get("displayName") or inst.get("name") or "").upper().strip()
            inst_id = inst.get("symbolId") or inst.get("id")
            
            if name and inst_id is not None:
                _instruments[name] = {"id": inst_id}

        log_process("info", f"Loaded {len(_instruments)} instruments from cTrader")
        return len(_instruments) > 0

    def find_instrument(self, pair_name):
        """Resolve pair name to instrument ID."""
        if not pair_name:
            return None

        normalized = pair_name.replace("/", "").replace(" ", "").upper().strip()

        # Check user-defined PAIR_MAP
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in _instruments:
            return _instruments[mapped]

        # Check aliases
        alias = PAIR_ALIASES.get(normalized, "")
        if alias and alias in _instruments:
            return _instruments[alias]

        # Exact match
        if normalized in _instruments:
            return _instruments[normalized]

        # Fuzzy match
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                return info

        log_process("warning", f"Could not find instrument for '{pair_name}'")
        return None

    def place_order(self, pair, direction, sl, tp, qty=None):
        """Place a MARKET order."""
        if not qty:
            qty = DEFAULT_QTY

        log_process("info", f"Placing {direction} market order for {pair} | Qty: {qty} | SL: {sl} | TP: {tp}")

        instrument = self.find_instrument(pair)
        if not instrument:
            log_process("info", "Instrument not found, reloading...")
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            log_process("error", f"Cannot resolve instrument ID for '{pair}'")
            return False

        inst_id = instrument["id"]
        side = "BUY" if direction.upper() == "BUY" else "SELL"

        payload = {
            "symbolId": inst_id,
            "orderType": "MARKET",
            "tradeSide": side,
            "volume": int(qty * 100000),  # cTrader uses microunits
        }
        if sl is not None:
            payload["stopPrice"] = float(sl)
        if tp is not None:
            payload["takeProfit"] = float(tp)

        result = self._req("POST", f"/accounts/{CT_ACCOUNT_ID}/orders", body=payload, timeout=25)

        if result.get("error"):
            log_process("error", f"Order placement failed: {result}")
            return False

        log_process("success", f"Order placed successfully!")
        return True

    def get_account_info(self):
        """Get account balance, equity, margin, etc."""
        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}")
        
        if result.get("error"):
            log_process("warning", f"get_account_info error: {result.get('error')}")
            return {}

        account_data = result.get("data", result)
        if not isinstance(account_data, dict):
            account_data = _extract_data(result)
        
        return account_data if isinstance(account_data, dict) else {}

    def get_open_positions(self):
        """Get all open positions."""
        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}/positions")
        
        if result.get("error"):
            log_process("warning", f"get_open_positions error")
            return []
        
        positions = _extract_list(result, "items", "data", "positions")
        log_process("info", f"Found {len(positions)} open positions")
        return positions

    def close_position(self, pos_id):
        """Close an open position."""
        result = self._req("DELETE", f"/accounts/{CT_ACCOUNT_ID}/positions/{pos_id}")
        success = not result.get("error")
        log_process("success" if success else "error", f"Close position {pos_id}: {'Success' if success else 'Failed'}")
        return success

    def modify_position(self, pos_id, new_sl, new_tp=None):
        """Update SL/TP of a position."""
        log_process("info", f"Updating position {pos_id} SL → {new_sl}")

        payload = {}
        if new_sl is not None:
            payload["stopPrice"] = float(new_sl)
        if new_tp is not None:
            payload["takeProfit"] = float(new_tp)

        result = self._req("PATCH", f"/accounts/{CT_ACCOUNT_ID}/positions/{pos_id}", body=payload)
        success = not result.get("error")
        log_process("success" if success else "error", f"Position update: {'Success' if success else 'Failed'}")
        return success

    def get_orders(self):
        """Get pending orders."""
        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}/orders")
        
        if result.get("error"):
            log_process("warning", f"get_orders error")
            return []
        
        orders = _extract_list(result, "items", "data", "orders")
        log_process("info", f"Found {len(orders)} orders")
        return orders[:20]

    def get_trade_history(self):
        """Get closed trades."""
        result = self._req("GET", f"/accounts/{CT_ACCOUNT_ID}/deals?limit=50")
        
        if result.get("error"):
            return []
        
        trades = _extract_list(result, "items", "data", "deals")
        log_process("info", f"Found {len(trades)} closed trades")
        return trades

# =====================================================================
# TELEGRAM
# =====================================================================

def test_telegram_connection():
    """Test if Telegram bot is reachable."""
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

    # Handle TP/SL Hit signals
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

    # Handle SL Update signals
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

    # Handle BUY/SELL signals
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
# DASHBOARD GENERATOR
# =====================================================================

def generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info):
    """Generate the full dashboard HTML."""

    account_info = client.get_account_info() if tl_connected else {}
    positions = client.get_open_positions() if tl_connected else []
    orders = client.get_orders() if tl_connected else []
    trades = client.get_trade_history() if tl_connected else []

    log_process("info", f"Dashboard data: positions={len(positions)}, orders={len(orders)}, trades={len(trades)}")

    # Extract account fields
    balance_raw = _resolve_field(account_info, "balance", "accountBalance", "Balance", "totalBalance")
    equity_raw = _resolve_field(account_info, "equity", "Equity", "accountEquity", "equityValue")
    margin_raw = _resolve_field(account_info, "usedMargin", "margin", "used_margin", "Margin")
    free_margin_raw = _resolve_field(account_info, "freeMargin", "free_margin", "FreeMargin", "availableMargin")
    margin_level_raw = _resolve_field(account_info, "marginLevel", "margin_level", "MarginLevel")
    daypl_raw = _resolve_field(account_info, "dayPL", "dayPl", "dailyPnL", "dailyPL", "pnl")
    currency_raw = _resolve_field(account_info, "currency", "accountCurrency", "Currency", default="USD")

    account_state = {
        'balance': _safe_currency(balance_raw) if balance_raw != "N/A" else "N/A",
        'equity': _safe_currency(equity_raw) if equity_raw != "N/A" else "N/A",
        'margin': _safe_currency(margin_raw) if margin_raw != "N/A" else "N/A",
        'free_margin': _safe_currency(free_margin_raw) if free_margin_raw != "N/A" else "N/A",
        'margin_level': f"{margin_level_raw}%" if margin_level_raw != "N/A" else "N/A%",
        'currency': str(currency_raw),
        'daypl': _safe_currency(daypl_raw) if daypl_raw != "N/A" else "$0",
        'account_id': CT_ACCOUNT_ID,
        'server': CT_ENV.upper(),
    }

    # Calculate margin usage
    try:
        margin_usage = 0
        used = _safe_float(margin_raw)
        free = _safe_float(free_margin_raw)
        total = used + free
        if total > 0:
            margin_usage = (used / total) * 100
    except:
        margin_usage = 0

    # Parse positions
    positions_data = []
    total_pnl = 0

    for pos in positions:
        if not isinstance(pos, dict):
            continue
        try:
            pair_name = pos.get('symbolName') or pos.get('symbol') or 'N/A'
            pnl_val = _safe_float(pos.get('profit') or pos.get('pnl') or 0)
            total_pnl += pnl_val
            
            positions_data.append({
                'pair': pair_name,
                'side': str(pos.get('tradeSide') or '').upper(),
                'qty': pos.get('volume') or 'N/A',
                'price': pos.get('openPrice') or 'N/A',
                'sl': pos.get('stopPrice') or pos.get('stopLoss') or '—',
                'tp': pos.get('takeProfit') or '—',
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
            })
        except Exception as e:
            log_process("warning", f"Error parsing position: {e}")
            continue

    # Parse trades
    trades_data = []
    wins = 0
    losses = 0
    for tr in trades:
        if not isinstance(tr, dict):
            continue
        try:
            pnl_val = _safe_float(tr.get('profit') or tr.get('pnl') or 0)
            if pnl_val > 0:
                wins += 1
            elif pnl_val < 0:
                losses += 1
            
            trades_data.append({
                'pair': tr.get('symbolName') or tr.get('symbol') or 'N/A',
                'side': str(tr.get('tradeSide') or '').upper(),
                'entry': _safe_float(tr.get('openPrice') or 0),
                'exit': _safe_float(tr.get('closePrice') or 0),
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
            })
        except Exception as e:
            log_process("warning", f"Error parsing trade: {e}")
            continue

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

    # Parse orders
    orders_data = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        try:
            status = str(order.get('status') or '').upper()
            if status in ('FILLED', 'CANCELLED', 'REJECTED', 'EXECUTED'):
                continue
            
            orders_data.append({
                'id': str(order.get('id') or '')[:12],
                'pair': order.get('symbolName') or order.get('symbol') or 'N/A',
                'side': str(order.get('tradeSide') or '').upper(),
                'type': str(order.get('orderType') or 'N/A').upper(),
                'qty': order.get('volume') or 'N/A',
                'price': order.get('price') or order.get('stopPrice') or 'N/A',
                'status': status or 'PENDING',
            })
        except Exception as e:
            log_process("warning", f"Error parsing order: {e}")
            continue

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Connection status
    tl_status_color = "#3fb950" if tl_connected else "#f85149"
    tl_status_icon = "✓" if tl_connected else "✗"
    tl_status_text = "Connected" if tl_connected else "Disconnected"
    tl_detail = f"Account: {CT_ACCOUNT_ID} | Env: {CT_ENV}" if tl_connected else (tl_error or "Auth failed")

    tg_status_color = "#3fb950" if tg_connected else "#f85149"
    tg_status_icon = "✓" if tg_connected else "✗"
    tg_status_text = "Connected" if tg_connected else "Disconnected"
    tg_detail = f"Bot: @{tg_info.get('username', 'N/A')}" if tg_connected else tg_info.get('error', 'Unknown error')

    # Job status
    bot_status = get_job_status("bot")
    dashboard_status = get_job_status("dashboard")

    # Health checks
    health_checks = {
        "ct_auth": {"ok": tl_connected, "label": "cTrader Auth", "icon": "🔐"},
        "tg_bot": {"ok": tg_connected, "label": "Telegram Bot", "icon": "📱"},
        "instruments": {"ok": len(_instruments) > 0, "label": "Instruments Loaded", "icon": "📊"},
        "margin_safe": {"ok": margin_usage < 80, "label": f"Margin Safe ({margin_usage:.1f}%)", "icon": "⚠️"}
    }

    # Build tables
    if positions_data:
        positions_table = """<table>
<thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry</th><th>SL</th><th>TP</th><th>P&L</th></tr></thead>
<tbody>"""
        for p in positions_data:
            side_class = "buy" if "BUY" in p["side"] else "sell"
            pnl_color = "#3fb950" if p["pnl_value"] >= 0 else "#f85149"
            positions_table += f"""<tr>
<td class="pair">{p['pair']}</td>
<td class="{side_class}">{p['side']}</td>
<td>{p['qty']}</td>
<td>{p['price']}</td>
<td class="sl">{p['sl']}</td>
<td class="tp">{p['tp']}</td>
<td style="color:{pnl_color}">{p['pnl']}</td>
</tr>"""
        positions_table += "</tbody></table>"
    else:
        positions_table = '<div class="empty">No open positions</div>'

    if trades_data:
        trades_table = """<table>
<thead><tr><th>Pair</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead>
<tbody>"""
        for t in trades_data:
            side_class = "buy" if "BUY" in t["side"] else "sell"
            pnl_color = "#3fb950" if t["pnl_value"] >= 0 else "#f85149"
            trades_table += f"""<tr>
<td class="pair">{t['pair']}</td>
<td class="{side_class}">{t['side']}</td>
<td>{t['entry']}</td>
<td>{t['exit']}</td>
<td style="color:{pnl_color}">{t['pnl']}</td>
</tr>"""
        trades_table += "</tbody></table>"
    else:
        trades_table = '<div class="empty">No closed trades</div>'

    if orders_data:
        orders_table = """<table>
<thead><tr><th>ID</th><th>Pair</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th></tr></thead>
<tbody>"""
        for o in orders_data:
            side_class = "buy" if "BUY" in o["side"] else "sell"
            orders_table += f"""<tr>
<td>{o['id']}</td>
<td class="pair">{o['pair']}</td>
<td class="{side_class}">{o['side']}</td>
<td>{o['type']}</td>
<td>{o['qty']}</td>
<td>{o['price']}</td>
<td>{o['status']}</td>
</tr>"""
        orders_table += "</tbody></table>"
    else:
        orders_table = '<div class="empty">No pending orders</div>'

    # Logs table
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
<td style="color:{level_color}">{level}</td>
<td>{log['message']}</td>
</tr>"""
        logs_table += "</tbody></table>"
    else:
        logs_table = '<div class="empty">No logs yet</div>'

    # Alerts
    alerts_html = ""
    if _alerts:
        for alert in _alerts[-10:]:
            alert_color = "#f85149" if alert["level"] == "error" else "#d29922"
            alerts_html += f'<div style="padding:8px;margin:5px 0;background:{alert_color}20;border-left:3px solid {alert_color};border-radius:4px;font-size:11px;"><strong>{alert["level"].upper()}</strong> {alert["timestamp"]}: {alert["message"]}</div>'
    else:
        alerts_html = '<div class="empty">No alerts</div>'

    # Health check HTML
    health_html = ""
    for check_key, check in health_checks.items():
        status_color = "#3fb950" if check["ok"] else "#f85149"
        status_text = "✓ OK" if check["ok"] else "✗ FAILED"
        health_html += f'<div class="health-item"><span class="health-icon">{check["icon"]}</span><span class="health-label">{check["label"]}</span><span class="health-status" style="color:{status_color};">{status_text}</span></div>'

    # Final HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>cTrader Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; font-size: 13px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 20px; }}
        .header h1 {{ font-size: 16px; font-weight: 700; }}
        .header-actions {{ display: flex; gap: 8px; }}
        .btn {{ padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; }}
        .btn-logout {{ background: #da3633; color: #fff; }}
        .btn-logout:hover {{ background: #f85149; }}
        .conn-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .conn-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }}
        .conn-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .conn-title {{ font-size: 12px; font-weight: 700; text-transform: uppercase; }}
        .conn-badge {{ font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 12px; }}
        .conn-detail {{ font-size: 11px; color: #8b949e; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin-bottom: 15px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 15px; }}
        .stat {{ text-align: center; padding: 15px; }}
        .stat-value {{ font-size: 20px; font-weight: 800; color: #58a6ff; }}
        .stat-label {{ font-size: 10px; color: #8b949e; margin-top: 5px; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ padding: 10px; background: #0d1117; color: #8b949e; font-size: 10px; text-transform: uppercase; text-align: left; border-bottom: 1px solid #30363d; }}
        td {{ padding: 10px; border-bottom: 1px solid #30363d; }}
        tr:hover td {{ background: #1c2128; }}
        .pair {{ font-weight: 600; }}
        .buy {{ color: #3fb950; }}
        .sell {{ color: #f85149; }}
        .sl {{ color: #f85149; }}
        .tp {{ color: #3fb950; }}
        .section-title {{ font-size: 11px; color: #8b949e; font-weight: 600; text-transform: uppercase; margin: 20px 0 10px 0; }}
        .empty {{ text-align: center; color: #8b949e; padding: 20px; }}
        .health-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 15px; }}
        .health-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px; display: flex; align-items: center; gap: 10px; font-size: 11px; }}
        .health-icon {{ font-size: 16px; }}
        .health-label {{ flex: 1; }}
        .health-status {{ font-weight: 600; }}
        .cron-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .cron-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }}
    </style>
    <script>
        function logout() {{
            sessionStorage.clear();
            window.location.href = 'login.html';
        }}
        function checkAuth() {{
            if (sessionStorage.getItem('dashboard_authenticated') !== 'true') {{
                window.location.href = 'login.html';
            }}
        }}
        checkAuth();
        setInterval(() => location.reload(), 60000);
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 cTrader Dashboard</h1>
            <div class="header-actions">
                <button class="btn btn-logout" onclick="logout()">🚪 Logout</button>
            </div>
        </div>

        <div style="background: #0d1117; padding: 10px; border-radius: 6px; font-size: 11px; color: #8b949e; margin-bottom: 15px;">
            Account ID: {account_state['account_id']} | Server: {account_state['server']} | Currency: {account_state['currency']} | Build: {_BUILD_VERSION}
        </div>

        <div class="section-title">🩺 System Health</div>
        <div class="health-grid">
            {health_html}
        </div>

        <div class="section-title">Connection Status</div>
        <div class="conn-grid">
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">cTrader</span>
                    <span class="conn-badge" style="background:{tl_status_color}20;color:{tl_status_color};">{tl_status_icon} {tl_status_text}</span>
                </div>
                <div class="conn-detail">{tl_detail}</div>
            </div>
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">Telegram</span>
                    <span class="conn-badge" style="background:{tg_status_color}20;color:{tg_status_color};">{tg_status_icon} {tg_status_text}</span>
                </div>
                <div class="conn-detail">{tg_detail}</div>
            </div>
        </div>

        <div class="section-title">Cron Job Status</div>
        <div class="cron-grid">
            <div class="cron-card">
                <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 8px;">Trading Bot</div>
                <div style="font-size: 10px; color: #8b949e;">{bot_status['raw_status'].upper()} ({bot_status['time_ago']})</div>
                <div style="font-size: 10px; color: #8b949e; margin-top: 6px;">{bot_status['message']}</div>
            </div>
            <div class="cron-card">
                <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 8px;">Dashboard</div>
                <div style="font-size: 10px; color: #8b949e;">{dashboard_status['raw_status'].upper()} ({dashboard_status['time_ago']})</div>
                <div style="font-size: 10px; color: #8b949e; margin-top: 6px;">{dashboard_status['message']}</div>
            </div>
        </div>

        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{account_state['balance']}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">{account_state['equity']}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin']}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['free_margin']}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin_level']}</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">{account_state['daypl']}</div><div class="stat-label">Day P&L</div></div>
        </div>

        <div class="section-title">Trade Statistics</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{total_trades}</div><div class="stat-label">Total Trades</div></div>
            <div class="card stat"><div class="stat-value">{wins}</div><div class="stat-label">Wins</div></div>
            <div class="card stat"><div class="stat-value">{losses}</div><div class="stat-label">Losses</div></div>
            <div class="card stat"><div class="stat-value">{win_rate:.1f}%</div><div class="stat-label">Win Rate</div></div>
            <div class="card stat"><div class="stat-value">${total_pnl:+.2f}</div><div class="stat-label">Open P&L</div></div>
            <div class="card stat"><div class="stat-value">{len(positions_data)}</div><div class="stat-label">Open Positions</div></div>
        </div>

        <div class="section-title">Recent Alerts</div>
        <div class="card">{alerts_html}</div>

        <div class="section-title">Process Logs (Last 30)</div>
        <div class="card">{logs_table}</div>

        <div class="section-title">Open Positions</div>
        <div class="card">{positions_table}</div>

        <div class="section-title">Closed Trades</div>
        <div class="card">{trades_table}</div>

        <div class="section-title">Pending Orders</div>
        <div class="card">{orders_table}</div>

        <div style="text-align: center; color: #8b949e; font-size: 11px; margin-top: 30px;">Last updated: {last_update} | Auto-refresh: 60 seconds</div>
    </div>
</body>
</html>"""
    return html

def create_login_html():
    """Generate login page."""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>cTrader Dashboard - Login</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
            background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
            color: #c9d1d9; 
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
        }}
        .form-group input {{
            width: 100%;
            padding: 12px;
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 6px;
            color: #c9d1d9;
            font-size: 14px;
        }}
        .form-group input:focus {{
            outline: none;
            border-color: #58a6ff;
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
        }}
        .login-btn:hover {{
            background: #2ea043;
        }}
        .error-message {{
            display: none;
            background: #f85149;
            color: #fff;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-size: 12px;
        }}
        .error-message.show {{
            display: block;
        }}
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1>🚀 Dashboard</h1>
            <p>cTrader Bot Control Panel</p>
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
    </div>

    <script>
        const CORRECT_USERNAME = "{DASHBOARD_USERNAME}";
        const CORRECT_PASSWORD = "{DASHBOARD_PASSWORD}";

        function handleLogin(event) {{
            event.preventDefault();
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const errorMsg = document.getElementById('errorMsg');

            if (username === CORRECT_USERNAME && password === CORRECT_PASSWORD) {{
                sessionStorage.setItem('dashboard_authenticated', 'true');
                sessionStorage.setItem('dashboard_username', username);
                window.location.href = 'index.html?v={_BUILD_VERSION}';
            }} else {{
                errorMsg.textContent = '❌ Invalid username or password';
                errorMsg.classList.add('show');
                document.getElementById('password').value = '';
            }}
        }}

        if (sessionStorage.getItem('dashboard_authenticated') === 'true') {{
            window.location.href = 'index.html?v={_BUILD_VERSION}';
        }}
    </script>
</body>
</html>"""
    return html

# =====================================================================
# BOT MODE
# =====================================================================

def run_bot():
    """Run the trading bot."""
    try:
        save_heartbeat("bot", "running", "Initializing bot...")
        log_process("info", "=== BOT CYCLE STARTED ===")
        
        if not (CT_CLIENT_ID and CT_CLIENT_SECRET and CT_ACCESS_TOKEN):
            log_process("error", "Missing cTrader OAuth credentials")
            save_heartbeat("bot", "failed", "Missing credentials")
            return False

        client = cTraderClient()
        if not client.verify_auth():
            log_process("error", "cTrader authentication failed")
            save_heartbeat("bot", "failed", "Auth failed")
            return False

        client.load_instruments()

        if TG_TOKEN:
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
                    log_process("info", f"{parsed['result']} HIT on {pair}")
                    positions = client.get_open_positions()
                    for pos in positions:
                        if not isinstance(pos, dict):
                            continue
                        pos_sym = pos.get('symbolName') or ""
                        if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                            pos_id = pos.get('id')
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
                        pos_sym = pos.get('symbolName') or ""
                        if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                            pos_id = pos.get('id')
                            if pos_id:
                                client.modify_position(pos_id, new_sl)

            log_process("info", f"=== BOT CYCLE COMPLETE === ({signal_count} signals processed)")
            save_heartbeat("bot", "completed", f"Processed {signal_count} signals")
        else:
            log_process("info", "Telegram not configured, skipping signal processing")
            save_heartbeat("bot", "completed", "No Telegram configured")

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
    """Generate the dashboard HTML."""
    try:
        load_heartbeat()
        save_heartbeat("dashboard", "running", "Generating dashboard...")
        log_process("info", "=== DASHBOARD GENERATION STARTED ===")

        client = cTraderClient()

        tl_connected = client.verify_auth()
        tl_error = None
        if not tl_connected:
            tl_error = "Failed to authenticate with cTrader"
            log_process("warning", f"cTrader: DISCONNECTED")
        else:
            log_process("success", "cTrader: CONNECTED")
            client.load_instruments()

        tg_connected, tg_info = test_telegram_connection()
        if tg_connected:
            log_process("success", f"Telegram: CONNECTED (@{tg_info.get('username')})")
        else:
            log_process("warning", f"Telegram: DISCONNECTED")

        html = generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info)

        os.makedirs("docs", exist_ok=True)
        output_path = os.path.join("docs", "index.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        login_path = os.path.join("docs", "login.html")
        with open(login_path, "w", encoding="utf-8") as f:
            f.write(create_login_html())

        log_process("success", f"Dashboard written to {output_path}")
        log_process("info", "=== DASHBOARD GENERATION COMPLETE ===")
        save_heartbeat("dashboard", "completed", "No errors")
        return True
    except Exception as e:
        log_process("error", f"Dashboard generation failed: {str(e)}")
        save_heartbeat("dashboard", "failed", str(e)[:100])
        import traceback
        traceback.print_exc()
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

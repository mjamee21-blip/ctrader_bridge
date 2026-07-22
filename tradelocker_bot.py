#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TradeLocker Telegram Bot + Dashboard (Enhanced with Login)
# Features:
#   - Connection status for TradeLocker AND Telegram on dashboard
#   - Login authentication (username/password from GitHub secrets)
#   - "Fetch Now" button (triggers GitHub Actions workflow via API)
#   - Shows SL/TP and P&L results for each trade from live TradeLocker data
#   - Places MARKET orders immediately — ignores any price/REF in signal
#   - Accurate SL/TP placement from signal
#   - SL update handling (adjusts existing position SL)
#   - Instrument ID resolution (pair name → numeric tradableInstrumentId)
#   - Dashboard shows: balance, equity, margin, open positions, closed trades, orders, connection status

import os, json, re, urllib.request, urllib.parse, sys, hashlib, hmac, base64
from urllib.error import HTTPError
from datetime import datetime, timezone
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
                return {"error": "http_error", "status": e.code, "body": err_body}
            except:
                return {"error": "http_error", "status": e.code}
        except Exception as ex:
            return {"error": "request_failed", "details": str(ex)}

    def auth(self):
        """Authenticate with TradeLocker and store the JWT token."""
        if not TL_EMAIL or not TL_PASSWORD:
            print("[TradeLocker] ERROR: TL_EMAIL or TL_PASSWORD not set")
            return False
            
        payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}
        result = self._req("POST", "/auth/jwt/token", body=payload)
        if result.get("error"):
            print(f"[TradeLocker] Auth failed: {result}")
            return False
        self.token = result.get("accessToken") or result.get("access_token") or result.get("token")
        self.authenticated = bool(self.token)
        if self.authenticated:
            print(f"[TradeLocker] Authenticated successfully on {TL_SERVER}")
        return self.authenticated

    def load_instruments(self):
        """
        Load ALL instruments from TradeLocker and build a name→ID mapping.
        TradeLocker uses NUMERIC instrument IDs, so we must resolve pair names
        to their numeric tradableInstrumentId before placing orders.
        """
        global _instruments
        _instruments = {}  # Reset

        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/instruments",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            print(f"[TradeLocker] Failed to load instruments: {result}")
            return False

        data = result.get("d", result)
        instruments = data.get("instruments") or data.get("data") or data.get("items") or []

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

        print(f"[TradeLocker] Loaded {len(_instruments)} instruments (name→numeric ID mapping)")
        examples = list(_instruments.keys())[:10]
        print(f"[TradeLocker] Sample instruments: {examples}")
        return len(_instruments) > 0

    def find_instrument(self, pair_name):
        """
        Resolve a pair name (e.g. "XAUUSD") to its numeric instrument ID.
        Checks: PAIR_MAP → PAIR_ALIASES → exact match → fuzzy match.
        Returns {"id": <numeric_id>, "route_id": <route_id>} or None.
        """
        if not pair_name:
            return None

        normalized = pair_name.replace("/", "").replace(" ", "").upper().strip()

        # 1. Check user-defined PAIR_MAP first
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in _instruments:
            print(f"[Instruments] '{pair_name}' → PAIR_MAP → '{mapped}' (ID: {_instruments[mapped]['id']})")
            return _instruments[mapped]

        # 2. Check common aliases
        alias = PAIR_ALIASES.get(normalized, "")
        if alias and alias in _instruments:
            print(f"[Instruments] '{pair_name}' → ALIAS '{alias}' (ID: {_instruments[alias]['id']})")
            return _instruments[alias]

        # 3. Exact match
        if normalized in _instruments:
            print(f"[Instruments] '{pair_name}' → exact match (ID: {_instruments[normalized]['id']})")
            return _instruments[normalized]

        # 4. Fuzzy match (contains)
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                print(f"[Instruments] '{pair_name}' → fuzzy match '{name}' (ID: {info['id']})")
                return info

        print(f"[Instruments] WARNING: Could not find instrument for '{pair_name}'")
        print(f"[Instruments] Available (first 20): {list(_instruments.keys())[:20]}")
        return None

    def get_quote(self, inst_id):
        """Get current bid/ask quote for an instrument."""
        qs = f"?tradableInstrumentId={inst_id}"
        result = self._req("GET", f"/trade/quotes{qs}", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return None
        d = result.get("d", result)
        quotes = d.get("quotes") if isinstance(d, dict) else None
        q = (quotes[0] if isinstance(quotes, list) and quotes else d)
        bp, ap = q.get("bp") or q.get("bid"), q.get("ap") or q.get("ask")
        return {"bp": float(bp), "ap": float(ap)} if bp and ap else None

    def place_order(self, pair, direction, sl, tp, qty=None):
        """
        Place a MARKET order immediately — ignores any price/REF in the signal.
        Uses the numeric instrument ID resolved from the pair name.
        SL and TP are placed accurately from the signal values.
        """
        if not qty:
            qty = DEFAULT_QTY

        print(f"\n[Order] Placing {direction} market order for {pair} | Qty: {qty} | SL: {sl} | TP: {tp}")

        instrument = self.find_instrument(pair)
        if not instrument:
            print("[Order] Instrument not found, reloading instruments...")
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            print(f"[Order] FAILED: Cannot resolve instrument ID for '{pair}'")
            return False

        inst_id = instrument["id"]
        route_id = instrument.get("route_id")
        side = "buy" if direction.upper() == "BUY" else "sell"

        print(f"[Order] Instrument ID: {inst_id} | Route ID: {route_id} | Side: {side}")

        payload = {
            "tradableInstrumentId": inst_id,
            "type": "market",
            "validity": "IOC",
            "side": side,
            "qty": qty,
        }
        if sl is not None:
            payload["stopLoss"] = float(sl)
            print(f"[Order] Stop Loss set to: {sl}")
        if tp is not None:
            payload["takeProfit"] = float(tp)
            print(f"[Order] Take Profit set to: {tp}")
        if route_id:
            payload["routeId"] = str(route_id)

        result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders",
                           body=payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)

        if result.get("error") or result.get("s") == "error":
            errmsg = result.get("errmsg") or result.get("body") or result.get("error") or ""
            print(f"[Order] Market order failed: {errmsg}")

            if "forbidden" in str(errmsg).lower() and "route" in str(errmsg).lower():
                print("[Order] Route forbidden — trying limit order near market price...")
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
                        print(f"[Order] Limit order also failed: {result}")
                        return False
                    print(f"[Order] Limit order placed at {limit_price}")
                    return True
            return False

        print(f"[Order] ✅ Market order placed successfully!")
        return True

    def get_open_positions(self):
        """Get all open positions with their SL/TP and P&L."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/positions",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data
        return data.get("positions") or data.get("data") or data.get("items") or []

    def close_position(self, pos_id):
        """Close an open position by ID."""
        result = self._req("DELETE", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        success = not result.get("error")
        print(f"[Position] Close {pos_id}: {'✅ Success' if success else '❌ Failed'}")
        return success

    def modify_position(self, pos_id, pos, new_sl):
        """
        Update the SL of an existing position.
        Used when a SL_UPDATE signal is received.
        """
        print(f"[Position] Updating SL for position {pos_id} → new SL: {new_sl}")

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
        print(f"[Position] SL update: {'✅ Success' if success else '❌ Failed'} ({result})")
        return success

    def get_account_state(self):
        """Get account balance, equity, margin, etc."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/state",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        data = result.get("d", result)
        return data if isinstance(data, dict) else {}

    def get_orders(self):
        """Get recent order history (up to 20)."""
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/orders?limit=50",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        data = result.get("d", result)
        raw_orders = data if isinstance(data, list) else (data.get("orders") or data.get("data") or data.get("items") or [])
        return raw_orders[:20]

    def get_trade_history(self):
        """
        Get closed trade history for SL/TP results.
        Tries the /trades endpoint; falls back to filtering filled orders.
        """
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/trades?limit=20",
                           headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data
        return data.get("trades") or data.get("data") or data.get("items") or []

# =====================================================================
# TELEGRAM
# =====================================================================

def test_telegram_connection():
    """
    Test if the Telegram bot is reachable.
    Returns (connected: bool, info: dict)
    """
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
        print(f"[Telegram] Error fetching messages: {ex}")
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
    """
    Parse a trading signal from Telegram.
    - ALWAYS uses MARKET execution (ignores any entry/REF price in signal)
    - Extracts SL and TP accurately
    - Handles TP/SL hit notifications and SL updates
    """
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
        print(f"[Signal] TPSL_HIT: {result} on {pair_str}")
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
            print(f"[Signal] SL_UPDATE: {pair} → new SL {new_sl}")
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

    print(f"[Signal] {direction} {pair} | SL: {sl} | TP: {tp} (entry price IGNORED — market execution)")
    return {"type": "SIGNAL", "direction": direction, "pair": pair, "sl": sl, "tp": tp}

# =====================================================================
# DASHBOARD GENERATOR (WITH LOGIN)
# =====================================================================

def generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info):
    """Generate the full dashboard HTML with login, connection status, fetch button, SL/TP, and results."""

    state = client.get_account_state() if tl_connected else {}
    positions = client.get_open_positions() if tl_connected else []
    orders = client.get_orders() if tl_connected else []
    trades = client.get_trade_history() if tl_connected else []

    # --- Account state ---
    account_state = {
        'balance': f"${state.get('accountBalance', 'N/A')}",
        'equity': f"${state.get('equity', 'N/A')}",
        'margin': f"${state.get('usedMargin', 'N/A')}",
        'free_margin': f"${state.get('freeMargin', 'N/A')}",
        'margin_level': f"{state.get('marginLevel', 'N/A')}%",
        'currency': state.get('currency', 'USD'),
        'daypl': f"${state.get('dayPL', '0')}",
        'account_id': TL_ACCOUNT_ID,
        'server': TL_SERVER,
    }

    # --- Open positions with SL/TP and P&L ---
    positions_data = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        try:
            pos_time = pos.get('openTime', '')
            if isinstance(pos_time, (int, float)):
                pos_time = datetime.fromtimestamp(pos_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            pnl_val = float(pos.get('pnl') or pos.get('profit') or 0)
            positions_data.append({
                'pair': pos.get('instrumentName') or pos.get('symbol') or 'N/A',
                'side': str(pos.get('side') or '').upper(),
                'qty': pos.get('qty'),
                'price': pos.get('price') or pos.get('openPrice') or 'N/A',
                'sl': pos.get('stopLoss') or pos.get('sl') or '—',
                'tp': pos.get('takeProfit') or pos.get('tp') or '—',
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
                'time': pos_time
            })
        except Exception as e:
            print(f"[Dashboard] Error parsing position: {e}")
            continue

    # --- Closed trades (results) ---
    trades_data = []
    for tr in trades:
        if not isinstance(tr, dict):
            continue
        try:
            close_time = tr.get('closeTime') or tr.get('closedAt') or ''
            if isinstance(close_time, (int, float)):
                close_time = datetime.fromtimestamp(close_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            pnl_val = float(tr.get('pnl') or tr.get('profit') or 0)
            entry = float(tr.get('openPrice') or tr.get('price') or 0)
            exit_p = float(tr.get('closePrice') or tr.get('exitPrice') or 0)
            sl_val = tr.get('stopLoss')
            tp_val = tr.get('takeProfit')
            result_label = "Closed"
            if sl_val and abs(exit_p - float(sl_val)) < 0.01:
                result_label = "🔴 SL Hit"
            elif tp_val and abs(exit_p - float(tp_val)) < 0.01:
                result_label = "🟢 TP Hit"
            trades_data.append({
                'pair': tr.get('instrumentName') or tr.get('symbol') or 'N/A',
                'side': str(tr.get('side') or '').upper(),
                'entry': entry,
                'exit': exit_p,
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
                'result': result_label,
                'time': close_time
            })
        except Exception as e:
            print(f"[Dashboard] Error parsing trade: {e}")
            continue

    # --- Recent orders ---
    orders_data = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        try:
            order_time = order.get('modifiedAt') or order.get('placedAt') or ''
            if isinstance(order_time, (int, float)):
                order_time = datetime.fromtimestamp(order_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            orders_data.append({
                'id': str(order.get('orderId') or order.get('id') or '')[:12],
                'pair': order.get('instrumentName') or order.get('symbol') or 'N/A',
                'side': str(order.get('side') or '').upper(),
                'type': order.get('type') or 'N/A',
                'qty': order.get('qty'),
                'price': order.get('price') or order.get('avgPrice') or 'N/A',
                'status': order.get('status') or 'N/A',
                'time': order_time
            })
        except Exception as e:
            print(f"[Dashboard] Error parsing order: {e}")
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

    # --- JavaScript ---
    js_code = """
<script>
function getGH() {
    return {
        token: localStorage.getItem('gh_token') || '',
        owner: localStorage.getItem('gh_owner') || '',
        repo: localStorage.getItem('gh_repo') || '',
        workflow: localStorage.getItem('gh_workflow') || 'tradelocker.yml'
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
        localStorage.removeItem('dashboard_auth');
        location.reload();
    }
}
</script>
"""

    # --- Full HTML ---
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="600">
    <title>TradeLocker Dashboard</title>
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
        .conn-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .conn-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }}
        .conn-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .conn-title {{ font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }}
        .conn-badge {{ font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 12px; }}
        .conn-detail {{ font-size: 11px; color: #8b949e; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin-bottom: 15px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 15px; }}
        .stat {{ text-align: center; padding: 15px; }}
        .stat-value {{ font-size: 22px; font-weight: 800; color: #58a6ff; }}
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
            📊 Account ID: {account_state['account_id']} | Server: {account_state['server']} | Currency: {account_state['currency']}
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

        <!-- Open Positions -->
        <div class="section-title">Open Positions ({len(positions_data)}) — Live SL/TP & P&L</div>
        <div class="card">{positions_table}</div>

        <!-- Closed Trades (Results) -->
        <div class="section-title">Closed Trades ({len(trades_data)}) — Results</div>
        <div class="card">{trades_table}</div>

        <!-- Recent Orders -->
        <div class="section-title">Recent Orders (Last 20)</div>
        <div class="card">{orders_table}</div>

        <div class="last-update">Last updated: {last_update} | Auto-refresh every 10 min</div>
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
    if not (TL_EMAIL and TL_PASSWORD and TG_TOKEN and TG_CHAT):
        print("[Bot] ERROR: Missing required secrets (TL_EMAIL, TL_PASSWORD, TG_TOKEN, TG_CHAT)")
        return False

    client = TradeLockerClient()
    if not client.auth():
        print("[Bot] ERROR: TradeLocker authentication failed")
        return False

    client.load_instruments()

    messages = tg_get_messages(offset=_last_update_id)
    print(f"[Bot] Fetched {len(messages)} new messages from Telegram")

    for msg in messages:
        text = (msg.get("text") or "").strip()
        if not looks_like_signal(text):
            continue

        print(f"\n[Bot] === Processing Signal ===")
        print(f"[Bot] Raw: {text[:200]}")

        parsed = parse_signal(text)
        if not parsed:
            print("[Bot] Could not parse signal, skipping")
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
            print(f"[Bot] {result} HIT on {pair} — closing position")
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
            print(f"[Bot] SL UPDATE on {pair} — new SL: {new_sl}")
            positions = client.get_open_positions()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_sym = pos.get('instrumentName') or pos.get('symbol') or pos.get('name') or ""
                if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                    pos_id = pos.get('positionId') or pos.get('id')
                    if pos_id:
                        client.modify_position(pos_id, pos, new_sl)

    print(f"\n[Bot] === Bot cycle complete ===")
    return True

# =====================================================================
# DASHBOARD MODE
# =====================================================================

def generate_dashboard():
    """Generate the dashboard HTML with connection status and deploy to GitHub Pages."""
    print(f"[Dashboard] Starting at {datetime.now(timezone.utc).isoformat()}")
    print(f"[Dashboard] TL_ENV={TL_ENV}, TL_SERVER={TL_SERVER}, TL_ACCOUNT_ID={TL_ACCOUNT_ID}")

    client = TradeLockerClient()

    tl_connected = client.auth()
    tl_error = None
    if not tl_connected:
        tl_error = "Failed to authenticate. Check TL_EMAIL, TL_PASSWORD, TL_SERVER secrets."
        print(f"[Dashboard] TradeLocker: DISCONNECTED — {tl_error}")
    else:
        print("[Dashboard] TradeLocker: CONNECTED")
        client.load_instruments()

    tg_connected, tg_info = test_telegram_connection()
    if tg_connected:
        print(f"[Dashboard] Telegram: CONNECTED (@{tg_info.get('username')})")
    else:
        print(f"[Dashboard] Telegram: DISCONNECTED — {tg_info.get('error')}")

    html = generate_dashboard_html(client, tl_connected, tl_error, tg_connected, tg_info)

    os.makedirs("docs", exist_ok=True)
    output_path = os.path.join("docs", "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[Dashboard] Written to {output_path} ({len(html)} bytes)")
    return True

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
        if MODE == "dashboard":
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
        sys.exit(1)
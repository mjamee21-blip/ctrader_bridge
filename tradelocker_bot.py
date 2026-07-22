#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TradeLocker Telegram Bot + Dashboard
# All-in-one: Trading bot + GitHub Pages dashboard
# Fixed version: Dashboard auto-deploys to GitHub Pages via gh-pages branch

import os, json, re, urllib.request, urllib.parse, sys
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

try:
    PAIR_MAP = json.loads(TL_PAIR_MAP_JSON)
except:
    PAIR_MAP = {}

TL_BASE = "https://live.tradelocker.com" if TL_ENV.lower() == "live" else "https://demo.tradelocker.com"
_last_update_id = 0
_instruments = {}

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
        headers = {"User-Agent": "TLBot/1.0", "Accept": "application/json"}
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
        except HTTPError:
            return {"error": "http_error"}
        except:
            return {"error": "request_failed"}

    def auth(self):
        payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}
        result = self._req("POST", "/auth/jwt/token", body=payload)
        if result.get("error"):
            return False
        self.token = result.get("accessToken") or result.get("access_token") or result.get("token")
        self.authenticated = bool(self.token)
        return self.authenticated

    def load_instruments(self):
        global _instruments
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/instruments", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return False
        data = result.get("d", result)
        instruments = data.get("instruments") or data.get("data") or data.get("items") or []
        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = (inst.get("name") or inst.get("symbol") or "").upper()
            inst_id = inst.get("tradableInstrumentId") or inst.get("id") or ""
            route_id = None
            for route in (inst.get("routes") or []):
                if route.get("type") == "TRADE":
                    route_id = route.get("id")
                    break
            if name:
                _instruments[name] = {"id": inst_id, "route_id": route_id}
        return len(_instruments) > 0

    def find_instrument(self, pair_name):
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in _instruments:
            return _instruments[mapped]
        normalized = pair_name.replace("/", "").upper()
        if normalized in _instruments:
            return _instruments[normalized]
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                return info
        return None

    def get_quote(self, inst_id):
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
        if not qty:
            qty = DEFAULT_QTY

        instrument = self.find_instrument(pair)
        if not instrument:
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            return False

        route_id = instrument.get("route_id")
        inst_id = instrument["id"]
        side = "buy" if direction.upper() == "BUY" else "sell"

        payload = {"tradableInstrumentId": inst_id, "type": "market", "validity": "IOC", "side": side, "qty": qty}
        if sl:
            payload["stopLoss"] = sl
        if tp:
            payload["takeProfit"] = tp
        if route_id:
            payload["routeId"] = str(route_id)

        result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)

        if result.get("error") or result.get("s") == "error":
            errmsg = result.get("errmsg") or result.get("error") or ""
            if "forbidden" in str(errmsg).lower() and "route" in str(errmsg).lower():
                quote = self.get_quote(inst_id)
                if quote:
                    mid = (quote["bp"] + quote["ap"]) / 2.0
                    offset = max(mid * 0.0015, 0.5)
                    limit_price = round(quote["ap"] + offset, 5) if side == "buy" else round(quote["bp"] - offset, 5)
                    lim_payload = {"tradableInstrumentId": inst_id, "type": "limit", "side": side, "qty": qty, "price": limit_price}
                    if sl:
                        lim_payload["stopLoss"] = sl
                    if tp:
                        lim_payload["takeProfit"] = tp
                    if route_id:
                        lim_payload["routeId"] = str(route_id)
                    result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=lim_payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)
            if result.get("error") or result.get("s") == "error":
                return False

        return True

    def get_open_positions(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/positions", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data
        return data.get("positions") or data.get("data") or data.get("items") or []

    def close_position(self, pos_id):
        result = self._req("DELETE", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", headers_extra={"accNum": str(TL_ACC_NUM)})
        return not result.get("error")

    def modify_position(self, pos_id, pos, new_sl):
        payload = {"stopLoss": new_sl}
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

        result = self._req("PUT", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            result = self._req("PATCH", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        return not result.get("error")

    def get_account_state(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/state", headers_extra={"accNum": str(TL_ACC_NUM)})
        data = result.get("d", result)
        return data if isinstance(data, dict) else {}

    def get_orders(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/orders?limit=50", headers_extra={"accNum": str(TL_ACC_NUM)})
        data = result.get("d", result)
        raw_orders = data if isinstance(data, list) else (data.get("orders") or data.get("data") or data.get("items") or [])
        return raw_orders[:20]

# =====================================================================
# SIGNAL PARSER
# =====================================================================

def looks_like_signal(text):
    if not text:
        return False
    t = text.upper()
    return "BUY" in t or "SELL" in t or "TP HIT" in t or "SL HIT" in t or "SL_UPDATE" in t

def parse_signal(text):
    if not text:
        return None
    lines = text.strip().split("\n")
    first = lines[0].strip().upper()

    if "TP HIT" in first or "SL HIT" in first:
        pair = re.search(r"(TP|SL)\s*HIT\s*[-–:]\s*(\S+)", first, re.IGNORECASE)
        pair_str = pair.group(2) if pair else ""
        if not pair_str:
            for line in lines[1:]:
                m = re.search(r"([A-Z]{3,6}[/]?[A-Z]{0,6})", line.strip())
                if m:
                    pair_str = m.group(1)
                    break
        result = "TP" if "TP HIT" in first else "SL"
        return {"type": "TPSL_HIT", "result": result, "pair": pair_str}

    if "#SL_UPDATE" in text.upper() or "SL UPDATE" in text.upper():
        pair, new_sl = None, None
        for line in lines:
            if "PAIR" in line.upper():
                pair = line.split(":", 1)[1].strip() if ":" in line else None
            m = re.search(r"(?:New\s*)?SL\s*[:=]\s*([\d.]+)", line, re.IGNORECASE)
            if m:
                try:
                    new_sl = float(m.group(1))
                except:
                    pass
        if pair and new_sl:
            return {"type": "SL_UPDATE", "pair": pair, "new_sl": new_sl}
        return None

    sig = re.search(r"\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)", first, re.IGNORECASE)
    if not sig:
        return None

    direction = sig.group(1).upper()
    pair = sig.group(2).upper()
    sl = tp = None

    for line in lines:
        cl = re.sub(r"<[^>]+>", "", line).strip()
        m = re.search(r"(?<![A-Za-z])SL\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and not sl:
            try:
                sl = float(m.group(1))
            except:
                pass
        m = re.search(r"(?<![A-Za-z])TP\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and not tp:
            try:
                tp = float(m.group(1))
            except:
                pass

    return {"type": "SIGNAL", "direction": direction, "pair": pair, "sl": sl, "tp": tp}

# =====================================================================
# TELEGRAM
# =====================================================================

def tg_get_messages(offset=0):
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
    except:
        return []

# =====================================================================
# DASHBOARD GENERATOR
# =====================================================================

def generate_dashboard_html(client, connected, error):
    state = client.get_account_state() if connected else {}
    positions = client.get_open_positions() if connected else []
    orders = client.get_orders() if connected else []

    account_state = {
        'balance': f"${state.get('accountBalance', 'N/A')}",
        'equity': f"${state.get('equity', 'N/A')}",
        'margin': f"${state.get('usedMargin', 'N/A')}",
        'free_margin': f"${state.get('freeMargin', 'N/A')}",
        'margin_level': f"{state.get('marginLevel', 'N/A')}%"
    }

    positions_data = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        try:
            pos_time = pos.get('openTime', '')
            if isinstance(pos_time, (int, float)):
                pos_time = datetime.fromtimestamp(pos_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            pnl_val = float(pos.get('pnl') or 0)
            positions_data.append({
                'pair': pos.get('instrumentName') or pos.get('symbol') or 'N/A',
                'side': str(pos.get('side') or '').upper(),
                'qty': pos.get('qty'),
                'price': pos.get('price'),
                'sl': pos.get('stopLoss'),
                'tp': pos.get('takeProfit'),
                'pnl': f"{pnl_val:+.2f}",
                'pnl_value': pnl_val,
                'time': pos_time
            })
        except:
            continue

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
                'price': order.get('price'),
                'status': order.get('status') or 'N/A',
                'time': order_time
            })
        except:
            continue

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    status_color = "ok" if connected else "error"
    status_text = "✓ Connected" if connected else "✗ Disconnected"
    error_html = f'<div class="error-box">⚠️ {error}</div>' if error else '<div class="success-box">✓ Live data from TradeLocker API (updates every 10 min)</div>'

    positions_table = ""
    if positions_data:
        positions_table = "<table><thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry Price</th><th>Stop Loss</th><th>Take Profit</th><th>P&L</th><th>Time</th></tr></thead><tbody>"
        for p in positions_data:
            side_class = "buy" if "buy" in p["side"].lower() else "sell"
            pnl_color = "green" if p["pnl_value"] >= 0 else "red"
            positions_table += f'<tr><td class="pair">{p["pair"]}</td><td class="{side_class}">{p["side"]}</td><td>{p["qty"]}</td><td>{p["price"]}</td><td style="color: #f85149;">{p["sl"]}</td><td style="color: #3fb950;">{p["tp"]}</td><td style="color: {pnl_color}; font-weight: 600;">{p["pnl"]}</td><td style="font-size: 11px; color: #8b949e;">{p["time"]}</td></tr>'
        positions_table += "</tbody></table>"
    else:
        positions_table = '<div style="text-align: center; color: #8b949e; padding: 20px;">No open positions</div>'

    orders_table = ""
    if orders_data:
        orders_table = "<table><thead><tr><th>Order ID</th><th>Pair</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th><th>Time</th></tr></thead><tbody>"
        for o in orders_data:
            side_class = "buy" if "buy" in o["side"].lower() else "sell"
            orders_table += f'<tr><td style="font-size: 10px; color: #8b949e;">{o["id"]}</td><td class="pair">{o["pair"]}</td><td class="{side_class}">{o["side"]}</td><td>{o["type"]}</td><td>{o["qty"]}</td><td>{o["price"]}</td><td style="font-size: 11px;">{o["status"]}</td><td style="font-size: 11px; color: #8b949e;">{o["time"]}</td></tr>'
        orders_table += "</tbody></table>"
    else:
        orders_table = '<div style="text-align: center; color: #8b949e; padding: 20px;">No recent orders</div>'

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
        .status {{ font-size: 11px; color: #8b949e; }}
        .status.ok {{ color: #3fb950; }}
        .status.error {{ color: #f85149; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin-bottom: 15px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 15px; }}
        .stat {{ text-align: center; padding: 15px; }}
        .stat-value {{ font-size: 24px; font-weight: 800; color: #58a6ff; }}
        .stat-label {{ font-size: 10px; color: #8b949e; margin-top: 5px; text-transform: uppercase; letter-spacing: 0.5px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ padding: 10px; background: #0d1117; color: #8b949e; font-size: 10px; text-transform: uppercase; text-align: left; border-bottom: 1px solid #30363d; }}
        td {{ padding: 10px; border-bottom: 1px solid #30363d; }}
        tr:hover td {{ background: #1c2128; }}
        .pair {{ font-weight: 600; }}
        .buy {{ color: #3fb950; }}
        .sell {{ color: #f85149; }}
        .section-title {{ font-size: 11px; color: #8b949e; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin: 20px 0 10px 0; }}
        .error-box {{ background: rgba(248, 81, 73, 0.1); border: 1px solid #f85149; border-radius: 6px; padding: 10px; margin-bottom: 15px; color: #f85149; font-size: 12px; }}
        .success-box {{ background: rgba(63, 185, 80, 0.1); border: 1px solid #3fb950; border-radius: 6px; padding: 10px; margin-bottom: 15px; color: #3fb950; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 TradeLocker Dashboard</h1>
            <div style="text-align: right;">
                <div class="status {status_color}">{status_text}</div>
                <div style="color: #8b949e; font-size: 11px; margin-top: 5px;">{last_update}</div>
            </div>
        </div>
        {error_html}
        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{account_state['balance']}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">{account_state['equity']}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin']}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['free_margin']}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state['margin_level']}</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">{len(positions_data)}</div><div class="stat-label">Open Positions</div></div>
        </div>
        <div class="section-title">Open Positions ({len(positions_data)})</div>
        <div class="card">{positions_table}</div>
        <div class="section-title">Recent Orders (Last 20)</div>
        <div class="card">{orders_table}</div>
    </div>
</body>
</html>"""
    return html

# =====================================================================
# BOT MODE
# =====================================================================

def run_bot():
    if not (TL_EMAIL and TL_PASSWORD and TG_TOKEN and TG_CHAT):
        print("ERROR: Missing required secrets")
        return False

    client = TradeLockerClient()
    if not client.auth():
        print("ERROR: Authentication failed")
        return False

    client.load_instruments()
    messages = tg_get_messages(offset=_last_update_id)

    for msg in messages:
        text = (msg.get("text") or "").strip()
        if not looks_like_signal(text):
            continue

        parsed = parse_signal(text)
        if not parsed:
            continue

        if parsed["type"] == "SIGNAL":
            client.place_order(parsed["pair"], parsed["direction"], parsed["sl"], parsed["tp"])

        elif parsed["type"] == "TPSL_HIT":
            pair = parsed["pair"]
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
            positions = client.get_open_positions()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_sym = pos.get('instrumentName') or pos.get('symbol') or pos.get('name') or ""
                if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                    pos_id = pos.get('positionId') or pos.get('id')
                    if pos_id:
                        client.modify_position(pos_id, pos, new_sl)

    return True

# =====================================================================
# DASHBOARD MODE
# =====================================================================

def generate_dashboard():
    """
    Generates the dashboard HTML and saves it to docs/index.html.
    This file is then deployed to GitHub Pages by the workflow.
    Always writes the file — even on error — so the dashboard shows status.
    """
    print(f"[Dashboard] Starting dashboard generation at {datetime.now(timezone.utc).isoformat()}")
    print(f"[Dashboard] TL_ENV={TL_ENV}, TL_SERVER={TL_SERVER}, TL_ACCOUNT_ID={TL_ACCOUNT_ID}")

    client = TradeLockerClient()
    connected = client.auth()
    error = None

    if not connected:
        error = "Failed to authenticate. Check TL_EMAIL, TL_PASSWORD, TL_SERVER secrets."
        print(f"[Dashboard] WARNING: {error}")
    else:
        print("[Dashboard] Authenticated successfully.")
        client.load_instruments()
        print(f"[Dashboard] Loaded {len(_instruments)} instruments.")

    html = generate_dashboard_html(client, connected, error)

    # Always ensure the docs directory exists and write the file
    os.makedirs("docs", exist_ok=True)
    output_path = os.path.join("docs", "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[Dashboard] Dashboard written to {output_path} ({len(html)} bytes)")
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
        # Even on fatal error, try to write an error dashboard
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

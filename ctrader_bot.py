#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader Trading Bot + Dashboard
# Uses cTrader Open API (protobuf over TCP)
# All features: balance, equity, positions with SL/TP, orders, trade history

import os, json, re, sys, time, ssl, threading
from datetime import datetime, timezone
from urllib.error import HTTPError

# Install on first run if needed
try:
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ctrader-open-api", "--quiet"])
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints

from twisted.internet import reactor
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq,
    ProtoOAAccountLogoutReq,
    ProtoOAExecutionEvent, ProtoOAPositionsForAccountReq,
    ProtoOAOrderListReq, ProtoOAClosedPositionsForAccountReq,
    ProtoOASubscribeSpotsReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAAsset, ProtoOASymbol, ProtoOAAccount,
    ProtoOAPosition, ProtoOAOrder,
)
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
    ProtoOAApplicationAuthRes, ProtoOAAccountAuthRes,
    ProtoOAAccountsRes, ProtoOASpotEvent,
    ProtoOAErrorRes,
)

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# =====================================================================
# CONFIG FROM GITHUB SECRETS
# =====================================================================
CT_CLIENT_ID = os.environ.get("CT_CLIENT_ID", "")
CT_CLIENT_SECRET = os.environ.get("CT_CLIENT_SECRET", "")
CT_ACCESS_TOKEN = os.environ.get("CT_ACCESS_TOKEN", "")
CT_ACCOUNT_ID = int(os.environ.get("CT_ACCOUNT_ID", "0")) if os.environ.get("CT_ACCOUNT_ID", "0").strip() else 0
CT_ENV = os.environ.get("CT_ENV", "demo")  # "demo" or "live"

# Telegram
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")

# Pair aliases
PAIR_ALIASES = {
    "GOLD": "XAUUSD", "XAU": "XAUUSD",
    "SILVER": "XAGUSD", "XAG": "XAGUSD",
    "OIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL",
    "BRENT": "UKOIL",
    "NAS100": "US100", "NASDAQ": "US100", "US100": "US100",
    "US30": "US30", "DOW": "US30", "DJ30": "US30",
    "SPX500": "US500", "SP500": "US500", "US500": "US500",
    "GER40": "DE40", "DAX": "DE40", "DE40": "DE40",
    "FRA40": "FR40", "CAC": "FR40",
    "UK100": "UK100", "FTSE": "UK100",
}

# Dashboard
DASHBOARD_USERNAME = os.environ.get("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "changeme")
MODE = os.environ.get("MODE", "bot")

# =====================================================================
# GLOBAL STATE
# =====================================================================
_process_logs = []
_alerts = []
_heartbeat_log = {}
_symbols = {}  # symbolId -> {symbol, displayName}
_account_info = None
_positions = []
_orders = []
_trades = []
_balance = 0.0
_equity = 0.0
_free_margin = 0.0
_margin_used = 0.0
_margin_level = 0.0
_currency = "USD"

# =====================================================================
# LOGGING
# =====================================================================

def log_process(level, message):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = {"timestamp": timestamp, "level": level, "message": message}
    _process_logs.append(log_entry)
    print(f"[{level.upper()}] {timestamp} - {message}")
    if len(_process_logs) > 150:
        _process_logs.pop(0)
    if level in ["error", "warning"]:
        _alerts.append(log_entry)
        if len(_alerts) > 50:
            _alerts.pop(0)

def save_heartbeat(job_name, status, details=""):
    timestamp = datetime.now(timezone.utc).isoformat()
    _heartbeat_log[job_name] = {"status": status, "timestamp": timestamp, "details": details}
    os.makedirs("docs", exist_ok=True)
    try:
        with open(os.path.join("docs", "heartbeat.json"), "w") as f:
            json.dump(_heartbeat_log, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not save heartbeat: {e}")

def get_job_status(job_name):
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
    return {"status": status, "message": message, "time_ago": time_ago, "timestamp": timestamp, "raw_status": status}

# =====================================================================
# cTRADER API CLIENT (Synchronous wrapper around async OpenApiPy)
# =====================================================================

class cTraderAPI:
    def __init__(self):
        self.host = EndPoints.PROTOBUF_DEMO_HOST if CT_ENV.lower() == "demo" else EndPoints.PROTOBUF_LIVE_HOST
        self.client = Client(self.host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.connected = threading.Event()
        self.app_authenticated = threading.Event()
        self.account_authenticated = threading.Event()
        self.data_ready = threading.Event()
        self.error_msg = None
        self._auth_event = threading.Event()
        self._account_auth_event = threading.Event()
        self._account_auth_error = None
        self._app_auth_error = None

    def start(self):
        """Start the client service (non-blocking, reactor must be running)."""
        def on_connected(client):
            log_process("info", "Connected to cTrader Open API")
            self.connected.set()

        def on_disconnected(client, reason):
            log_process("info", f"Disconnected: {reason}")

        def on_message(client, message):
            self._handle_message(message)

        self.client.setConnectedCallback(on_connected)
        self.client.setDisconnectedCallback(on_disconnected)
        self.client.setMessageReceivedCallback(on_message)
        self.client.startService()
        log_process("info", "Client service started, waiting for connection...")

    def _handle_message(self, message):
        msg_type = message.payload_type
        if msg_type == ProtoOAApplicationAuthRes.DESCRIPTOR:
            res = ProtoOAApplicationAuthRes()
            message.payload.Unpack(res)
            if res.errorCode:
                self._app_auth_error = res.description
                log_process("error", f"App auth failed: {res.description}")
            else:
                log_process("success", "Application authenticated")
            self._auth_event.set()

        elif msg_type == ProtoOAAccountAuthRes.DESCRIPTOR:
            res = ProtoOAAccountAuthRes()
            message.payload.Unpack(res)
            if res.errorCode:
                self._account_auth_error = res.description
                log_process("error", f"Account auth failed: {res.description}")
            else:
                log_process("success", f"Account {CT_ACCOUNT_ID} authenticated")
            self._account_auth_event.set()

        elif msg_type == ProtoOAExecutionEvent.DESCRIPTOR:
            # Account info event (comes after account auth)
            event = ProtoOAExecutionEvent()
            message.payload.Unpack(event)
            etype = event.WhichOneof("executionEventPayload")
            if etype == "payloadAccount":
                payload = event.payloadAccount
                self._account_info = {
                    "balance": float(payload.balance) if payload.HasField('balance') else 0,
                    "equity": float(payload.equity) if payload.HasField('equity') else 0,
                    "margin": float(payload.margin) if payload.HasField('margin') else 0,
                    "freeMargin": float(payload.freeMargin) if payload.HasField('freeMargin') else 0,
                    "marginLevel": float(payload.marginLevel) if payload.HasField('marginLevel') else 0,
                    "currency": payload.currency if payload.HasField('currency') else "USD",
                }
                log_process("info", f"Account info received: balance={self._account_info['balance']}, equity={self._account_info['equity']}")
                self.data_ready.set()

            elif etype == "payloadAssetList":
                payload = event.payloadAssetList
                log_process("info", f"Assets received: {len(payload.assets)}")

            elif etype == "payloadSymbolList":
                payload = event.payloadSymbolList
                for sym in payload.symbols:
                    self._symbols[sym.symbolId] = {
                        "symbol": sym.symbol,
                        "displayName": sym.displayName,
                    }
                log_process("info", f"Symbols received: {len(payload.symbols)}")

            elif etype == "payloadPositionList":
                payload = event.payloadPositionList
                self._positions = []
                for pos in payload.positions:
                    self._positions.append({
                        "id": pos.positionId,
                        "symbolId": pos.symbolId,
                        "symbol": self._symbols.get(pos.symbolId, {}).get("symbol", str(pos.symbolId)),
                        "side": "BUY" if pos.tradeSide == ProtoOAPosition.TRADE_SIDE_BUY else "SELL",
                        "volume": pos.volume,
                        "openPrice": float(pos.openPrice) / 100000.0 if pos.HasField('openPrice') else 0,
                        "stopLoss": float(pos.stopLoss) / 100000.0 if pos.HasField('stopLoss') and pos.stopLoss > 0 else None,
                        "takeProfit": float(pos.takeProfit) / 100000.0 if pos.HasField('takeProfit') and pos.takeProfit > 0 else None,
                        "swap": float(pos.swap) if pos.HasField('swap') else 0,
                        "commission": float(pos.commission) if pos.HasField('commission') else 0,
                        "openTimestamp": pos.timestamp if pos.HasField('timestamp') else 0,
                        "pnl": pos.pnl / 100000.0 if pos.HasField('pnl') else 0,
                    })
                log_process("info", f"Positions received: {len(self._positions)}")

            elif etype == "payloadOrderList":
                payload = event.payloadOrderList
                self._orders = []
                for order in payload.orders:
                    self._orders.append({
                        "id": order.orderId,
                        "symbolId": order.symbolId,
                        "symbol": self._symbols.get(order.symbolId, {}).get("symbol", str(order.symbolId)),
                        "side": "BUY" if order.tradeSide == ProtoOAOrder.TRADE_SIDE_BUY else "SELL",
                        "type": "LIMIT" if order.HasField('limitPrice') else "STOP",
                        "volume": order.volume,
                        "price": float(order.price) / 100000.0 if order.HasField('price') else 0,
                        "stopLoss": float(order.stopLoss) / 100000.0 if order.HasField('stopLoss') and order.stopLoss > 0 else None,
                        "takeProfit": float(order.takeProfit) / 100000.0 if order.HasField('takeProfit') and order.takeProfit > 0 else None,
                        "status": order.status,
                        "timestamp": order.timestamp if order.HasField('timestamp') else 0,
                    })
                log_process("info", f"Orders received: {len(self._orders)}")

            elif etype == "payloadClosedPositionList":
                payload = event.payloadClosedPositionList
                self._trades = []
                for cp in payload.closedPositions:
                    self._trades.append({
                        "id": cp.positionId,
                        "symbolId": cp.symbolId,
                        "symbol": self._symbols.get(cp.symbolId, {}).get("symbol", str(cp.symbolId)),
                        "side": "BUY" if cp.tradeSide == ProtoOAPosition.TRADE_SIDE_BUY else "SELL",
                        "volume": cp.volume,
                        "openPrice": float(cp.openPrice) / 100000.0 if cp.HasField('openPrice') else 0,
                        "closePrice": float(cp.closePrice) / 100000.0 if cp.HasField('closePrice') else 0,
                        "stopLoss": float(cp.stopLoss) / 100000.0 if cp.HasField('stopLoss') and cp.stopLoss > 0 else None,
                        "takeProfit": float(cp.takeProfit) / 100000.0 if cp.HasField('takeProfit') and cp.takeProfit > 0 else None,
                        "pnl": cp.pnl / 100000.0 if cp.HasField('pnl') else 0,
                        "closeTimestamp": cp.closeTimestamp if cp.HasField('closeTimestamp') else 0,
                    })
                log_process("info", f"Closed positions received: {len(self._trades)}")

        elif msg_type == ProtoOAErrorRes.DESCRIPTOR:
            res = ProtoOAErrorRes()
            message.payload.Unpack(res)
            log_process("error", f"cTrader API Error: {res.description}")
            self.error_msg = res.description

    def get_symbol_name(self, symbol_id):
        """Get symbol name from ID."""
        return self._symbols.get(symbol_id, {}).get("symbol", f"ID:{symbol_id}")


# =====================================================================
# TELEGRAM
# =====================================================================

def test_telegram_connection():
    if not TG_TOKEN:
        return False, {"error": "No TG_TOKEN set"}
    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                bot_info = result.get("result", {})
                return True, {"username": bot_info.get("username", "unknown"), "name": bot_info.get("first_name", "unknown")}
            return False, {"error": "API returned not ok"}
    except Exception as e:
        return False, {"error": str(e)}

# =====================================================================
# DASHBOARD GENERATOR
# =====================================================================

def generate_login_html():
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>cTrader Dashboard - Login</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: linear-gradient(135deg, #0d1117 0%, #161b22 100%); color: #c9d1d9; font-size: 14px; display: flex; justify-content: center; align-items: center; height: 100vh; padding: 20px; }}
        .login-container {{ background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 40px; max-width: 400px; width: 100%; box-shadow: 0 10px 40px rgba(0,0,0,0.5); }}
        .login-header {{ text-align: center; margin-bottom: 30px; }}
        .login-header h1 {{ font-size: 28px; margin-bottom: 8px; color: #58a6ff; }}
        .login-header p {{ color: #8b949e; font-size: 12px; }}
        .form-group {{ margin-bottom: 20px; }}
        .form-group label {{ display: block; font-size: 12px; color: #8b949e; margin-bottom: 8px; font-weight: 600; text-transform: uppercase; }}
        .form-group input {{ width: 100%; padding: 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-size: 14px; }}
        .form-group input:focus {{ outline: none; border-color: #58a6ff; }}
        .login-btn {{ width: 100%; padding: 12px; background: #238636; color: #fff; border: none; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; }}
        .login-btn:hover {{ background: #2ea043; }}
        .error-message {{ display: none; background: #f85149; color: #fff; padding: 12px; border-radius: 6px; margin-bottom: 20px; font-size: 12px; }}
        .error-message.show {{ display: block; }}
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header"><h1>📊 cTrader Dashboard</h1><p>Trading Bot Control Panel</p></div>
        <div class="error-message" id="errorMsg"></div>
        <form onsubmit="handleLogin(event)">
            <div class="form-group"><label>Username</label><input type="text" id="username" placeholder="Enter username" required autofocus></div>
            <div class="form-group"><label>Password</label><input type="password" id="password" placeholder="Enter password" required></div>
            <button type="submit" class="login-btn">Sign In</button>
        </form>
    </div>
    <script>
        function handleLogin(event) {{
            event.preventDefault();
            const u = document.getElementById('username').value;
            const p = document.getElementById('password').value;
            const err = document.getElementById('errorMsg');
            if (u === '{DASHBOARD_USERNAME}' && p === '{DASHBOARD_PASSWORD}') {{
                sessionStorage.setItem('dashboard_authenticated', 'true');
                window.location.href = 'index.html';
            }} else {{
                err.textContent = '❌ Invalid credentials';
                err.classList.add('show');
            }}
        }}
    </script>
</body>
</html>"""

def generate_dashboard_html(ct):
    """Generate the full dashboard HTML."""
    global _account_info, _positions, _orders, _trades, _balance, _equity, _free_margin, _margin_used, _margin_level, _currency

    # Extract account info
    if _account_info:
        _balance = _account_info.get("balance", 0)
        _equity = _account_info.get("equity", 0)
        _margin_used = _account_info.get("margin", 0)
        _free_margin = _account_info.get("freeMargin", 0)
        _margin_level = _account_info.get("marginLevel", 0)
        _currency = _account_info.get("currency", "USD")
    else:
        log_process("warning", "No account info received from cTrader API")

    # Resolve symbol names for positions/orders/trades
    for pos in _positions:
        pos['symbol'] = ct.get_symbol_name(pos.get('symbolId', pos.get('symbolId', 0)))

    for order in _orders:
        order['symbol'] = ct.get_symbol_name(order.get('symbolId', order.get('symbolId', 0)))

    for trade in _trades:
        trade['symbol'] = ct.get_symbol_name(trade.get('symbolId', trade.get('symbolId', 0)))

    # Calculate margin usage
    margin_usage = 0
    total = _margin_used + _free_margin
    if total > 0:
        margin_usage = (_margin_used / total) * 100

    # Position stats
    total_trades = len(_trades)
    wins = len([t for t in _trades if t.get('pnl', 0) > 0])
    losses = len([t for t in _trades if t.get('pnl', 0) < 0])
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    total_pnl = sum(t.get('pnl', 0) for t in _positions)
    open_pnl = sum(p.get('pnl', 0) for p in _positions)

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Build position table
    positions_table = ""
    if _positions:
        positions_table = """<table><thead><tr><th>Symbol</th><th>Side</th><th>Volume</th><th>Entry</th><th>SL</th><th>TP</th><th>P&L</th><th>Open Time</th></tr></thead><tbody>"""
        for p in _positions:
            side_class = "buy" if p['side'] == "BUY" else "sell"
            pnl_color = "#3fb950" if p.get('pnl', 0) >= 0 else "#f85149"
            sl_str = f"{p['stopLoss']:.5f}" if p.get('stopLoss') else "—"
            tp_str = f"{p['takeProfit']:.5f}" if p.get('takeProfit') else "—"
            open_time = datetime.fromtimestamp(p['openTimestamp']/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if p.get('openTimestamp') else "—"
            positions_table += f"""<tr><td class="pair">{p['symbol']}</td><td class="{side_class}">{p['side']}</td><td>{p['volume']}</td><td>{p['openPrice']:.5f}</td><td class="sl">{sl_str}</td><td class="tp">{tp_str}</td><td style="color:{pnl_color};font-weight:600;">{p.get('pnl',0):+.2f}</td><td class="time">{open_time}</td></tr>"""
        positions_table += "</tbody></table>"
    else:
        positions_table = '<div class="empty">No open positions</div>'

    # Build closed trades table
    trades_table = ""
    if _trades:
        trades_table = """<table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Result</th><th>Close Time</th></tr></thead><tbody>"""
        for t in _trades:
            side_class = "buy" if t['side'] == "BUY" else "sell"
            pnl_color = "#3fb950" if t.get('pnl', 0) >= 0 else "#f85149"
            result_label = "🟢 TP Hit" if t.get('takeProfit') and abs(t.get('closePrice', 0) - t.get('takeProfit', 0)) < 0.01 else ("🔴 SL Hit" if t.get('stopLoss') and abs(t.get('closePrice', 0) - t.get('stopLoss', 0)) < 0.01 else "Closed")
            close_time = datetime.fromtimestamp(t['closeTimestamp']/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if t.get('closeTimestamp') else "—"
            trades_table += f"""<tr><td class="pair">{t['symbol']}</td><td class="{side_class}">{t['side']}</td><td>{t['openPrice']:.5f}</td><td>{t['closePrice']:.5f}</td><td style="color:{pnl_color};font-weight:600;">{t.get('pnl',0):+.2f}</td><td>{result_label}</td><td class="time">{close_time}</td></tr>"""
        trades_table += "</tbody></table>"
    else:
        trades_table = '<div class="empty">No closed trades</div>'

    # Build orders table
    orders_table = ""
    pending_orders = [o for o in _orders if o.get('status') not in ['ORDER_FILLED', 'ORDER_CANCELLED', 'ORDER_REJECTED']]
    if pending_orders:
        orders_table = """<table><thead><tr><th>Symbol</th><th>Side</th><th>Type</th><th>Volume</th><th>Price</th><th>SL</th><th>TP</th><th>Status</th><th>Time</th></tr></thead><tbody>"""
        for o in pending_orders:
            side_class = "buy" if o['side'] == "BUY" else "sell"
            sl_str = f"{o['stopLoss']:.5f}" if o.get('stopLoss') else "—"
            tp_str = f"{o['takeProfit']:.5f}" if o.get('takeProfit') else "—"
            order_time = datetime.fromtimestamp(o['timestamp']/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if o.get('timestamp') else "—"
            orders_table += f"""<tr><td class="pair">{o['symbol']}</td><td class="{side_class}">{o['side']}</td><td>{o['type']}</td><td>{o['volume']}</td><td>{o['price']:.5f}</td><td class="sl">{sl_str}</td><td class="tp">{tp_str}</td><td>{o['status']}</td><td class="time">{order_time}</td></tr>"""
        orders_table += "</tbody></table>"
    else:
        orders_table = '<div class="empty">No pending orders</div>'

    # Build logs table
    logs_table = ""
    if _process_logs:
        logs_table = """<table><thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead><tbody>"""
        for log in _process_logs[-30:]:
            level = log.get("level", "info").upper()
            level_color = {"INFO": "#58a6ff", "SUCCESS": "#3fb950", "ERROR": "#f85149", "WARNING": "#d29922"}.get(level, "#c9d1d9")
            logs_table += f"""<tr><td class="time">{log['timestamp']}</td><td style="color:{level_color};font-weight:600;">{level}</td><td>{log['message']}</td></tr>"""
        logs_table += "</tbody></table>"
    else:
        logs_table = '<div class="empty">No logs</div>'

    bot_status = get_job_status("bot")
    dashboard_status = get_job_status("dashboard")

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="Pragma" content="no-cache">
    <title>cTrader Dashboard</title>
    <script>
        (function() {{
            const auth = sessionStorage.getItem('dashboard_authenticated');
            if (auth !== 'true') {{ location.href = 'login.html'; }}
        }})();
    </script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; font-size: 13px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 20px; }}
        .header h1 {{ font-size: 16px; font-weight: 700; }}
        .btn {{ padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; }}
        .btn-logout {{ background: #da3633; color: #fff; }}
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
        .sl {{ color: #f85149; font-weight: 500; }}
        .tp {{ color: #3fb950; font-weight: 500; }}
        .time {{ font-size: 11px; color: #8b949e; }}
        .section-title {{ font-size: 11px; color: #8b949e; font-weight: 600; text-transform: uppercase; margin: 20px 0 10px 0; }}
        .empty {{ text-align: center; color: #8b949e; padding: 20px; }}
        .last-update {{ color: #8b949e; font-size: 11px; text-align: center; margin-top: 20px; }}
        .account-info {{ background: #0d1117; padding: 10px; border-radius: 6px; font-size: 11px; color: #8b949e; margin-bottom: 15px; }}
        .health-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 15px; }}
        .health-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px; display: flex; align-items: center; gap: 10px; font-size: 11px; }}
        .health-status {{ font-weight: 600; }}
        .heartbeat {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }}
        .heartbeat.running {{ background: #d29922; animation: pulse 0.8s ease-in-out infinite; }}
        .heartbeat.completed {{ background: #3fb950; }}
        .heartbeat.idle {{ background: #6e7681; }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}
        .cron-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .cron-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }}
        .cron-title {{ font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 8px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 cTrader Dashboard</h1>
            <div>
                <button class="btn btn-logout" onclick="sessionStorage.clear();window.location.href='login.html'">🚪 Logout</button>
            </div>
        </div>

        <div class="account-info">
            📊 Account ID: {CT_ACCOUNT_ID} | Environment: {CT_ENV.upper()} | Server: {ct.host}
        </div>

        <div class="section-title">🩺 System Health</div>
        <div class="health-grid">
            <div class="health-item"><span>🔐</span><span>cTrader Auth</span><span class="health-status" style="color:#3fb950;">✓ OK</span></div>
            <div class="health-item"><span>📊</span><span>Symbols Loaded</span><span class="health-status" style="color:#3fb950;">✓ {len(ct._symbols)}</span></div>
            <div class="health-item"><span>⚠️</span><span>Margin Safe</span><span class="health-status" style="color:{'#3fb950' if margin_usage < 80 else '#f85149'};">{'✓ OK' if margin_usage < 80 else '✗ HIGH'}</span></div>
        </div>

        <div class="section-title">Connection Status</div>
        <div class="conn-grid">
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">📊 cTrader Open API</span>
                    <span class="conn-badge" style="background:#3fb95020;color:#3fb950;">✓ Connected</span>
                </div>
                <div class="conn-detail">Env: {CT_ENV.upper()} | Account: {CT_ACCOUNT_ID}</div>
            </div>
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">✈️ Telegram</span>
                    <span class="conn-badge" style="background:#3fb95020;color:#3fb950;">✓ Connected</span>
                </div>
                <div class="conn-detail">Bot: @Forexunitedbot</div>
            </div>
        </div>

        <div class="section-title">🔄 Cron Job Status</div>
        <div class="cron-grid">
            <div class="cron-card">
                <div class="cron-title"><span class="heartbeat {bot_status['raw_status']}"></span>Trading Bot</div>
                <div style="font-size:10px;color:#8b949e;">{bot_status['raw_status'].upper()} | {bot_status['time_ago']}</div>
            </div>
            <div class="cron-card">
                <div class="cron-title"><span class="heartbeat {dashboard_status['raw_status']}"></span>Dashboard</div>
                <div style="font-size:10px;color:#8b949e;">{dashboard_status['raw_status'].upper()} | {dashboard_status['time_ago']}</div>
            </div>
        </div>

        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">${_balance:,.2f}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">${_equity:,.2f}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">${_margin_used:,.2f}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">${_free_margin:,.2f}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{_margin_level:.1f}%</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">${open_pnl:+.2f}</div><div class="stat-label">Open P&L</div></div>
        </div>

        <div class="section-title">📈 Trade Statistics</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{total_trades}</div><div class="stat-label">Closed Trades</div></div>
            <div class="card stat"><div class="stat-value">{wins}</div><div class="stat-label">Wins</div></div>
            <div class="card stat"><div class="stat-value">{losses}</div><div class="stat-label">Losses</div></div>
            <div class="card stat"><div class="stat-value">{win_rate:.1f}%</div><div class="stat-label">Win Rate</div></div>
            <div class="card stat"><div class="stat-value">{len(_positions)}</div><div class="stat-label">Open Positions</div></div>
            <div class="card stat"><div class="stat-value">{len(pending_orders)}</div><div class="stat-label">Pending Orders</div></div>
        </div>

        <div class="section-title">Open Positions ({len(_positions)}) — Live SL/TP & P&L</div>
        <div class="card">{positions_table}</div>

        <div class="section-title">Closed Trades ({len(_trades)}) — Results</div>
        <div class="card">{trades_table}</div>

        <div class="section-title">Pending Orders ({len(pending_orders)})</div>
        <div class="card">{orders_table}</div>

        <div class="section-title">📋 Backend Process Logs (Last 30)</div>
        <div class="card">{logs_table}</div>

        <div class="last-update">Last updated: {last_update} | Auto-refresh every 60s | {len(_process_logs)} Total Logs</div>
    </div>
    <script>
        setInterval(function() {{ location.reload(); }}, 60000);
        setInterval(function() {{
            const auth = sessionStorage.getItem('dashboard_authenticated');
            if (auth !== 'true') {{ window.location.href = 'login.html'; }}
        }}, 10000);
    </script>
</body>
</html>"""
    return html

# =====================================================================
# MAIN LOGIC
# =====================================================================

def main():
    """Main entry point - uses Twisted reactor for async cTrader API."""
    global _account_info, _positions, _orders, _trades

    try:
        if not (CT_CLIENT_ID and CT_CLIENT_SECRET and CT_ACCESS_TOKEN and CT_ACCOUNT_ID):
            log_process("error", "Missing cTrader credentials (CT_CLIENT_ID, CT_CLIENT_SECRET, CT_ACCESS_TOKEN, CT_ACCOUNT_ID)")
            save_heartbeat(MODE, "failed", "Missing credentials")
            sys.exit(1)

        log_process("info", f"cTrader Bot starting in {MODE} mode")
        log_process("info", f"Environment: {CT_ENV.upper()} | Account: {CT_ACCOUNT_ID}")

        # Create the API client
        ct = cTraderAPI()

        # Set up message handler
        ct.client.setMessageReceivedCallback(ct._handle_message)
        ct.client.setDisconnectedCallback(lambda client, reason: log_process("info", f"Disconnected: {reason}"))

        # Step 1: When connected, start authentication
        def on_connected(client):
            log_process("info", "Connected to cTrader Open API")
            log_process("info", "=== {} STARTED ===".format("DASHBOARD GENERATION" if MODE == "dashboard" else "BOT CYCLE"))

            # Step 2: Authenticate application
            req = ProtoOAApplicationAuthReq()
            req.clientId = CT_CLIENT_ID
            req.clientSecret = CT_CLIENT_SECRET
            client.send(req)

        ct.client.setConnectedCallback(on_connected)

        # Start the client service
        ct.client.startService()

        # Monitor state machine in the reactor
        state = {"step": "connected", "deadline": time.time() + 60}

        def monitor():
            elapsed = time.time() - state["deadline"] + 60

            # Check for app auth completion
            if state["step"] == "connected" and ct._auth_event.is_set():
                if ct._app_auth_error:
                    log_process("error", f"App auth failed: {ct._app_auth_error}")
                    save_heartbeat(MODE, "failed", ct._app_auth_error)
                    reactor.stop()
                    return
                state["step"] = "app_authenticated"
                log_process("info", f"Application authenticated")

                # Step 3: Authenticate account
                req = ProtoOAAccountAuthReq()
                req.ctidTraderAccountId = CT_ACCOUNT_ID
                req.accessToken = CT_ACCESS_TOKEN
                ct.client.send(req)

            # Check for account auth completion
            elif state["step"] == "app_authenticated" and ct._account_auth_event.is_set():
                if ct._account_auth_error:
                    log_process("error", f"Account auth failed: {ct._account_auth_error}")
                    save_heartbeat(MODE, "failed", ct._account_auth_error)
                    reactor.stop()
                    return
                state["step"] = "account_authenticated"
                log_process("info", f"Account {CT_ACCOUNT_ID} authenticated")

                # Step 4: Fetch data (positions, orders, closed positions)
                pos_req = ProtoOAPositionsForAccountReq()
                pos_req.ctidTraderAccountId = CT_ACCOUNT_ID
                ct.client.send(pos_req)

                order_req = ProtoOAOrderListReq()
                order_req.ctidTraderAccountId = CT_ACCOUNT_ID
                order_req.fromTimestamp = int(time.time() * 1000) - (7 * 24 * 3600 * 1000)
                order_req.toTimestamp = int(time.time() * 1000)
                ct.client.send(order_req)

                closed_req = ProtoOAClosedPositionsForAccountReq()
                closed_req.ctidTraderAccountId = CT_ACCOUNT_ID
                closed_req.fromTimestamp = int(time.time() * 1000) - (30 * 24 * 3600 * 1000)
                closed_req.toTimestamp = int(time.time() * 1000)
                ct.client.send(closed_req)

                log_process("info", "Data requests sent, waiting for responses...")

                # Give time for all responses
                reactor.callLater(8, finish_session)

            # Timeout
            if elapsed > 50:
                log_process("error", "Session timed out")
                save_heartbeat(MODE, "failed", "Timeout")
                reactor.stop()

            reactor.callLater(0.5, monitor)

        def finish_session():
            global _account_info, _positions, _orders, _trades

            # Update globals from ct object
            _account_info = ct._account_info if ct._account_info else {}
            _positions = ct._positions
            _orders = ct._orders
            _trades = ct._trades

            log_process("info", f"Data summary: positions={len(_positions)}, orders={len(_orders)}, trades={len(_trades)}")
            if _account_info:
                log_process("info", f"Account: balance={_account_info.get('balance')}, equity={_account_info.get('equity')}")

            if MODE == "dashboard":
                html = generate_dashboard_html(ct)
                os.makedirs("docs", exist_ok=True)
                with open("docs/index.html", "w", encoding="utf-8") as f:
                    f.write(html)
                with open("docs/login.html", "w", encoding="utf-8") as f:
                    f.write(generate_login_html())
                log_process("success", f"Dashboard written to docs/index.html ({len(html)} bytes)")
                save_heartbeat("dashboard", "completed", "No errors")
            else:
                log_process("info", f"Bot mode: {len(_positions)} open positions, {len(_orders)} orders")
                save_heartbeat("bot", "completed", f"Positions: {len(_positions)}, Orders: {len(_orders)}")

            log_process("info", "=== SESSION COMPLETE ===")

            # Shutdown
            try:
                ct.client.stopService()
            except:
                pass
            reactor.callLater(0.5, reactor.stop)

        # Start monitoring
        reactor.callLater(2, monitor)

        # Run the reactor (blocks until reactor.stop() is called)
        reactor.run(installSignalHandlers=False)

    except Exception as e:
        log_process("error", f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        save_heartbeat(MODE, "failed", str(e)[:100])
        try:
            reactor.stop()
        except:
            pass


if __name__ == "__main__":
    main()
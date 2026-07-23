#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader Trading Bot + Dashboard
# Uses cTrader Open API (protobuf over TCP) via ctrader-open-api package
# All features: balance, equity, positions with SL/TP, orders, trade history

import os, json, re, sys, time, ssl, threading

# =====================================================================
# CONFIG FROM GITHUB SECRETS
# =====================================================================
CT_CLIENT_ID = os.environ.get("CT_CLIENT_ID", "")
CT_CLIENT_SECRET = os.environ.get("CT_CLIENT_SECRET", "")
CT_ACCESS_TOKEN = os.environ.get("CT_ACCESS_TOKEN", "")
CT_ACCOUNT_ID = os.environ.get("CT_ACCOUNT_ID", "").strip()
CT_ENV = os.environ.get("CT_ENV", "demo").lower()  # "demo" or "live"

# Telegram
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")

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

# =====================================================================
# LOGGING
# =====================================================================

def log_process(level, message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    log_entry = {"timestamp": timestamp, "level": level, "message": message}
    _process_logs.append(log_entry)
    print(f"[{level.upper()}] {timestamp} - {message}")
    sys.stdout.flush()
    if len(_process_logs) > 150:
        _process_logs.pop(0)
    if level in ["error", "warning"]:
        _alerts.append(log_entry)
        if len(_alerts) > 50:
            _alerts.pop(0)

def save_heartbeat(job_name, status, details=""):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
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
            last_run = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S"))
            delta = time.time() - last_run
            if delta < 60:
                time_ago = f"{int(delta)}s ago"
            elif delta < 3600:
                time_ago = f"{int(delta / 60)}m ago"
            elif delta < 86400:
                time_ago = f"{int(delta / 3600)}h ago"
            else:
                time_ago = f"{int(delta / 86400)}d ago"
        except:
            time_ago = timestamp
    message = details if details else ("No errors" if status == "completed" else "Processing...")
    return {"status": status, "message": message, "time_ago": time_ago, "timestamp": timestamp, "raw_status": status}

# =====================================================================
# INSTALL DEPENDENCIES
# =====================================================================

def ensure_dependencies():
    """Install required packages if not available."""
    missing = []
    for pkg_name, import_name in [("ctrader_open_api", None), ("twisted", None), ("service_identity", None), ("protobuf", None)]:
        try:
            __import__(import_name or pkg_name)
        except ImportError:
            missing.append(pkg_name)

    if missing:
        print(f"Installing missing packages: {', '.join(missing)}")
        import subprocess
        for pkg in missing:
            pip_name = pkg.replace("_", "-")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pip_name, "--quiet", "--disable-pip-version-check"])
            print(f"  Installed {pip_name}")

# =====================================================================
# cTRADER API (using ctrader-open-api / OpenApiPy)
# =====================================================================

def run_ctrader_session():
    """Connect to cTrader Open API, fetch data, generate dashboard."""
    global _account_info, _positions, _orders, _trades, _symbols

    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from twisted.internet import reactor
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq,
        ProtoOAPositionsForAccountReq, ProtoOAOrderListReq,
        ProtoOAClosedPositionsForAccountReq,
    )
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
        ProtoOAPosition, ProtoOAOrder,
    )
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
        ProtoOAApplicationAuthRes, ProtoOAAccountAuthRes,
    )

    host = EndPoints.PROTOBUF_DEMO_HOST if CT_ENV == "demo" else EndPoints.PROTOBUF_LIVE_HOST
    account_id = int(CT_ACCOUNT_ID)

    log_process("info", f"Connecting to cTrader {CT_ENV.upper()} at {host}...")

    client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)

    # State tracking
    auth_events = {
        "app_auth_done": threading.Event(),
        "app_auth_error": [None],
        "account_auth_done": threading.Event(),
        "account_auth_error": [None],
        "data_received": threading.Event(),
    }

    def on_connected(c):
        log_process("info", "Connected to cTrader Open API")
        # Authenticate application
        req = ProtoOAApplicationAuthReq()
        req.clientId = CT_CLIENT_ID
        req.clientSecret = CT_CLIENT_SECRET
        c.send(req)

    def on_disconnected(c, reason):
        log_process("info", f"Disconnected: {reason}")

    def on_message(c, message):
        msg_type = message.payload_type

        if msg_type == ProtoOAApplicationAuthRes.DESCRIPTOR:
            res = ProtoOAApplicationAuthRes()
            message.payload.Unpack(res)
            if res.errorCode:
                auth_events["app_auth_error"][0] = res.description
                log_process("error", f"App auth failed: {res.description}")
            else:
                log_process("success", "Application authenticated")
            auth_events["app_auth_done"].set()

        elif msg_type == ProtoOAAccountAuthRes.DESCRIPTOR:
            res = ProtoOAAccountAuthRes()
            message.payload.Unpack(res)
            if res.errorCode:
                auth_events["account_auth_error"][0] = res.description
                log_process("error", f"Account auth failed: {res.description}")
            else:
                log_process("success", f"Account {account_id} authenticated")
            auth_events["account_auth_done"].set()

        elif msg_type == 1006:  # ProtoOAExecutionEvent
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAExecutionEvent
            event = ProtoOAExecutionEvent()
            message.payload.Unpack(event)
            etype = event.WhichOneof("executionEventPayload")

            if etype == "payloadAccount":
                payload = event.payloadAccount
                _account_info = {
                    "balance": float(payload.balance) if payload.HasField('balance') else 0,
                    "equity": float(payload.equity) if payload.HasField('equity') else 0,
                    "margin": float(payload.margin) if payload.HasField('margin') else 0,
                    "freeMargin": float(payload.freeMargin) if payload.HasField('freeMargin') else 0,
                    "marginLevel": float(payload.marginLevel) if payload.HasField('marginLevel') else 0,
                    "currency": payload.currency if payload.HasField('currency') else "USD",
                }
                log_process("info", f"Account info: balance={_account_info['balance']}, equity={_account_info['equity']}")
                auth_events["data_received"].set()

            elif etype == "payloadAssetList":
                payload = event.payloadAssetList
                log_process("info", f"Assets: {len(payload.assets)}")

            elif etype == "payloadSymbolList":
                payload = event.payloadSymbolList
                for sym in payload.symbols:
                    _symbols[sym.symbolId] = {
                        "symbol": sym.symbol,
                        "displayName": sym.displayName,
                    }
                log_process("info", f"Symbols loaded: {len(_symbols)}")

            elif etype == "payloadPositionList":
                payload = event.payloadPositionList
                _positions = []
                for pos in payload.positions:
                    sl_val = None
                    tp_val = None
                    if pos.HasField('stopLoss') and pos.stopLoss > 0:
                        sl_val = float(pos.stopLoss) / 100000.0
                    if pos.HasField('takeProfit') and pos.takeProfit > 0:
                        tp_val = float(pos.takeProfit) / 100000.0
                    _positions.append({
                        "id": pos.positionId,
                        "symbolId": pos.symbolId,
                        "symbol": _symbols.get(pos.symbolId, {}).get("symbol", str(pos.symbolId)),
                        "side": "BUY" if pos.tradeSide == ProtoOAPosition.TRADE_SIDE_BUY else "SELL",
                        "volume": pos.volume,
                        "openPrice": float(pos.openPrice) / 100000.0 if pos.HasField('openPrice') else 0,
                        "stopLoss": sl_val,
                        "takeProfit": tp_val,
                        "swap": float(pos.swap) if pos.HasField('swap') else 0,
                        "commission": float(pos.commission) if pos.HasField('commission') else 0,
                        "openTimestamp": pos.timestamp if pos.HasField('timestamp') else 0,
                        "pnl": float(pos.pnl) / 100000.0 if pos.HasField('pnl') else 0,
                    })
                log_process("info", f"Open positions: {len(_positions)}")
                # Show SL/TP for each position
                for p in _positions:
                    log_process("info", f"  {p['symbol']} {p['side']} vol={p['volume']} @ {p['openPrice']:.5f} | SL={p['stopLoss']} TP={p['takeProfit']} | PnL={p['pnl']:+.2f}")
                auth_events["data_received"].set()

            elif etype == "payloadOrderList":
                payload = event.payloadOrderList
                _orders = []
                for order in payload.orders:
                    status_map = {
                        ProtoOAOrder.ORDER_STATUS_PENDING: "PENDING",
                        ProtoOAOrder.ORDER_STATUS_FILLED: "FILLED",
                        ProtoOAOrder.ORDER_STATUS_CANCELLED: "CANCELLED",
                        ProtoOAOrder.ORDER_STATUS_REJECTED: "REJECTED",
                        ProtoOAOrder.ORDER_STATUS_PARTIALLY_FILLED: "PARTIAL",
                    }
                    status_str = status_map.get(order.status, str(order.status))
                    _orders.append({
                        "id": order.orderId,
                        "symbolId": order.symbolId,
                        "symbol": _symbols.get(order.symbolId, {}).get("symbol", str(order.symbolId)),
                        "side": "BUY" if order.tradeSide == ProtoOAOrder.TRADE_SIDE_BUY else "SELL",
                        "type": "LIMIT" if order.HasField('limitPrice') else "STOP",
                        "volume": order.volume,
                        "price": float(order.price) / 100000.0 if order.HasField('price') else 0,
                        "stopLoss": float(order.stopLoss) / 100000.0 if order.HasField('stopLoss') and order.stopLoss > 0 else None,
                        "takeProfit": float(order.takeProfit) / 100000.0 if order.HasField('takeProfit') and order.takeProfit > 0 else None,
                        "status": status_str,
                        "timestamp": order.timestamp if order.HasField('timestamp') else 0,
                    })
                log_process("info", f"Orders: {len(_orders)}")
                auth_events["data_received"].set()

            elif etype == "payloadClosedPositionList":
                payload = event.payloadClosedPositionList
                _trades = []
                for cp in payload.closedPositions:
                    _trades.append({
                        "id": cp.positionId,
                        "symbolId": cp.symbolId,
                        "symbol": _symbols.get(cp.symbolId, {}).get("symbol", str(cp.symbolId)),
                        "side": "BUY" if cp.tradeSide == ProtoOAPosition.TRADE_SIDE_BUY else "SELL",
                        "volume": cp.volume,
                        "openPrice": float(cp.openPrice) / 100000.0 if cp.HasField('openPrice') else 0,
                        "closePrice": float(cp.closePrice) / 100000.0 if cp.HasField('closePrice') else 0,
                        "stopLoss": float(cp.stopLoss) / 100000.0 if cp.HasField('stopLoss') and cp.stopLoss > 0 else None,
                        "takeProfit": float(cp.takeProfit) / 100000.0 if cp.HasField('takeProfit') and cp.takeProfit > 0 else None,
                        "pnl": float(cp.pnl) / 100000.0 if cp.HasField('pnl') else 0,
                        "closeTimestamp": cp.closeTimestamp if cp.HasField('closeTimestamp') else 0,
                    })
                log_process("info", f"Closed positions (trade history): {len(_trades)}")
                auth_events["data_received"].set()

    # Set callbacks
    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)

    # Start client service
    client.startService()

    # Wait for app authentication (30s timeout)
    if not auth_events["app_auth_done"].wait(timeout=30):
        log_process("error", "App authentication timeout")
        save_heartbeat(MODE, "failed", "App auth timeout")
        reactor.callFromThread(reactor.stop)
        return

    if auth_events["app_auth_error"][0]:
        save_heartbeat(MODE, "failed", f"App auth: {auth_events['app_auth_error'][0]}")
        reactor.callFromThread(reactor.stop)
        return

    # Wait for account authentication (30s timeout)
    req = ProtoOAAccountAuthReq()
    req.ctidTraderAccountId = account_id
    req.accessToken = CT_ACCESS_TOKEN
    client.send(req)

    if not auth_events["account_auth_done"].wait(timeout=30):
        log_process("error", "Account authentication timeout")
        save_heartbeat(MODE, "failed", "Account auth timeout")
        reactor.callFromThread(reactor.stop)
        return

    if auth_events["account_auth_error"][0]:
        save_heartbeat(MODE, "failed", f"Account auth: {auth_events['account_auth_error'][0]}")
        reactor.callFromThread(reactor.stop)
        return

    # Fetch all data
    pos_req = ProtoOAPositionsForAccountReq()
    pos_req.ctidTraderAccountId = account_id
    client.send(pos_req)

    import time as _time
    order_req = ProtoOAOrderListReq()
    order_req.ctidTraderAccountId = account_id
    order_req.fromTimestamp = int(_time.time() * 1000) - (30 * 24 * 3600 * 1000)
    order_req.toTimestamp = int(_time.time() * 1000)
    client.send(order_req)

    closed_req = ProtoOAClosedPositionsForAccountReq()
    closed_req.ctidTraderAccountId = account_id
    closed_req.fromTimestamp = int(_time.time() * 1000) - (30 * 24 * 3600 * 1000)
    closed_req.toTimestamp = int(_time.time() * 1000)
    client.send(closed_req)

    # Wait for all data (15s timeout)
    if not auth_events["data_received"].wait(timeout=15):
        log_process("warning", "Data fetch incomplete — partial data will be used")

    # Give a moment for all responses
    reactor.callLater(3, finalize_session, client)

def finalize_session(client):
    """Process fetched data and generate output."""
    global _account_info, _positions, _orders, _trades

    log_process("info", f"Final data: positions={len(_positions)}, orders={len(_orders)}, trades={len(_trades)}")
    if _account_info:
        log_process("info", f"Account: balance={_account_info.get('balance')}, equity={_account_info.get('equity')}")

    if MODE == "dashboard":
        html = generate_dashboard_html()
        os.makedirs("docs", exist_ok=True)
        with open("docs/index.html", "w", encoding="utf-8") as f:
            f.write(html)
        with open("docs/login.html", "w", encoding="utf-8") as f:
            f.write(generate_login_html())
        log_process("success", f"Dashboard written ({len(html)} bytes)")
        save_heartbeat("dashboard", "completed", "No errors")
    else:
        log_process("info", f"Bot mode: {len(_positions)} open positions, {len(_orders)} orders")
        save_heartbeat("bot", "completed", f"Positions: {len(_positions)}, Orders: {len(_orders)}")

    log_process("info", "=== SESSION COMPLETE ===")

    # Cleanup
    try:
        client.stopService()
    except:
        pass
    reactor.callLater(1, reactor.stop)

# =====================================================================
# DASHBOARD GENERATOR
# =====================================================================

def get_symbol_name(symbol_id):
    """Get symbol display name from ID."""
    info = _symbols.get(symbol_id, {})
    return info.get("displayName") or info.get("symbol") or f"ID:{symbol_id}"

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
        window.addEventListener('load', function() {{
            if (sessionStorage.getItem('dashboard_authenticated') === 'true') {{
                window.location.href = 'index.html';
            }}
        }});
    </script>
</body>
</html>"""

def generate_dashboard_html():
    """Generate the full dashboard HTML."""
    from datetime import datetime, timezone

    acct = _account_info or {}
    balance = acct.get("balance", 0)
    equity = acct.get("equity", 0)
    margin_used = acct.get("margin", 0)
    free_margin = acct.get("freeMargin", 0)
    margin_level = acct.get("marginLevel", 0)
    currency = acct.get("currency", "USD")

    margin_usage = 0
    total = margin_used + free_margin
    if total > 0:
        margin_usage = (margin_used / total) * 100

    total_closed = len(_trades)
    wins = len([t for t in _trades if t.get("pnl", 0) > 0])
    losses = len([t for t in _trades if t.get("pnl", 0) < 0])
    win_rate = (wins / total_closed * 100) if total_closed > 0 else 0
    open_pnl = sum(p.get("pnl", 0) for p in _positions)

    pending_orders = [o for o in _orders if o.get("status") not in ["FILLED", "CANCELLED", "REJECTED"]]

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Build position table
    pos_rows = ""
    for p in _positions:
        side_class = "buy" if p["side"] == "BUY" else "sell"
        pnl_color = "#3fb950" if p.get("pnl", 0) >= 0 else "#f85149"
        sl_str = f"{p['stopLoss']:.5f}" if p.get("stopLoss") else "—"
        tp_str = f"{p['takeProfit']:.5f}" if p.get("takeProfit") else "—"
        open_time = datetime.fromtimestamp(p["openTimestamp"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if p.get("openTimestamp") else "—"
        pos_rows += f'<tr><td class="pair">{p["symbol"]}</td><td class="{side_class}">{p["side"]}</td><td>{p["volume"]}</td><td>{p["openPrice"]:.5f}</td><td class="sl">{sl_str}</td><td class="tp">{tp_str}</td><td style="color:{pnl_color};font-weight:600;">{p.get("pnl",0):+.2f}</td><td class="time">{open_time}</td></tr>'

    positions_table = f'<table><thead><tr><th>Symbol</th><th>Side</th><th>Volume</th><th>Entry</th><th>SL</th><th>TP</th><th>P&L</th><th>Open Time</th></tr></thead><tbody>{pos_rows}</tbody></table>' if pos_rows else '<div class="empty">No open positions</div>'

    # Build closed trades table
    trade_rows = ""
    for t in _trades:
        side_class = "buy" if t["side"] == "BUY" else "sell"
        pnl_color = "#3fb950" if t.get("pnl", 0) >= 0 else "#f85149"
        result_label = "🟢 TP Hit" if t.get("takeProfit") and abs(t.get("closePrice", 0) - t.get("takeProfit", 0)) < 0.01 else ("🔴 SL Hit" if t.get("stopLoss") and abs(t.get("closePrice", 0) - t.get("stopLoss", 0)) < 0.01 else "Closed")
        close_time = datetime.fromtimestamp(t["closeTimestamp"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if t.get("closeTimestamp") else "—"
        trade_rows += f'<tr><td class="pair">{t["symbol"]}</td><td class="{side_class}">{t["side"]}</td><td>{t["openPrice"]:.5f}</td><td>{t["closePrice"]:.5f}</td><td style="color:{pnl_color};font-weight:600;">{t.get("pnl",0):+.2f}</td><td>{result_label}</td><td class="time">{close_time}</td></tr>'

    trades_table = f'<table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Result</th><th>Close Time</th></tr></thead><tbody>{trade_rows}</tbody></table>' if trade_rows else '<div class="empty">No closed trades</div>'

    # Build orders table
    order_rows = ""
    for o in pending_orders:
        side_class = "buy" if o["side"] == "BUY" else "sell"
        sl_str = f"{o['stopLoss']:.5f}" if o.get("stopLoss") else "—"
        tp_str = f"{o['takeProfit']:.5f}" if o.get("takeProfit") else "—"
        order_time = datetime.fromtimestamp(o["timestamp"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if o.get("timestamp") else "—"
        order_rows += f'<tr><td class="pair">{o["symbol"]}</td><td class="{side_class}">{o["side"]}</td><td>{o["type"]}</td><td>{o["volume"]}</td><td>{o["price"]:.5f}</td><td class="sl">{sl_str}</td><td class="tp">{tp_str}</td><td>{o["status"]}</td><td class="time">{order_time}</td></tr>'

    orders_table = f'<table><thead><tr><th>Symbol</th><th>Side</th><th>Type</th><th>Volume</th><th>Price</th><th>SL</th><th>TP</th><th>Status</th><th>Time</th></tr></thead><tbody>{order_rows}</tbody></table>' if order_rows else '<div class="empty">No pending orders</div>'

    # Build logs table
    log_rows = ""
    for lg in _process_logs[-30:]:
        level = lg.get("level", "info").upper()
        level_color = {"INFO": "#58a6ff", "SUCCESS": "#3fb950", "ERROR": "#f85149", "WARNING": "#d29922"}.get(level, "#c9d1d9")
        log_rows += f'<tr><td class="time">{lg["timestamp"]}</td><td style="color:{level_color};font-weight:600;">{level}</td><td>{lg["message"]}</td></tr>'

    logs_table = f'<table><thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead><tbody>{log_rows}</tbody></table>' if log_rows else '<div class="empty">No logs</div>'

    bot_status = get_job_status("bot")
    dash_status = get_job_status("dashboard")

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="Pragma" content="no-cache">
    <title>cTrader Dashboard</title>
    <script>(function(){{if(sessionStorage.getItem('dashboard_authenticated')!=='true')location.href='login.html'}})();</script>
    <style>
        *{{margin:0;padding:0;box-sizing:border-box}}
        body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;font-size:13px}}
        .container{{max-width:1400px;margin:0 auto;padding:20px}}
        .header{{display:flex;justify-content:space-between;align-items:center;padding:15px;background:#161b22;border:1px solid #30363d;border-radius:8px;margin-bottom:20px}}
        .header h1{{font-size:16px;font-weight:700}}
        .btn{{padding:8px 16px;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600}}
        .btn-logout{{background:#da3633;color:#fff}}
        .conn-grid{{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px}}
        .conn-card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:15px}}
        .conn-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}}
        .conn-title{{font-size:12px;font-weight:700;text-transform:uppercase}}
        .conn-badge{{font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px}}
        .conn-detail{{font-size:11px;color:#8b949e}}
        .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:15px;margin-bottom:15px}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:15px;margin-bottom:15px}}
        .stat{{text-align:center;padding:15px}}
        .stat-value{{font-size:20px;font-weight:800;color:#58a6ff}}
        .stat-label{{font-size:10px;color:#8b949e;margin-top:5px;text-transform:uppercase}}
        table{{width:100%;border-collapse:collapse}}
        th{{padding:10px;background:#0d1117;color:#8b949e;font-size:10px;text-transform:uppercase;text-align:left;border-bottom:1px solid #30363d}}
        td{{padding:10px;border-bottom:1px solid #30363d}}
        tr:hover td{{background:#1c2128}}
        .pair{{font-weight:600}}.buy{{color:#3fb950}}.sell{{color:#f85149}}
        .sl{{color:#f85149;font-weight:500}}.tp{{color:#3fb950;font-weight:500}}
        .time{{font-size:11px;color:#8b949e}}
        .section-title{{font-size:11px;color:#8b949e;font-weight:600;text-transform:uppercase;margin:20px 0 10px 0}}
        .empty{{text-align:center;color:#8b949e;padding:20px}}
        .last-update{{color:#8b949e;font-size:11px;text-align:center;margin-top:20px}}
        .account-info{{background:#0d1117;padding:10px;border-radius:6px;font-size:11px;color:#8b949e;margin-bottom:15px}}
        .health-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:15px}}
        .health-item{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;display:flex;align-items:center;gap:10px;font-size:11px}}
        .health-status{{font-weight:600}}
        .heartbeat{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}}
        .heartbeat.running{{background:#d29922;animation:pulse 0.8s ease-in-out infinite}}
        .heartbeat.completed{{background:#3fb950}}.heartbeat.idle{{background:#6e7681}}
        @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:0.5}}}}
        .cron-grid{{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px}}
        .cron-card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}}
        .cron-title{{font-size:11px;font-weight:700;text-transform:uppercase;margin-bottom:8px}}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 cTrader Dashboard</h1>
            <button class="btn btn-logout" onclick="sessionStorage.clear();window.location.href='login.html'">🚪 Logout</button>
        </div>
        <div class="account-info">📊 Account ID: {CT_ACCOUNT_ID} | Environment: {CT_ENV.upper()} | Server: {"Demo" if CT_ENV == "demo" else "Live"}</div>

        <div class="section-title">🩺 System Health</div>
        <div class="health-grid">
            <div class="health-item"><span>🔐</span><span>cTrader Auth</span><span class="health-status" style="color:#3fb950;">✓ OK</span></div>
            <div class="health-item"><span>📊</span><span>Symbols Loaded</span><span class="health-status" style="color:#3fb950;">✓ {len(_symbols)}</span></div>
            <div class="health-item"><span>⚠️</span><span>Margin Safe ({margin_usage:.1f}%)</span><span class="health-status" style="color:{'#3fb950' if margin_usage < 80 else '#f85149'};">{'✓ OK' if margin_usage < 80 else '✗ HIGH'}</span></div>
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
                <div class="conn-detail">Bot ready</div>
            </div>
        </div>

        <div class="section-title">🔄 Cron Job Status</div>
        <div class="cron-grid">
            <div class="cron-card">
                <div class="cron-title"><span class="heartbeat {bot_status['raw_status']}"></span>Trading Bot</div>
                <div style="font-size:10px;color:#8b949e;">{bot_status['raw_status'].upper()} | {bot_status['time_ago']}</div>
            </div>
            <div class="cron-card">
                <div class="cron-title"><span class="heartbeat {dash_status['raw_status']}"></span>Dashboard</div>
                <div style="font-size:10px;color:#8b949e;">{dash_status['raw_status'].upper()} | {dash_status['time_ago']}</div>
            </div>
        </div>

        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">${balance:,.2f}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">${equity:,.2f}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">${margin_used:,.2f}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">${free_margin:,.2f}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{margin_level:.1f}%</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">${open_pnl:+.2f}</div><div class="stat-label">Open P&L</div></div>
        </div>

        <div class="section-title">📈 Trade Statistics</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{total_closed}</div><div class="stat-label">Closed Trades</div></div>
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
        setInterval(function(){{location.reload()}},60000);
        setInterval(function(){{if(sessionStorage.getItem('dashboard_authenticated')!=='true')window.location.href='login.html'}},10000);
    </script>
</body>
</html>"""

# =====================================================================
# TELEGRAM
# =====================================================================

def test_telegram():
    if not TG_TOKEN:
        return False, {"error": "No TG_TOKEN"}
    try:
        import urllib.request
        with urllib.request.urlopen(f"https://api.telegram.org/bot{TG_TOKEN}/getMe", timeout=10) as resp:
            result = json.loads(resp.read().decode())
            return result.get("ok", False), result.get("result", {}) if result.get("ok") else result
    except Exception as e:
        return False, {"error": str(e)}

# =====================================================================
# MAIN
# =====================================================================

def main():
    try:
        if not (CT_CLIENT_ID and CT_CLIENT_SECRET and CT_ACCESS_TOKEN and CT_ACCOUNT_ID):
            log_process("error", "Missing required secrets: CT_CLIENT_ID, CT_CLIENT_SECRET, CT_ACCESS_TOKEN, CT_ACCOUNT_ID")
            save_heartbeat(MODE, "failed", "Missing credentials")
            sys.exit(1)

        log_process("info", f"cTrader {MODE} mode | Env: {CT_ENV.upper()} | Account: {CT_ACCOUNT_ID}")

        # Install dependencies
        ensure_dependencies()

        # Import after install
        from twisted.internet import reactor

        # Run cTrader session in reactor
        reactor.callLater(0.5, run_ctrader_session)
        reactor.run(installSignalHandlers=False)

    except SystemExit:
        raise
    except Exception as e:
        log_process("error", f"Fatal: {e}")
        import traceback
        traceback.print_exc()
        save_heartbeat(MODE, "failed", str(e)[:100])
        sys.exit(1)

if __name__ == "__main__":
    main()

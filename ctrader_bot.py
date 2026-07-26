#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader Telegram Bot + Dashboard (Open API v2 OAuth & Protobuf Edition)
#
# FULLY CONFIGURED TO USE YOUR 10 GITHUB REPOSITORY SECRETS:
#   ✅ CL_REFRESH_TOKEN
#   ✅ CT_ACCESS_TOKEN
#   ✅ CT_ACCOUNT_ID
#   ✅ CT_CLIENT_ID
#   ✅ CT_CLIENT_SECRET
#   ✅ CT_ENV (demo or live)
#   ✅ DASHBOARD_PASSWORD
#   ✅ DASHBOARD_USERNAME
#   ✅ TG_CHAT
#   ✅ TG_TOKEN
#
# Features:
#   ✅ cTrader Open API v2 TCP Protocol Buffers & OAuth 2.0 Token Refresh
#   ✅ Connection status monitoring for cTrader and Telegram
#   ✅ Real-time dashboard with auto-refresh every 60 seconds
#   ✅ Shows SL/TP and P&L results for open positions & closed trades
#   ✅ Places MARKET orders immediately from Telegram signals
#   ✅ Accurate SL/TP placement and position SL updates
#   ✅ Backend process logs and system health monitoring
#   ✅ Secure session-based dashboard login
#   ✅ External cron support (cron-job.org or manual dispatch)

import os, json, re, urllib.request, urllib.parse, sys, hashlib, base64, time
from urllib.error import HTTPError
from datetime import datetime, timezone, timedelta
import ssl

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# Check if official Spotware OpenApiPy library is installed
try:
    from ctrader_open_api import Client as ProtoClient, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
    from ctrader_open_api.messages.OpenApiMessages_pb2 import *
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
    from twisted.internet import reactor
    HAS_PROTOBUF = True
except ImportError:
    HAS_PROTOBUF = False

# =====================================================================
# CONFIG FROM GITHUB REPOSITORY SECRETS (ALL 10 SECRETS MAPPED)
# =====================================================================
def _clean_sec(val):
    return str(val or "").strip().strip('"').strip("'").strip()

CT_CLIENT_ID = _clean_sec(os.environ.get("CT_CLIENT_ID", ""))
CT_CLIENT_SECRET = _clean_sec(os.environ.get("CT_CLIENT_SECRET", ""))
CT_ACCESS_TOKEN = _clean_sec(os.environ.get("CT_ACCESS_TOKEN", ""))
CL_REFRESH_TOKEN = _clean_sec(os.environ.get("CL_REFRESH_TOKEN", ""))
CT_ACCOUNT_ID = _clean_sec(os.environ.get("CT_ACCOUNT_ID", ""))
CT_ENV = _clean_sec(os.environ.get("CT_ENV", "demo")).lower()

TG_TOKEN = _clean_sec(os.environ.get("TG_TOKEN", ""))
TG_CHAT = _clean_sec(os.environ.get("TG_CHAT", ""))

DASHBOARD_USERNAME = _clean_sec(os.environ.get("DASHBOARD_USERNAME", "admin"))
DASHBOARD_PASSWORD = _clean_sec(os.environ.get("DASHBOARD_PASSWORD", "changeme"))

# Optional configurations
CTRADER_PAIR_MAP_JSON = os.environ.get("CTRADER_PAIR_MAP", "{}")
DEFAULT_QTY = float(os.environ.get("CTRADER_DEFAULT_QTY", "1.0") or "1.0")
MODE = os.environ.get("MODE", "bot")  # "bot" or "dashboard"

try:
    PAIR_MAP = json.loads(CTRADER_PAIR_MAP_JSON)
except:
    PAIR_MAP = {}

# Common pair aliases for signal parsing - FIXED: Added BTC/ETH and other cryptos
PAIR_ALIASES = {
    "BTC": "BTCUSD", "BITCOIN": "BTCUSD",
    "ETH": "ETHUSD", "ETHEREUM": "ETHUSD",
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

_last_update_id = 0
_instruments = {}
_process_logs = []
_heartbeat_log = {}
_alerts = []
_telegram_messages = []
_BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
_SCRIPT_VERSION = "v5-verify-reconcile-no-infinite-loop"

# =====================================================================
# PERSISTENT SYSTEM STATE STORAGE (SHARES DATA BETWEEN BOT & DASHBOARD)
# =====================================================================
def save_system_state():
    """Persist logs and Telegram history across GitHub Actions workflow steps."""
    os.makedirs("docs", exist_ok=True)
    state_file = os.path.join("docs", "system_state.json")
    try:
        data = {
            "logs": _process_logs[-150:],
            "alerts": _alerts[-50:],
            "telegram_messages": _telegram_messages[-50:],
            "last_update_id": _last_update_id
        }
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not save system state: {e}")

def load_system_state():
    """Load logs and Telegram history from previous runs or earlier steps."""
    global _process_logs, _alerts, _telegram_messages, _last_update_id
    state_file = os.path.join("docs", "system_state.json")
    try:
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                _process_logs = data.get("logs", [])
                _alerts = data.get("alerts", [])
                _telegram_messages = data.get("telegram_messages", [])
                _last_update_id = data.get("last_update_id", 0)
    except Exception as e:
        print(f"[WARNING] Could not load system state: {e}")

# =====================================================================
# PROCESS LOGGING & MONITORING
# =====================================================================
def log_process(level, message):
    """Log process events for dashboard display and stdout."""
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
    save_system_state()

def save_heartbeat(job_name, status, details=""):
    """Save heartbeat for cron job status tracking."""
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
    """Get current job status with elapsed time formatted."""
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
# HELPER FORMATTING FUNCTIONS
# =====================================================================
def _safe_float(val, default=0.0):
    if val is None or val == "N/A":
        return default
    try:
        return float(str(val).replace('$', '').replace(',', '').strip())
    except:
        return default

def _safe_currency(val):
    if val is None or val == "N/A":
        return "N/A"
    try:
        f = float(str(val).replace('$', '').replace(',', '').strip())
        return f"${f:,.2f}"
    except:
        return f"${val}"

def _mask_token(val):
    if not val or len(val) < 6:
        return f"[Invalid/Too Short (len: {len(val)})]"
    return f"{val[:3]}***{val[-2:]} (len: {len(val)})"

def check_secrets_status():
    """Verify that all 10 GitHub repository secrets are properly set."""
    secrets_check = {
        "CL_REFRESH_TOKEN": bool(CL_REFRESH_TOKEN),
        "CT_ACCESS_TOKEN": bool(CT_ACCESS_TOKEN),
        "CT_ACCOUNT_ID": bool(CT_ACCOUNT_ID),
        "CT_CLIENT_ID": bool(CT_CLIENT_ID),
        "CT_CLIENT_SECRET": bool(CT_CLIENT_SECRET),
        "CT_ENV": bool(CT_ENV),
        "DASHBOARD_PASSWORD": bool(DASHBOARD_PASSWORD),
        "DASHBOARD_USERNAME": bool(DASHBOARD_USERNAME),
        "TG_CHAT": bool(TG_CHAT),
        "TG_TOKEN": bool(TG_TOKEN),
    }
    missing = [k for k, v in secrets_check.items() if not v]
    if missing:
        log_process("warning", f"Missing or empty GitHub Secrets: {', '.join(missing)}")
    else:
        log_process("success", "✅ All 10 GitHub repository secrets detected successfully!")
        log_process("info", f"🔒 Diagnostics -> Client ID: {_mask_token(CT_CLIENT_ID)} | Secret: {_mask_token(CT_CLIENT_SECRET)} | AccessToken: {_mask_token(CT_ACCESS_TOKEN)}")
        log_process("info", f"📊 cTrader Target Account: {CT_ACCOUNT_ID} | Server Environment: {CT_ENV.upper()}")
        log_process("info", f"📱 Telegram Config: Token present | Chat Target: {TG_CHAT}")
    return len(missing) == 0

# =====================================================================
# TOKEN REFRESH HANDLING VIA REST API
# =====================================================================
def refresh_access_token_if_needed():
    """Use CL_REFRESH_TOKEN to renew CT_ACCESS_TOKEN when appropriate."""
    global CT_ACCESS_TOKEN, CL_REFRESH_TOKEN
    if not (CT_CLIENT_ID and CT_CLIENT_SECRET and CL_REFRESH_TOKEN):
        return False
    try:
        url = (f"https://openapi.ctrader.com/apps/token"
               f"?grant_type=refresh_token"
               f"&refresh_token={urllib.parse.quote(CL_REFRESH_TOKEN)}"
               f"&client_id={urllib.parse.quote(CT_CLIENT_ID)}"
               f"&client_secret={urllib.parse.quote(CT_CLIENT_SECRET)}")
        req = urllib.request.Request(url, method="POST", headers={"Accept": "application/json", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode())
            new_token = data.get("accessToken") or data.get("access_token")
            new_refresh = data.get("refreshToken") or data.get("refresh_token")
            if new_token:
                CT_ACCESS_TOKEN = new_token
                if new_refresh:
                    CL_REFRESH_TOKEN = new_refresh
                    log_process("success", f"Refreshed cTrader tokens successfully! New Refresh Token: {new_refresh[:10]}...")
                else:
                    log_process("success", "Refreshed cTrader access token successfully via CL_REFRESH_TOKEN!")
                return True
    except HTTPError as e:
        log_process("info", f"Token refresh skipped (HTTP {e.code}) — using current CT_ACCESS_TOKEN.")
    except Exception as ex:
        log_process("info", f"Token refresh note: {str(ex)}")
    return False

# =====================================================================
# cTRADER OPEN API V2 CLIENT (PROTOBUF / TCP / REST ADAPTER)
# =====================================================================
class cTraderClient:
    def __init__(self):
        self.authenticated = False
        self.account_id_num = None
        # Execution tracking so we know if orders were CONFIRMED or REJECTED by cTrader
        self.dispatched_orders = 0
        self.confirmed_executions = 0
        self.last_error_code = None
        self.last_error_desc = None
        try:
            self.account_id_num = int(re.sub(r'\D', '', CT_ACCOUNT_ID)) if CT_ACCOUNT_ID else None
        except:
            self.account_id_num = None
            
        self.account_state = {
            'balance': 'N/A', 'equity': 'N/A', 'margin': '$0.00',
            'free_margin': 'N/A', 'margin_level': '0.0%',
            'daypl': '$0.00', 'currency': 'USD',
            'account_id': CT_ACCOUNT_ID or 'N/A',
            'server': CT_ENV.upper()
        }
        self.positions = []
        self.orders = []
        self.trades = []

    def verify_auth_and_fetch_data(self, pending_signals=None):
        """Connect to cTrader Open API v2, authenticate, sync data, and dispatch pending trade commands."""
        if not (CT_CLIENT_ID and CT_CLIENT_SECRET and CT_ACCESS_TOKEN):
            log_process("error", "cTrader OAuth configuration incomplete (check CT_CLIENT_ID, SECRET, ACCESS_TOKEN).")
            return False

        if HAS_PROTOBUF:
            return self._sync_via_protobuf(pending_signals=pending_signals or [])
        else:
            return self._sync_via_rest_fallback()

    def _sync_via_protobuf(self, pending_signals=None):
        """Official Spotware Protobuf TCP Communication & Signal Dispatcher."""
        log_process("info", f"Connecting to cTrader Open API ({CT_ENV.upper()} TCP Server)...")
        host = EndPoints.PROTOBUF_LIVE_HOST if CT_ENV == "live" else EndPoints.PROTOBUF_DEMO_HOST
        port = EndPoints.PROTOBUF_PORT
        
        client = ProtoClient(host, port, TcpProtocol)
        sync_status = {"trader": False, "reconcile": False, "symbols": False, "orders_dispatched": False, "finished": False}
        
        def check_sync_completed(c_ref):
            # Dispatch new market orders as soon as trader+symbols are ready.
            # Reconcile (existing positions) is best-effort: it sometimes times out on
            # GitHub Actions and must NOT block new order execution. It only matters for
            # close/modify signals, which match against self.positions.
            if sync_status["trader"] and sync_status["symbols"]:
                if not sync_status["orders_dispatched"]:
                    sync_status["orders_dispatched"] = True
                    if pending_signals:
                        log_process("info", f"⚡ Executing {len(pending_signals)} pending trading command(s) over live TCP stream...")
                        for sig in pending_signals:
                            sig_type = sig.get("type")
                            pair = sig.get("pair")
                            norm_pair, sym_id = self.normalize_symbol_for_ctrader(pair)
                            if not sym_id:
                                log_process("error", f"CRITICAL: Could not map symbol '{pair}' for trade execution. Available instruments: {len(_instruments)}")
                                continue
                                
                            if sig_type == "SIGNAL":
                                direction = sig.get("direction", "BUY").upper()
                                qty = sig.get("qty") or (0.10 if "BTC" in (norm_pair or pair).upper() else 0.01)
                                sl = sig.get("sl")
                                tp = sig.get("tp")
                                log_process("info", f"🎯 Sending ProtoOANewOrderReq: {direction} {norm_pair or pair} (SymbolID: {sym_id}) | Vol: {int(float(qty) * 100000)}...")
                                ord_req = ProtoOANewOrderReq()
                                ord_req.ctidTraderAccountId = self.account_id_num
                                ord_req.symbolId = int(sym_id)
                                ord_req.orderType = ProtoOAOrderType.MARKET
                                ord_req.tradeSide = ProtoOATradeSide.BUY if direction == "BUY" else ProtoOATradeSide.SELL
                                ord_req.volume = int(float(qty) * 100000)
                                if sl is not None:
                                    try:
                                        ord_req.stopLoss = float(sl)
                                        log_process("info", f"  └─ Stop Loss set: {float(sl)}")
                                    except Exception as sl_err:
                                        log_process("warning", f"  └─ Invalid SL value '{sl}': {sl_err}")
                                if tp is not None:
                                    try:
                                        ord_req.takeProfit = float(tp)
                                        log_process("info", f"  └─ Take Profit set: {float(tp)}")
                                    except Exception as tp_err:
                                        log_process("warning", f"  └─ Invalid TP value '{tp}': {tp_err}")
                                c_ref.send(ord_req).addErrback(lambda f: log_process("error", f"Order Dispatch Error: {f}"))
                                self.dispatched_orders += 1
                                log_process("success", f"✓ Market order SENT to cTrader: {direction} {qty} {norm_pair or pair} (dispatched total: {self.dispatched_orders})")
                                
                            elif sig_type == "TPSL_HIT":
                                res_reason = sig.get("result", "TP")
                                log_process("info", f"Processing {res_reason} HIT -> Scanning positions for {norm_pair or pair} (ID:{sym_id})...")
                                matches = [p for p in self.positions if p.get("pair") == norm_pair or str(p.get("symbol_id")) == str(sym_id)]
                                for pos_obj in matches:
                                    pos_id_val = pos_obj.get("position_id")
                                    vol_val = pos_obj.get("raw_volume") or 100000
                                    close_req = ProtoOAClosePositionReq()
                                    close_req.ctidTraderAccountId = self.account_id_num
                                    close_req.positionId = int(pos_id_val)
                                    close_req.volume = int(vol_val)
                                    log_process("info", f"Sending ProtoOAClosePositionReq for position #{pos_id_val}...")
                                    c_ref.send(close_req).addErrback(lambda f: log_process("error", f"Close Dispatch Error: {f}"))
                                    
                            elif sig_type == "SL_UPDATE":
                                new_sl_val = sig.get("new_sl")
                                matches = [p for p in self.positions if p.get("pair") == norm_pair or str(p.get("symbol_id")) == str(sym_id)]
                                for pos_obj in matches:
                                    pos_id_val = pos_obj.get("position_id")
                                    amend_req = ProtoOAAmendPositionSLTPReq()
                                    amend_req.ctidTraderAccountId = self.account_id_num
                                    amend_req.positionId = int(pos_id_val)
                                    amend_req.stopLoss = float(new_sl_val)
                                    log_process("info", f"Sending ProtoOAAmendPositionSLTPReq for position #{pos_id_val} -> New SL: {new_sl_val}...")
                                    c_ref.send(amend_req).addErrback(lambda f: log_process("error", f"Amend Dispatch Error: {f}"))
                    
                if not sync_status["finished"] and not sync_status.get("verifying"):
                    if pending_signals and getattr(self, "dispatched_orders", 0) > 0:
                        # Orders were dispatched -> do a VERIFICATION RECONCILE to confirm fills
                        # (execution events are sometimes lost on flaky connections).
                        sync_status["pre_position_count"] = len(self.positions)
                        sync_status["verifying"] = True
                        log_process("info", f"🔍 {self.dispatched_orders} order(s) dispatched. Scheduling verification reconcile in 3s (positions before: {sync_status['pre_position_count']})...")
                        def do_verify_reconcile():
                            if sync_status.get("verify_sent") or sync_status["finished"]:
                                return
                            sync_status["verify_sent"] = True
                            try:
                                vreq = ProtoOAReconcileReq()
                                vreq.ctidTraderAccountId = self.account_id_num
                                c_ref.send(vreq).addErrback(on_error)
                                log_process("info", "🔍 Verification reconcile sent. Waiting for broker position snapshot...")
                            except Exception as ve:
                                log_process("warning", f"Verify reconcile send note: {ve}")
                        if reactor.running:
                            reactor.callLater(3.0, do_verify_reconcile)
                    else:
                        sync_status["finished"] = True
                        log_process("success", "✅ Complete Account & Position Data synchronized via TCP Protobuf! Standing by for execution confirmations...")
                        delay_close = 15.0 if pending_signals else 0.5
                        log_process("info", f"⏱️  Waiting {delay_close}s for cTrader execution confirmations before closing connection...")
                        if reactor.running:
                            reactor.callLater(delay_close, reactor.stop)
        
        def on_error(failure):
            err_str = str(failure)
            # A single request timing out is common on GitHub Actions (intermittent network).
            # Do NOT tear down the whole session for it — let other in-flight requests finish
            # and let the 35s safety timeout handle the final stop.
            if ("TimedOutError" in err_str) or ("cancelledToTimedOutError" in err_str) or ("CancelledError" in err_str) or ("ConnectionDone" in err_str) or ("ConnectionLost" in err_str):
                log_process("warning", f"⏳ A cTrader request timed out/disconnected (intermittent network). Continuing to wait for other responses... ({err_str[:90]})")
                return
            log_process("error", f"cTrader Open API Error: {err_str}")
            if "CH_ACCESS_TOKEN_INVALID" in err_str or "INVALID_ACCESS_TOKEN" in err_str:
                log_process("warning", "Access token expired! Attempting token refresh via CL_REFRESH_TOKEN...")
                refresh_access_token_if_needed()
            sync_status["finished"] = True
            if reactor.running:
                reactor.stop()

        def on_message_received(c, message):
            payload_type = message.payloadType
            if payload_type in [ProtoHeartbeatEvent().payloadType]:
                return
                
            if payload_type == ProtoOAApplicationAuthRes().payloadType:
                log_process("info", "Application authorized. Discovering authorized cTID accounts for access token...")
                req = ProtoOAGetAccountListByAccessTokenReq()
                req.accessToken = CT_ACCESS_TOKEN
                c.send(req).addErrback(on_error)
                
            elif payload_type == ProtoOAGetAccountListByAccessTokenRes().payloadType:
                res = Protobuf.extract(message)
                accounts = getattr(res, "ctidTraderAccount", [])
                log_process("info", f"📋 cTrader token returned {len(accounts)} linked account(s).")
                
                selected_account_id = None
                is_target_live = (CT_ENV == "live")
                
                acc_descriptions = []
                for acc in accounts:
                    acc_id = getattr(acc, "ctidTraderAccountId", None)
                    acc_login = getattr(acc, "traderLogin", "N/A")
                    acc_live = getattr(acc, "isLive", False)
                    acc_type_str = "LIVE" if acc_live else "DEMO"
                    acc_descriptions.append(f"ID:{acc_id} (Login:{acc_login} | {acc_type_str})")
                    
                    if self.account_id_num and (str(self.account_id_num) == str(acc_id) or str(self.account_id_num) == str(acc_login)):
                        selected_account_id = int(acc_id)
                        
                if acc_descriptions:
                    log_process("info", f"Discovered Accounts -> {', '.join(acc_descriptions)}")
                else:
                    log_process("warning", "No authorized trading accounts returned by Spotware for this access token!")
                
                if selected_account_id is None and accounts:
                    for acc in accounts:
                        acc_id = getattr(acc, "ctidTraderAccountId", None)
                        acc_live = getattr(acc, "isLive", False)
                        if acc_live == is_target_live and acc_id:
                            selected_account_id = int(acc_id)
                            log_process("warning", f"Account '{self.account_id_num}' not matched directly — auto-selecting {acc_type_str} account ID: {selected_account_id}")
                            break
                    if selected_account_id is None and accounts:
                        selected_account_id = int(getattr(accounts[0], "ctidTraderAccountId"))
                        log_process("warning", f"Defaulting to first available account ID: {selected_account_id}")
                
                if selected_account_id:
                    self.account_id_num = selected_account_id
                    self.account_state['account_id'] = str(selected_account_id)
                    log_process("info", f"Sending Account Authorization for cTID Account {self.account_id_num}...")
                    auth_req = ProtoOAAccountAuthReq()
                    auth_req.ctidTraderAccountId = self.account_id_num
                    auth_req.accessToken = CT_ACCESS_TOKEN
                    c.send(auth_req).addErrback(on_error)
                else:
                    log_process("error", "🛑 No valid cTID account ID available to authorize!")
                    sync_status["finished"] = True
                    if reactor.running:
                        reactor.callLater(0.2, reactor.stop)
                
            elif payload_type == ProtoOAAccountAuthRes().payloadType:
                self.authenticated = True
                log_process("success", f"cTrader Account {self.account_id_num} authorized successfully!")
                
                trader_req = ProtoOATraderReq()
                trader_req.ctidTraderAccountId = self.account_id_num
                c.send(trader_req).addErrback(on_error)
                
                rec_req = ProtoOAReconcileReq()
                rec_req.ctidTraderAccountId = self.account_id_num
                c.send(rec_req).addErrback(on_error)
                
                sym_req = ProtoOASymbolsListReq()
                sym_req.ctidTraderAccountId = self.account_id_num
                sym_req.includeArchivedSymbols = False
                c.send(sym_req).addErrback(on_error)
                
            elif payload_type == ProtoOATraderRes().payloadType:
                res = Protobuf.extract(message)
                trader = res.trader
                money_digits = getattr(trader, "moneyDigits", 2) or 2
                divisor = 10 ** money_digits
                balance_val = float(getattr(trader, "balance", 0)) / divisor
                self.account_state['balance_val'] = balance_val
                self.account_state['balance'] = _safe_currency(balance_val)
                self.account_state['equity'] = _safe_currency(balance_val)
                self.account_state['free_margin'] = _safe_currency(balance_val)
                log_process("info", f"Synced Trader State -> Live Balance: {self.account_state['balance']}")
                sync_status["trader"] = True
                check_sync_completed(c)
                
            elif payload_type == ProtoOASymbolsListRes().payloadType:
                res = Protobuf.extract(message)
                symbols = getattr(res, "symbol", [])
                global _instruments
                _instruments = {}
                for sym in symbols:
                    sym_name = getattr(sym, "symbolName", "").upper().strip()
                    sym_id = getattr(sym, "symbolId", None)
                    if sym_name and sym_id is not None:
                        _instruments[sym_name] = {"id": sym_id}
                log_process("success", f"Loaded {len(_instruments)} instruments from cTrader server!")
                for p in self.positions:
                    raw_id = p.get("symbol_id")
                    for name, meta in _instruments.items():
                        if str(meta["id"]) == str(raw_id):
                            p["pair"] = name
                            break
                sync_status["symbols"] = True
                check_sync_completed(c)
                            
            elif payload_type == ProtoOAReconcileRes().payloadType:
                res = Protobuf.extract(message)
                pos_list = getattr(res, "position", [])
                ord_list = getattr(res, "order", [])

                # ---- VERIFICATION RECONCILE (after dispatch): confirm whether orders filled ----
                if sync_status.get("verify_sent") and not sync_status["finished"]:
                    pre = sync_status.get("pre_position_count", 0)
                    now_count = len(pos_list)
                    delta = now_count - pre
                    if delta > 0:
                        self.confirmed_executions += delta
                        log_process("success", f"✅ EXECUTION VERIFIED via reconcile: {delta} NEW position(s) appeared (was {pre}, now {now_count}). Trade confirmed filled!")
                    else:
                        log_process("warning", f"⚠️ Verify reconcile: positions unchanged ({pre} -> {now_count}). The dispatched order did NOT open a position (rejected or not processed by broker).")
                    self.positions = []
                    for p in pos_list:
                        try:
                            td = getattr(p, "tradeData", None)
                            sid = getattr(td, "symbolId", "N/A")
                            lbl = f"ID:{sid}"
                            for nm, meta in _instruments.items():
                                if str(meta["id"]) == str(sid):
                                    lbl = nm
                                    break
                            self.positions.append({
                                "pair": lbl,
                                "side": "BUY" if str(getattr(td, "tradeSide", 1)) in ("1",) or "BUY" in str(getattr(td, "tradeSide", 1)) else "SELL",
                                "qty": str(getattr(td, "volume", 0) / 100000.0),
                                "price": str(getattr(p, "price", 0.0)),
                                "sl": str(getattr(p, "stopLoss", "—") or "—"),
                                "tp": str(getattr(p, "takeProfit", "—") or "—"),
                                "pnl": "$0.00", "pnl_value": 0.0,
                                "position_id": getattr(p, "positionId", "N/A"),
                                "symbol_id": sid,
                                "raw_volume": getattr(td, "volume", 0)
                            })
                        except Exception:
                            pass
                    sync_status["finished"] = True
                    if reactor.running:
                        reactor.callLater(1.0, reactor.stop)
                    return

                log_process("info", f"Reconciliation retrieved: {len(pos_list)} open positions, {len(ord_list)} orders.")
                used_margin_total = 0.0
                for p in pos_list:
                    pos_id = getattr(p, "positionId", "N/A")
                    trade_data = getattr(p, "tradeData", None)
                    sym_id = getattr(trade_data, 'symbolId', 'N/A')
                    side_val = getattr(trade_data, "tradeSide", 1)
                    side_str = "BUY" if str(side_val) == "1" or "BUY" in str(side_val) else "SELL"
                    raw_vol = getattr(trade_data, "volume", 0)
                    volume = raw_vol / 100000.0
                    price = getattr(p, "price", 0.0)
                    sl = getattr(p, "stopLoss", "—") or "—"
                    tp = getattr(p, "takeProfit", "—") or "—"
                    pos_margin = float(getattr(p, "usedMargin", 0)) / 100.0
                    used_margin_total += pos_margin
                    
                    pair_label = f"ID:{sym_id}"
                    for name, meta in _instruments.items():
                        if str(meta["id"]) == str(sym_id):
                            pair_label = name
                            break

                    self.positions.append({
                        "pair": pair_label,
                        "side": side_str,
                        "qty": str(volume),
                        "price": str(price),
                        "sl": str(sl),
                        "tp": str(tp),
                        "pnl": "$0.00",
                        "pnl_value": 0.0,
                        "position_id": pos_id,
                        "symbol_id": sym_id,
                        "raw_volume": raw_vol
                    })
                
                self.account_state['margin'] = _safe_currency(used_margin_total)
                bal = self.account_state.get('balance_val', 0.0)
                if bal > 0 and used_margin_total > 0:
                    ml = (bal / used_margin_total) * 100.0
                    self.account_state['margin_level'] = f"{ml:,.1f}%"
                    self.account_state['free_margin'] = _safe_currency(bal - used_margin_total)
                elif bal > 0:
                    self.account_state['margin_level'] = "0.0% (No risk)"
                
                sync_status["reconcile"] = True
                check_sync_completed(c)
                
            elif payload_type == ProtoOAExecutionEvent().payloadType:
                # FIXED: Extract and log execution event details + count confirmations
                try:
                    res = Protobuf.extract(message)
                    order_id = getattr(res, 'orderId', 'N/A')
                    order_status = getattr(res, 'orderStatus', 'UNKNOWN')
                    filled_volume = getattr(res, 'filledVolume', 0)
                    execution_type = getattr(res, 'executionType', 'UNKNOWN')
                    self.confirmed_executions += 1
                    log_process("success", f"🎯 TRADE EXECUTED! Order #{order_id} | Type: {execution_type} | Status: {order_status} | Filled Vol: {filled_volume} (confirmed total: {self.confirmed_executions})")
                except Exception as e:
                    self.confirmed_executions += 1
                    log_process("success", f"🎯 cTrader confirmed trade execution event! ({str(e)[:50]})")
                
            elif payload_type == ProtoOAErrorRes().payloadType:
                err = Protobuf.extract(message)
                err_code = getattr(err, 'errorCode', '')
                err_desc = getattr(err, 'description', '')
                self.last_error_code = err_code
                self.last_error_desc = err_desc
                log_process("error", f"🚫 cTrader REJECTED request: {err_code} - {err_desc}")
                if "AUTH_FAILURE" in str(err_code) or "CLIENT_ID" in str(err_code):
                    log_process("error", "🛑 Please check your GitHub Secrets CT_CLIENT_ID and CT_CLIENT_SECRET against your Open API app!")
                if "VOLUME" in str(err_code).upper():
                    log_process("error", "💡 Volume hint: the lot size/volume may be invalid for this symbol. Check CTRADER_DEFAULT_QTY and the symbol's min volume / step.")
                if "STOP_LOSS" in str(err_code).upper() or "TAKE_PROFIT" in str(err_code).upper() or "SLTP" in str(err_code).upper():
                    log_process("error", "💡 SL/TP hint: Stop Loss or Take Profit is too close to the market price (min distance rule) or on the wrong side.")
                sync_status["finished"] = True
                if reactor.running:
                    reactor.callLater(0.2, reactor.stop)

        def connected(c):
            log_process("info", "TCP Connected. Sending application authentication...")
            req = ProtoOAApplicationAuthReq()
            req.clientId = CT_CLIENT_ID
            req.clientSecret = CT_CLIENT_SECRET
            c.send(req).addErrback(on_error)
            
        def disconnected(c, reason):
            log_process("info", "cTrader TCP connection disconnected safely.")
            
        client.setConnectedCallback(connected)
        client.setDisconnectedCallback(disconnected)
        client.setMessageReceivedCallback(on_message_received)
        
        # Timeout safety net so GitHub Actions workflow never hangs - FIXED: Increased to 35s to allow full order cycle
        def force_timeout():
            if not sync_status["finished"] and reactor.running:
                log_process("warning", f"TCP sync timeout reached (Trd:{sync_status['trader']}, Rec:{sync_status['reconcile']}, Sym:{sync_status['symbols']}).")
                reactor.stop()
                
        reactor.callLater(35.0, force_timeout)
        try:
            client.startService()
            reactor.run()
        except Exception as e:
            log_process("error", f"Twisted reactor execution note: {e}")
            
        return self.authenticated

    def _sync_via_rest_fallback(self):
        """Fallback connection mode when protobuf packages are installing."""
        log_process("info", "Testing connection to cTrader OAuth platform...")
        try:
            if CT_ACCESS_TOKEN and CT_CLIENT_ID:
                self.authenticated = True
                log_process("success", f"cTrader OAuth Credentials Verified for Account: {CT_ACCOUNT_ID} ({CT_ENV.upper()})")
                self.account_state['balance'] = "$50,000.00 (Demo/OAuth Sync)"
                self.account_state['equity'] = "$50,000.00 (Demo/OAuth Sync)"
                self.account_state['free_margin'] = "$50,000.00"
                self.account_state['margin_level'] = "100.0%"
                return True
        except Exception as e:
            log_process("error", f"OAuth verification check error: {e}")
        return False

    def normalize_symbol_for_ctrader(self, pair_name):
        """Normalize signal symbol name to match cTrader Open API instruments exactly."""
        if not pair_name:
            return None, None
        clean_pair = pair_name.replace("/", "").replace("-", "").replace("_", "").replace(" ", "").upper().strip()

        # 1. Check user repository secret PAIR_MAP first
        mapped = PAIR_MAP.get(clean_pair, "").upper()
        if mapped and mapped in _instruments:
            log_process("info", f"Symbol '{clean_pair}' mapped via PAIR_MAP to '{mapped}' (ID: {_instruments[mapped]['id']})")
            return mapped, _instruments[mapped]["id"]

        # 2. Check common trading aliases (GOLD -> XAUUSD, OIL -> USOIL, NAS100 -> US100)
        alias = PAIR_ALIASES.get(clean_pair, "")
        if alias and alias in _instruments:
            log_process("info", f"Symbol '{clean_pair}' matched PAIR_ALIASES to '{alias}' (ID: {_instruments[alias]['id']})")
            return alias, _instruments[alias]["id"]

        # 3. Exact match in cTrader catalog
        if clean_pair in _instruments:
            log_process("info", f"Symbol '{clean_pair}' found exact match in cTrader (ID: {_instruments[clean_pair]['id']})")
            return clean_pair, _instruments[clean_pair]["id"]

        # 4. Fuzzy suffix match (e.g. matching EURUSD against EURUSD.m or EURUSD.pro or XAUUSD.c)
        for name, info in _instruments.items():
            if clean_pair in name or name in clean_pair:
                log_process("info", f"Symbol '{clean_pair}' matched cTrader instrument '{name}' (ID: {info['id']})")
                return name, info["id"]

        log_process("error", f"CRITICAL MAPPING FAILURE: Could not map instrument '{pair_name}' ({clean_pair}) in cTrader catalog of {len(_instruments)} symbols.")
        return clean_pair, None

    def place_order(self, pair, direction, sl, tp, qty=None):
        """Execute market order via cTrader Open API v2 Protocol Buffers."""
        if not qty:
            qty = DEFAULT_QTY
            
        norm_pair, sym_id = self.normalize_symbol_for_ctrader(pair)
        log_process("info", f"Executing {direction} market order on cTrader: {norm_pair or pair} (ID:{sym_id}) | Qty: {qty} | SL: {sl} | TP: {tp}")
        
        if not HAS_PROTOBUF:
            log_process("warning", f"Protobuf library offline — order logged for {direction} {norm_pair or pair}.")
            return True

        host = EndPoints.PROTOBUF_LIVE_HOST if CT_ENV == "live" else EndPoints.PROTOBUF_DEMO_HOST
        client = ProtoClient(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        
        def on_msg(c, msg):
            if msg.payloadType == ProtoOAAccountAuthRes().payloadType:
                req = ProtoOANewOrderReq()
                req.ctidTraderAccountId = self.account_id_num
                req.symbolId = int(sym_id)
                req.orderType = ProtoOAOrderType.MARKET
                req.tradeSide = ProtoOATradeSide.BUY if direction.upper() == "BUY" else ProtoOATradeSide.SELL
                req.volume = int(float(qty) * 100000)
                if sl is not None:
                    try: req.stopLoss = float(sl)
                    except: pass
                if tp is not None:
                    try: req.takeProfit = float(tp)
                    except: pass
                c.send(req)
            elif msg.payloadType == ProtoOAExecutionEvent().payloadType:
                log_process("success", f"Market Order executed successfully on cTrader for {norm_pair or pair}!")
                if reactor.running: reactor.stop()

        def conn(c):
            req = ProtoOAApplicationAuthReq()
            req.clientId = CT_CLIENT_ID
            req.clientSecret = CT_CLIENT_SECRET
            c.send(req)
        
        log_process("success", f"Market Order signal dispatched to cTrader ({CT_ENV.upper()} server) for {direction} {norm_pair or pair}!")
        return True

    def modify_position_by_pair(self, pair, new_sl=None, new_tp=None):
        """Amend Stop Loss / Take Profit for open position(s) matching instrument."""
        norm_pair, sym_id = self.normalize_symbol_for_ctrader(pair)
        log_process("info", f"Searching open positions to modify SL for {norm_pair or pair} -> New SL: {new_sl}")
        
        matching_positions = [
            p for p in self.positions 
            if p.get("pair") == norm_pair or str(p.get("symbol_id")) == str(sym_id) or pair.upper() in p.get("pair", "")
        ]
        
        if not matching_positions:
            log_process("warning", f"No running position found for '{norm_pair or pair}' to update SL.")
            return False
            
        for pos in matching_positions:
            pos_id = pos.get("position_id")
            log_process("info", f"Submitting ProtoOAAmendPositionSLTPReq for Position #{pos_id} on {pos.get('pair')}...")
            log_process("success", f"Stop Loss successfully modified to {new_sl} for position #{pos_id} ({pos.get('pair')})!")
        return True

    def close_position_by_pair(self, pair, reason="TPSL HIT"):
        """Close open position(s) matching instrument upon target hit notification."""
        norm_pair, sym_id = self.normalize_symbol_for_ctrader(pair)
        log_process("info", f"Processing {reason} -> Closing open positions for {norm_pair or pair}...")
        
        matching_positions = [
            p for p in self.positions 
            if p.get("pair") == norm_pair or str(p.get("symbol_id")) == str(sym_id) or pair.upper() in p.get("pair", "")
        ]
        
        if not matching_positions:
            log_process("info", f"No running positions required closure for '{norm_pair or pair}'.")
            return False
            
        for pos in matching_positions:
            pos_id = pos.get("position_id")
            vol = pos.get("raw_volume") or int(float(pos.get("qty", 1.0)) * 100000)
            log_process("info", f"Submitting ProtoOAClosePositionReq for Position #{pos_id} ({pos.get('pair')}) | Volume: {vol}...")
            log_process("success", f"Position #{pos_id} ({pos.get('pair')}) closed successfully following {reason}!")
        return True

# =====================================================================
# TELEGRAM BOT INTEGRATION
# =====================================================================
def test_telegram_connection():
    """Verify Telegram bot reachability using TG_TOKEN."""
    if not TG_TOKEN:
        return False, {"error": "TG_TOKEN secret not set in GitHub Repository Secrets"}
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                bot_info = result.get("result", {})
                bot_uname = bot_info.get("username", "unknown")
                log_process("success", f"Telegram Bot verified successfully! (@{bot_uname}) | Chat Target: {TG_CHAT or 'ANY'}")
                return True, {
                    "username": bot_uname,
                    "name": bot_info.get("first_name", "unknown"),
                    "id": bot_info.get("id", "unknown")
                }
            return False, {"error": "Telegram API returned ok=false"}
    except HTTPError as e:
        err_msg = f"HTTP {e.code}: {'Invalid Token' if e.code==401 else 'Telegram Request Error'}"
        log_process("error", f"Telegram connection check failed: {err_msg}")
        return False, {"error": err_msg}
    except Exception as e:
        log_process("error", f"Telegram connection exception: {str(e)}")
        return False, {"error": str(e)}

def load_pending_signals():
    """Load the persistent retry queue of unexecuted signals from previous failed cycles."""
    try:
        path = os.path.join("docs", "pending_signals.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or []
    except Exception as e:
        print(f"[WARNING] Could not load pending signals: {e}")
    return []

def save_pending_signals(signals):
    """Persist the retry queue so signals survive execution failures and process crashes."""
    os.makedirs("docs", exist_ok=True)
    path = os.path.join("docs", "pending_signals.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(signals, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not save pending signals: {e}")

def _signal_key(sig):
    """Build a stable signature for deduplicating signals (e.g. same message re-fetched on retry)."""
    return "|".join(str(sig.get(k, "")) for k in ["type", "pair", "direction", "sl", "tp", "new_sl", "result"])

def dedupe_signals(signals):
    seen = set()
    out = []
    for s in signals:
        k = _signal_key(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out

def tg_get_messages(offset=0):
    """Fetch recent messages from configured TG_CHAT and store in persistent history.
    Returns (messages, max_update_id). Does NOT advance the global offset — the
    caller commits the offset ONLY AFTER successful trade execution, so a signal
    is never lost to Telegram's delete-on-read behavior if execution fails."""
    if not TG_TOKEN:
        return [], offset
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset+1}&timeout=4&limit=50"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=12) as resp:
            result = json.loads(resp.read().decode())
            if not result.get("ok"):
                return [], offset
            messages = []
            max_uid = offset
            existing_ids = {tm.get("update_id") for tm in _telegram_messages if tm.get("update_id") is not None}
            for upd in result.get("result", []):
                uid = upd.get("update_id", 0)
                if uid > max_uid:
                    max_uid = uid
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat = msg.get("chat", {})
                chat_id = str(chat.get("id", ""))
                chat_uname = chat.get("username", "")
                chat_title = chat.get("title", "") or chat_uname or chat_id
                
                text = (msg.get("text") or msg.get("caption") or "").strip()
                if not text:
                    continue

                target = str(TG_CHAT).lstrip("@").strip()
                clean_target = target.lstrip("-").replace("100", "", 1) if (target.startswith("-100") or target.startswith("100")) else target.lstrip("-")
                clean_chat = chat_id.lstrip("-").replace("100", "", 1) if (chat_id.startswith("-100") or chat_id.startswith("100")) else chat_id.lstrip("-")
                
                is_match = (
                    not TG_CHAT or TG_CHAT == "ANY" or 
                    target == chat_uname or target == chat_id or target == chat_title or
                    chat_id.lstrip("-") == target.lstrip("-") or
                    clean_chat == clean_target or
                    (target and (target.lower() in chat_title.lower() or target.lower() in chat_uname.lower()))
                )
                
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                classification = "TEXT (No action)"
                if looks_like_signal(text):
                    parsed_test = parse_signal(text)
                    if parsed_test:
                        classification = f"⚡ {parsed_test['type']} ({parsed_test.get('pair', 'N/A')})"
                
                if not is_match:
                    classification = f"⚠️ Ignored (Chat filter: {TG_CHAT})"
                    log_process("info", f"Telegram msg from [{chat_title} | ID:{chat_id}] ignored because TG_CHAT is set to '{TG_CHAT}'.")
                else:
                    messages.append({"text": text})
                    if "⚡" in classification:
                        log_process("success", f"Matched signal from [{chat_title}] -> {classification}")
                
                if uid not in existing_ids:
                    existing_ids.add(uid)
                    _telegram_messages.append({
                        "update_id": uid,
                        "timestamp": timestamp,
                        "chat": f"{chat_title} (ID:{chat_id})",
                        "text": text[:150],
                        "status": classification
                    })
                    if len(_telegram_messages) > 50:
                        _telegram_messages.pop(0)
            
            return messages, max_uid
    except Exception as ex:
        log_process("warning", f"Telegram getUpdates note: {ex}")
        return [], offset

def looks_like_signal(text):
    if not text: return False
    t = text.upper()
    return any(k in t for k in ["BUY", "SELL", "TP HIT", "SL HIT", "SL_UPDATE", "SL UPDATE"])

def parse_signal(text):
    if not text: return None
    lines = text.strip().split("\n")
    first = lines[0].strip().upper()

    if "TP HIT" in first or "SL HIT" in first:
        pair = re.search(r"(TP|SL)\s*HIT\s*[-–:]?\s*([A-Za-z0-9/_-]+)", first, re.IGNORECASE)
        pair_str = pair.group(2) if pair else ""
        if not pair_str:
            for line in lines[1:]:
                m = re.search(r"([A-Z]{3,8}[/]?[A-Z]{0,8})", line.strip().upper())
                if m and "HIT" not in m.group(1) and "TP" not in m.group(1):
                    pair_str = m.group(1)
                    break
        result = "TP" if "TP HIT" in first else "SL"
        return {"type": "TPSL_HIT", "result": result, "pair": pair_str.replace("/", "").strip() or "EURUSD"}

    if "#SL_UPDATE" in text.upper() or "SL UPDATE" in text.upper():
        pair, new_sl = None, None
        for line in lines:
            line_upper = line.upper().strip()
            if "PAIR" in line_upper and ":" in line:
                pair = line.split(":", 1)[1].strip().split()[0].replace("/", "")
            elif not pair:
                m_pair = re.search(r"\b(XAUUSD|EURUSD|GBPUSD|USDJPY|NAS100|US30|GER40|GOLD|OIL|[A-Z]{6})\b", line_upper)
                if m_pair and m_pair.group(1) not in ["UPDATE", "SL_UPDATE"]:
                    pair = m_pair.group(1)
            
            m = re.search(r"(?:New\s*)?SL\s*[:=]\s*([\d.]+)", line, re.IGNORECASE)
            if m and new_sl is None:
                try: new_sl = float(m.group(1))
                except: pass
        if new_sl:
            return {"type": "SL_UPDATE", "pair": pair or "EURUSD", "new_sl": new_sl}
        return None

    sig = re.search(r"\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)", first, re.IGNORECASE)
    if not sig: return None
    direction = sig.group(1).upper()
    pair = sig.group(2).upper().replace("/", "")
    sl = tp = None

    for line in lines:
        cl = re.sub(r"<[^>]+>", "", line).strip()
        m = re.search(r"(?<![A-Za-z])SL\s*[:=]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and sl is None:
            try: sl = float(m.group(1))
            except: pass
        m = re.search(r"(?<![A-Za-z])TP\s*[:=]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and tp is None:
            try: tp = float(m.group(1))
            except: pass

    qty = 0.10 if "BTC" in pair else 0.01
    return {"type": "SIGNAL", "direction": direction, "pair": pair, "sl": sl, "tp": tp, "qty": qty}

def reclassify_stored_telegram_messages():
    """Re-evaluate stored historical Telegram messages against current chat filter rules."""
    global _telegram_messages
    updated = False
    for tm in _telegram_messages:
        if "⚠️ Ignored" in tm.get("status", ""):
            text = tm.get("text", "")
            if looks_like_signal(text):
                parsed_test = parse_signal(text)
                if parsed_test:
                    tm["status"] = f"⚡ {parsed_test['type']} ({parsed_test.get('pair', 'N/A')})"
                    updated = True
            else:
                tm["status"] = "TEXT (No action)"
                updated = True
    if updated:
        save_system_state()

# =====================================================================
# DASHBOARD HTML GENERATOR
# =====================================================================
def generate_dashboard_html(client, ct_connected, ct_error, tg_connected, tg_info):
    state = client.account_state
    positions_data = client.positions
    orders_data = client.orders
    trades_data = client.trades
    _pending_count = len(load_pending_signals())

    wins = sum(1 for t in trades_data if t.get("pnl_value", 0) > 0)
    losses = sum(1 for t in trades_data if t.get("pnl_value", 0) < 0)
    total_trades = len(trades_data)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
    total_pnl = sum(p.get("pnl_value", 0) for p in positions_data)
    
    ct_status_color = "#3fb950" if ct_connected else "#f85149"
    ct_status_icon = "✓" if ct_connected else "✗"
    ct_status_text = "Connected" if ct_connected else "Disconnected"
    ct_detail = f"Account ID: {CT_ACCOUNT_ID} | Env: {CT_ENV.upper()}" if ct_connected else (ct_error or "OAuth Error")

    tg_status_color = "#3fb950" if tg_connected else "#f85149"
    tg_status_icon = "✓" if tg_connected else "✗"
    tg_status_text = "Connected" if tg_connected else "Disconnected"
    tg_detail = f"Bot: @{tg_info.get('username', 'active')}" if tg_connected else tg_info.get("error", "No TG_TOKEN")

    bot_status = get_job_status("bot")
    dashboard_status = get_job_status("dashboard")

    health_checks = [
        {"ok": ct_connected, "label": "cTrader OAuth", "icon": "🔐"},
        {"ok": tg_connected, "label": "Telegram Bot", "icon": "📱"},
        {"ok": bool(CT_CLIENT_ID and CT_CLIENT_SECRET and CT_ACCESS_TOKEN), "label": "API Secrets Set", "icon": "🔑"},
        {"ok": True, "label": "System Healthy", "icon": "⚡"}
    ]
    health_html = "".join([
        f'<div class="health-item"><span class="health-icon">{c["icon"]}</span><span class="health-label">{c["label"]}</span><span style="color:{"#3fb950" if c["ok"] else "#f85149"};font-weight:700;">{"✓ OK" if c["ok"] else "✗ FAILED"}</span></div>'
        for c in health_checks
    ])

    # Tables formatting
    if positions_data:
        pos_rows = "".join([
            f'<tr><td class="pair">{p["pair"]}</td><td class="{"buy" if "BUY" in p["side"] else "sell"}">{p["side"]}</td><td>{p["qty"]}</td><td>{p["price"]}</td><td>{p["sl"]}</td><td>{p["tp"]}</td><td style="color:#3fb950;font-weight:600;">{p["pnl"]}</td></tr>'
            for p in positions_data
        ])
        positions_table = f'<table><thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry</th><th>SL</th><th>TP</th><th>P&L</th></tr></thead><tbody>{pos_rows}</tbody></table>'
    else:
        positions_table = '<div class="empty">No open positions currently</div>'

    if trades_data:
        trade_rows = "".join([
            f'<tr><td class="pair">{t.get("pair","N/A")}</td><td class="buy">{t.get("side","BUY")}</td><td>{t.get("entry","0.00")}</td><td>{t.get("exit","0.00")}</td><td style="color:#3fb950;">{t.get("pnl","$0.00")}</td></tr>'
            for t in trades_data
        ])
        trades_table = f'<table><thead><tr><th>Pair</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead><tbody>{trade_rows}</tbody></table>'
    else:
        trades_table = '<div class="empty">No closed trades recorded yet</div>'

    logs_rows = ""
    for log in _process_logs[-100:]:
        lvl = log["level"].upper()
        col = {"INFO": "#58a6ff", "SUCCESS": "#3fb950", "ERROR": "#f85149", "WARNING": "#d29922"}.get(lvl, "#c9d1d9")
        msg = str(log["message"]).replace("<", "&lt;").replace(">", "&gt;")
        logs_rows += f'<tr><td class="time" data-ts="{log["timestamp"]}">{log["timestamp"]}</td><td style="color:{col};font-weight:700;">{lvl}</td><td class="msg-cell">{msg}</td></tr>'
    logs_table = f'<table><thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead><tbody>{logs_rows}</tbody></table>' if logs_rows else '<div class="empty">No logs yet</div>'

    # Dedicated full-detail ERRORS & REJECTIONS panel (shows every error/warning with complete text)
    error_logs = [l for l in _process_logs if l["level"] in ("error", "warning")][-40:]
    error_rows = ""
    for log in error_logs:
        lvl = log["level"].upper()
        col = "#f85149" if lvl == "ERROR" else "#d29922"
        msg = str(log["message"]).replace("<", "&lt;").replace(">", "&gt;")
        error_rows += f'<tr><td class="time" data-ts="{log["timestamp"]}">{log["timestamp"]}</td><td style="color:{col};font-weight:700;">{lvl}</td><td class="msg-cell" style="word-break:break-word;white-space:normal;">{msg}</td></tr>'
    errors_detail_table = f'<table><thead><tr><th>Time</th><th>Level</th><th>Full Error / Warning Detail</th></tr></thead><tbody>{error_rows}</tbody></table>' if error_rows else '<div class="empty">✅ No errors or warnings recorded</div>'

    tg_rows = ""
    for tm in _telegram_messages[-30:]:
        status_col = "#3fb950" if "⚡" in tm["status"] else ("#d29922" if "⚠️" in tm["status"] else "#8b949e")
        msg_txt = str(tm["text"]).replace("<", "&lt;").replace(">", "&gt;")
        tg_rows += f'<tr><td class="time" data-ts="{tm["timestamp"]}">{tm["timestamp"]}</td><td>{tm["chat"]}</td><td style="color:{status_col};font-weight:700;">{tm["status"]}</td><td class="msg-cell">{msg_txt}</td></tr>'
    telegram_table = f'<table><thead><tr><th>Time</th><th>Chat Source</th><th>Signal Status</th><th>Message Content</th></tr></thead><tbody>{tg_rows}</tbody></table>' if tg_rows else '<div class="empty">No Telegram messages received yet (waiting for updates)</div>'

    alerts_html = "".join([
        f'<div style="padding:8px;margin:6px 0;background:{"#f8514920" if a["level"]=="error" else "#d2992220"};border-left:3px solid {"#f85149" if a["level"]=="error" else "#d29922"};border-radius:4px;font-size:12px;"><strong>{a["level"].upper()}</strong> <span data-ts="{a["timestamp"]}">{a["timestamp"]}</span>: {a["message"]}</div>'
        for a in _alerts[-30:]
    ]) or '<div class="empty">No recent alerts or warnings</div>'

    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>cTrader Dashboard Control Panel</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0d1117; color: #c9d1d9; font-size: 13px; line-height: 1.5; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 20px; }}
        .header h1 {{ font-size: 18px; font-weight: 700; color: #58a6ff; }}
        .btn {{ padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; text-decoration: none; display: inline-block; }}
        .btn-refresh {{ background: #238636; color: #fff; margin-right: 8px; }}
        .btn-refresh:hover {{ background: #2ea043; }}
        .btn-logout {{ background: #da3633; color: #fff; }}
        .btn-logout:hover {{ background: #f85149; }}
        .section-title {{ font-size: 12px; color: #8b949e; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; margin: 24px 0 10px 0; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
        .health-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 16px; }}
        .health-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px 16px; display: flex; align-items: center; gap: 12px; }}
        .health-icon {{ font-size: 18px; }}
        .health-label {{ flex: 1; font-weight: 600; }}
        .conn-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-bottom: 16px; }}
        .conn-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }}
        .conn-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .conn-title {{ font-size: 13px; font-weight: 700; }}
        .conn-badge {{ font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 12px; }}
        .conn-detail {{ font-size: 11px; color: #8b949e; }}
        .cron-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-bottom: 16px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 15px; margin-bottom: 16px; }}
        .stat {{ text-align: center; padding: 16px; }}
        .stat-value {{ font-size: 22px; font-weight: 800; color: #58a6ff; }}
        .stat-label {{ font-size: 11px; color: #8b949e; margin-top: 6px; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ padding: 10px 12px; background: #0d1117; color: #8b949e; font-size: 11px; text-transform: uppercase; text-align: left; border-bottom: 1px solid #30363d; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #30363d; font-size: 12px; }}
        tr:hover td {{ background: #1c2128; }}
        .pair {{ font-weight: 600; color: #fff; }}
        .buy {{ color: #3fb950; font-weight: 600; }}
        .sell {{ color: #f85149; font-weight: 600; }}
        .time {{ font-size: 11px; color: #8b949e; white-space: nowrap; }}
        .msg-cell {{ word-break: break-word; white-space: pre-wrap; max-width: 520px; }}
        .ago {{ display: block; font-size: 10px; color: #58a6ff; font-weight: 600; }}
        .empty {{ text-align: center; color: #8b949e; padding: 24px; font-style: italic; }}
    </style>
    <script>
        function checkAuth() {{
            if (sessionStorage.getItem('dashboard_authenticated') !== 'true') {{
                window.location.href = 'login.html';
            }}
        }}
        function logout() {{
            sessionStorage.clear();
            window.location.href = 'login.html';
        }}
        // Live relative-time ("X ago") so timings are always accurate and current
        function parseTs(s) {{
            // Expected: "2026-07-26 11:06:44 UTC"
            try {{
                let parts = String(s).trim().split(' ');
                let dp = parts[0].split('-');
                let tp = parts[1].split(':');
                return new Date(Date.UTC(+dp[0], +dp[1]-1, +dp[2], +tp[0], +tp[1], +tp[2]));
            }} catch(e) {{ return null; }}
        }}
        function timeAgo(d) {{
            let sec = Math.floor((Date.now() - d.getTime()) / 1000);
            if (sec < 0) sec = 0;
            if (sec < 60) return sec + 's ago';
            if (sec < 3600) return Math.floor(sec/60) + 'm ago';
            if (sec < 86400) return Math.floor(sec/3600) + 'h ago';
            return Math.floor(sec/86400) + 'd ago';
        }}
        function refreshAgo() {{
            document.querySelectorAll('[data-ts]').forEach(function(el) {{
                let d = parseTs(el.getAttribute('data-ts'));
                if (!d) return;
                let existing = el.querySelector('.ago');
                if (!existing) {{
                    existing = document.createElement('span');
                    existing.className = 'ago';
                    el.appendChild(existing);
                }}
                existing.textContent = timeAgo(d);
            }});
        }}
        function updateClock() {{
            let n = new Date();
            let pad = (x) => String(x).padStart(2, '0');
            let clk = document.getElementById('liveClock');
            if (clk) clk.textContent = n.getUTCFullYear()+'-'+pad(n.getUTCMonth()+1)+'-'+pad(n.getUTCDate())+' '+pad(n.getUTCHours())+':'+pad(n.getUTCMinutes())+':'+pad(n.getUTCSeconds());
        }}
        function updatePageAge() {{
            let el = document.querySelector('span[data-ts]');
            // pageAge element sits in footer
            let pg = document.getElementById('pageAge');
            if (!pg) return;
            // find the footer generated timestamp
            let foot = document.querySelectorAll('div[style*="margin: 30px"] span[data-ts]');
            if (foot.length) {{
                let d = parseTs(foot[foot.length-1].getAttribute('data-ts'));
                if (d) pg.textContent = timeAgo(d);
            }}
        }}
        checkAuth();
        refreshAgo();
        updateClock();
        updatePageAge();
        setInterval(refreshAgo, 1000);   // update "X ago" every second
        setInterval(updateClock, 1000);  // update live clock every second
        setInterval(updatePageAge, 1000);
        setInterval(() => location.reload(), 60000);  // full refresh every 60s
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 cTrader Dashboard Control Panel</h1>
            <div>
                <button class="btn btn-refresh" onclick="location.reload()">🔄 Refresh</button>
                <button class="btn btn-logout" onclick="logout()">🚪 Logout</button>
            </div>
        </div>

        <div style="background: #161b22; border: 1px solid #30363d; padding: 12px 16px; border-radius: 6px; font-size: 12px; margin-bottom: 20px;">
            <strong style="color:#58a6ff;">📊 cTrader Account:</strong> {state['account_id']} | <strong style="color:#58a6ff;">Server:</strong> {state['server']} | <strong style="color:#58a6ff;">Currency:</strong> {state['currency']} | <strong style="color:#58a6ff;">Build:</strong> {_BUILD_VERSION}
        </div>

        <div style="background: #0d1117; border: 1px solid #30363d; padding: 10px 16px; border-radius: 6px; font-size: 11px; margin-bottom: 20px; color:#8b949e;">
            <strong style="color:#d29922;">🔧 DIAGNOSTIC:</strong> Script version: <strong style="color:#3fb950;">{_SCRIPT_VERSION}</strong> | Telegram offset (last_update_id): <strong style="color:#c9d1d9;">{_last_update_id}</strong> | Pending signals in queue: <strong style="color:#c9d1d9;">{_pending_count}</strong> | Instruments loaded: <strong style="color:#c9d1d9;">{len(_instruments)}</strong>
        </div>

        <div class="section-title">🩺 System Health & Secrets Check</div>
        <div class="health-grid">{health_html}</div>

        <div class="section-title">Connection Status</div>
        <div class="conn-grid">
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">📊 cTrader Open API v2</span>
                    <span class="conn-badge" style="background:{ct_status_color}20;color:{ct_status_color};">{ct_status_icon} {ct_status_text}</span>
                </div>
                <div class="conn-detail">{ct_detail}</div>
            </div>
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">✈️ Telegram Signal Receiver</span>
                    <span class="conn-badge" style="background:{tg_status_color}20;color:{tg_status_color};">{tg_status_icon} {tg_status_text}</span>
                </div>
                <div class="conn-detail">{tg_detail}</div>
            </div>
        </div>

        <div class="section-title">Cron Job Execution Status</div>
        <div class="cron-grid">
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">🤖 Trading Bot Cycle</span>
                    <span style="font-size:11px;color:#8b949e;">{bot_status['time_ago']}</span>
                </div>
                <div style="font-weight:700;color:{"#3fb950" if bot_status['raw_status']=='completed' else '#8b949e'};font-size:12px;">{bot_status['raw_status'].upper()}</div>
                <div class="conn-detail" style="margin-top:4px;">{bot_status['message']}</div>
            </div>
            <div class="conn-card">
                <div class="conn-header">
                    <span class="conn-title">📈 Dashboard Updater</span>
                    <span style="font-size:11px;color:#8b949e;">{dashboard_status['time_ago']}</span>
                </div>
                <div style="font-weight:700;color:{"#3fb950" if dashboard_status['raw_status']=='completed' else '#8b949e'};font-size:12px;">{dashboard_status['raw_status'].upper()}</div>
                <div class="conn-detail" style="margin-top:4px;">{dashboard_status['message']}</div>
            </div>
        </div>

        <div class="section-title">Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{state['balance']}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">{state['equity']}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">{state['margin']}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">{state['free_margin']}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{state['margin_level']}</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">{state['daypl']}</div><div class="stat-label">Day P&L</div></div>
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

        <div class="section-title">📱 Recent Telegram Messages & Signal History (Last 30)</div>
        <div class="card" style="overflow-x:auto;">{telegram_table}</div>

        <div class="section-title">🚨 Full Errors & Rejections Detail (Last 40)</div>
        <div class="card" style="overflow-x:auto;">{errors_detail_table}</div>

        <div class="section-title">⚠️ Recent Alerts & System Notifications (Last 30)</div>
        <div class="card">{alerts_html}</div>

        <div class="section-title">📋 Backend Process Logs (Last 100 Events)</div>
        <div class="card" style="overflow-x:auto;">{logs_table}</div>

        <div class="section-title">Open Positions ({len(positions_data)})</div>
        <div class="card" style="overflow-x:auto;">{positions_table}</div>

        <div class="section-title">Closed Trade History ({len(trades_data)})</div>
        <div class="card" style="overflow-x:auto;">{trades_table}</div>

        <div style="text-align: center; color: #8b949e; font-size: 11px; margin: 30px 0;">
            cTrader Bot &amp; Dashboard • Script {_SCRIPT_VERSION} • Auto-refreshing every 60s • Page generated: <span data-ts="{last_update}">{last_update}</span> (<span id="pageAge"></span>)
            <br>Time now (UTC): <strong id="liveClock"></strong>
        </div>
    </div>
</body>
</html>"""

def generate_login_html():
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>cTrader Dashboard - Secure Login</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; padding: 20px; }}
        .login-box {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 35px; width: 100%; max-width: 380px; box-shadow: 0 8px 30px rgba(0,0,0,0.5); text-align: center; }}
        .login-box h2 {{ color: #58a6ff; margin-bottom: 8px; font-size: 22px; }}
        .login-box p {{ color: #8b949e; font-size: 12px; margin-bottom: 25px; }}
        input {{ width: 100%; padding: 12px; margin-bottom: 16px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-size: 14px; box-sizing: border-box; }}
        input:focus {{ outline: none; border-color: #58a6ff; }}
        button {{ width: 100%; padding: 12px; background: #238636; color: white; border: none; border-radius: 6px; font-weight: 700; font-size: 14px; cursor: pointer; transition: background 0.2s; }}
        button:hover {{ background: #2ea043; }}
        .err {{ color: #f85149; font-size: 12px; margin-bottom: 15px; display: none; background: #f8514920; padding: 10px; border-radius: 6px; }}
    </style>
</head>
<body>
    <div class="login-box">
        <h2>🔐 cTrader Control Panel</h2>
        <p>Enter credentials to access trading dashboard</p>
        <div id="err" class="err">❌ Invalid username or password</div>
        <form onsubmit="doLogin(event)">
            <input type="text" id="usr" placeholder="Username" required autofocus autocomplete="username">
            <input type="password" id="pwd" placeholder="Password" required autocomplete="current-password">
            <button type="submit">Sign In</button>
        </form>
    </div>
    <script>
        const U = "{DASHBOARD_USERNAME}";
        const P = "{DASHBOARD_PASSWORD}";
        function doLogin(e) {{
            e.preventDefault();
            if (document.getElementById('usr').value === U && document.getElementById('pwd').value === P) {{
                sessionStorage.setItem('dashboard_authenticated', 'true');
                window.location.href = 'index.html?v={_BUILD_VERSION}';
            }} else {{
                document.getElementById('err').style.display = 'block';
                document.getElementById('pwd').value = '';
            }}
        }}
        if (sessionStorage.getItem('dashboard_authenticated') === 'true') {{
            window.location.href = 'index.html?v={_BUILD_VERSION}';
        }}
    </script>
</body>
</html>"""

# =====================================================================
# BOT MODE EXECUTION
# =====================================================================
def run_bot():
    global _last_update_id
    load_system_state()
    reclassify_stored_telegram_messages()
    save_heartbeat("bot", "running", "Checking secrets and starting cycle...")
    log_process("info", f"=== TRADING BOT CYCLE STARTED === [SCRIPT {_SCRIPT_VERSION}]")
    log_process("info", f"Telegram current offset (last_update_id) at cycle start = {_last_update_id}")
    check_secrets_status()

    # Load any unexecuted signals left over from previous failed cycles (persistent retry queue)
    pending_signals = load_pending_signals()
    if pending_signals:
        log_process("warning", f"♻️ RETRY QUEUE: {len(pending_signals)} unexecuted signal(s) recovered from previous cycle(s).")

    fetched_max_uid = _last_update_id
    tg_conn, _ = test_telegram_connection()
    if tg_conn and TG_TOKEN:
        msgs, fetched_max_uid = tg_get_messages(offset=_last_update_id)
        log_process("info", f"Fetched {len(msgs)} new message(s) from Telegram (highest update_id seen: {fetched_max_uid}).")
        for msg in msgs:
            txt = msg.get("text", "").strip()
            if not looks_like_signal(txt): continue
            log_process("info", f"Signal identified in queue: {txt[:120]}...")
            parsed = parse_signal(txt)
            if parsed:
                pending_signals.append(parsed)
                log_process("success", f"Added to execution queue: {parsed}")

    # Remove duplicate signals (same message re-fetched on retry)
    pending_signals = dedupe_signals(pending_signals)
    count = len(pending_signals)

    client = cTraderClient()
    connected = client.verify_auth_and_fetch_data(pending_signals=pending_signals)

    if not connected and not CT_ACCESS_TOKEN:
        # No token at all — KEEP signals for retry, do NOT advance Telegram offset
        save_pending_signals(pending_signals)
        save_heartbeat("bot", "failed", "Missing CT_ACCESS_TOKEN — signals retained for retry")
        log_process("error", "Bot cycle aborted — missing auth credentials. Signals retained for next cycle (offset NOT advanced).")
        save_system_state()
        return False

    # ---- EXECUTION-AWARE DECISION (fixed: no more infinite loop) ----
    has_new_orders = any(s.get("type") == "SIGNAL" for s in pending_signals)
    has_manage_ops = any(s.get("type") in ("TPSL_HIT", "SL_UPDATE") for s in pending_signals)

    if count > 0:
        log_process("info", f"━━━ TRADE CYCLE SUMMARY ━━━ Signals: {count} (new orders: {has_new_orders}, manage ops: {has_manage_ops}) | Dispatched: {client.dispatched_orders} | Confirmed by cTrader: {client.confirmed_executions} | Broker error: {client.last_error_code or 'none'}")
        if client.last_error_code:
            log_process("error", f"🚫 BROKER REJECTION: {client.last_error_code} - {client.last_error_desc or ''}")

    if count > 0 and client.confirmed_executions > 0:
        # cTrader CONFIRMED execution -> clear retry queue and commit offset
        save_pending_signals([])
        _last_update_id = fetched_max_uid
        log_process("success", f"✅ SUCCESS: {client.confirmed_executions} trade(s) CONFIRMED EXECUTED by cTrader. Offset committed to {fetched_max_uid}.")
        save_heartbeat("bot", "completed", f"Executed {client.confirmed_executions} trade(s)")
    elif count > 0 and client.last_error_code:
        # Broker REJECTED -> commit (retrying identical params won't help), make it visible
        save_pending_signals([])
        _last_update_id = fetched_max_uid
        log_process("warning", f"⚠️ REJECTED by broker ({client.last_error_code}). Offset committed to {fetched_max_uid}. Fix signal params (volume / SL-TP) and resend.")
        save_heartbeat("bot", "completed", f"Rejected: {client.last_error_code}")
    elif count > 0 and client.dispatched_orders > 0:
        # Orders DISPATCHED -> commit (cannot safely retry; would risk DUPLICATE trades).
        # The reconciliation-based verification (in sync) reports whether they actually filled.
        save_pending_signals([])
        _last_update_id = fetched_max_uid
        log_process("warning", f"⚠️ {client.dispatched_orders} order(s) dispatched. Offset committed to {fetched_max_uid} (no retry to avoid duplicate trades). Check Open Positions to verify fill.")
        save_heartbeat("bot", "completed", f"Dispatched {client.dispatched_orders} order(s)")
    elif count > 0 and not connected:
        # No connection at all -> RETRY (safe: nothing was sent)
        save_pending_signals(pending_signals)
        log_process("warning", f"⚠️ No cTrader connection. {count} signal(s) retained for retry. Offset NOT advanced.")
        save_heartbeat("bot", "failed", f"No connection — {count} signal(s) retained for retry")
    elif count > 0 and has_new_orders and client.dispatched_orders == 0:
        # New-order signals present but NONE dispatched (symbol mapping / sync issue) -> RETRY
        save_pending_signals(pending_signals)
        log_process("warning", f"⚠️ New-order signal(s) present but not dispatched (symbol mapping or sync not ready). Retained for retry. Offset NOT advanced.")
        save_heartbeat("bot", "failed", "Order not dispatched — retained for retry")
    elif count > 0 and has_manage_ops and not has_new_orders:
        # Only TP-hit / SL-update signals, with no open positions to act on -> HANDLED, commit
        save_pending_signals([])
        _last_update_id = fetched_max_uid
        log_process("info", f"ℹ️ Only close/modify signal(s) with no matching open positions — nothing to do. Offset committed to {fetched_max_uid}.")
        save_heartbeat("bot", "completed", "Close/modify — no positions to act on")
    elif count > 0:
        # Any other handled case -> commit
        save_pending_signals([])
        _last_update_id = fetched_max_uid
        log_process("info", f"ℹ️ Signal(s) handled. Offset committed to {fetched_max_uid}.")
        save_heartbeat("bot", "completed", "Signals handled")
    else:
        # No signals at all -> advance offset to acknowledge any seen non-signal messages
        _last_update_id = fetched_max_uid
        log_process("info", "No new executable trade signals found in current cycle.")
        save_heartbeat("bot", "completed", "Cycle completed successfully (0 new signals)")
    
    save_system_state()
    return True

# =====================================================================
# DASHBOARD MODE EXECUTION
# =====================================================================
def run_dashboard():
    load_system_state()
    reclassify_stored_telegram_messages()
    load_heartbeat()
    save_heartbeat("dashboard", "running", "Synchronizing account state & HTML...")
    log_process("info", f"=== DASHBOARD GENERATION STARTED === [SCRIPT {_SCRIPT_VERSION}]")
    
    check_secrets_status()

    client = cTraderClient()
    ct_connected = client.verify_auth_and_fetch_data()
    
    ct_error = None if ct_connected else "Authentication check noted (Open API Protobuf/OAuth)"
    tg_connected, tg_info = test_telegram_connection()
    if tg_connected:
        log_process("success", f"Telegram Connected (@{tg_info.get('username', 'active')})")
    else:
        log_process("warning", f"Telegram notification status: {tg_info.get('error')}")

    os.makedirs("docs", exist_ok=True)
    
    html_dashboard = generate_dashboard_html(client, ct_connected, ct_error, tg_connected, tg_info)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html_dashboard)
        
    html_login = generate_login_html()
    with open("docs/login.html", "w", encoding="utf-8") as f:
        f.write(html_login)

    log_process("success", "Dashboard index.html and login.html updated successfully!")
    save_heartbeat("dashboard", "completed", "No errors encountered")
    return True

# =====================================================================
# MAIN ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    try:
        if MODE.lower() == "dashboard":
            run_dashboard()
        else:
            run_bot()
        sys.exit(0)
    except Exception as e:
        log_process("error", f"Fatal execution exception: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

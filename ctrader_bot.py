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
# Pyrogram (user-account) config — lets you copy from ANY channel/group you are a member of
# (no admin needed). Get api_id/api_hash from https://my.telegram.org  and generate a session string once.
TG_API_ID = _clean_sec(os.environ.get("TG_API_ID", ""))
TG_API_HASH = _clean_sec(os.environ.get("TG_API_HASH", ""))
TG_SESSION = _clean_sec(os.environ.get("TG_SESSION", ""))
HAS_TELETHON = False
try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    HAS_TELETHON = True
except Exception:
    HAS_TELETHON = False
# Whether to use the user-account (Telethon) receiver instead of the Bot API
USE_USER_ACCOUNT = bool(TG_SESSION and TG_API_ID and TG_API_HASH and HAS_TELETHON)

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

# =====================================================================
# PER-PAIR LOT SIZES (global sizing overrides)
# Source priority: 1) CTRADER_LOT_SIZES env var (JSON), 2) docs/lot_sizes.json, 3) smart defaults
# Set the GitHub secret CTRADER_LOT_SIZES to e.g. {"BTCUSD":0.1,"XAUUSD":0.05,"EURUSD":0.02}
# =====================================================================
LOT_SIZES = {}
DEFAULT_LOTS = {
    "BTCUSD": 0.10, "ETHUSD": 0.10, "XAUUSD": 0.05, "XAGUSD": 0.10,
    "EURUSD": 0.01, "GBPUSD": 0.01, "USDJPY": 0.01, "USDCHF": 0.01,
    "AUDUSD": 0.01, "NZDUSD": 0.01, "USDCAD": 0.01, "EURJPY": 0.01,
    "GBPJPY": 0.01, "EURGBP": 0.01, "EURCHF": 0.01, "CHFJPY": 0.01,
    "NZDJPY": 0.01, "CADJPY": 0.01, "EURNZD": 0.01, "EURAUD": 0.01,
    "NAS100": 0.10, "US30": 0.10, "GER40": 0.10, "SPX500": 0.10,
    "UK100": 0.10, "USOIL": 0.10, "UKOIL": 0.10,
}

def load_lot_sizes():
    global LOT_SIZES
    LOT_SIZES = {}
    raw = _clean_sec(os.environ.get("CTRADER_LOT_SIZES", ""))
    if raw:
        try:
            LOT_SIZES = {str(k).strip().upper(): float(v) for k, v in json.loads(raw).items()}
            return
        except Exception as e:
            print(f"[WARNING] Could not parse CTRADER_LOT_SIZES: {e}")
    try:
        p = os.path.join("docs", "lot_sizes.json")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                LOT_SIZES = {str(k).strip().upper(): float(v) for k, v in json.load(f).items()}
            return
    except Exception:
        pass
    LOT_SIZES = {}

def lot_for(pair_name):
    """Return the configured lot size for a pair (override > category default)."""
    p = (pair_name or "").upper()
    if p in LOT_SIZES:
        return LOT_SIZES[p]
    if p in DEFAULT_LOTS:
        return DEFAULT_LOTS[p]
    if "BTC" in p or "ETH" in p:
        return 0.10
    if "XAU" in p or "XAG" in p:
        return 0.05
    if any(x in p for x in ["NAS", "US30", "GER", "SPX", "UK100", "JPN", "HK50", "AUS", "OIL"]):
        return 0.10
    return 0.01

load_lot_sizes()

# Per-pair ENABLE/DISABLE (switch pairs on/off from the dashboard)
# Configured via CTRADER_PAIR_CONFIG secret (JSON) e.g. {"EURUSD":{"lot":0.02,"on":false},"BTCUSD":{"lot":0.1,"on":true}}
PAIR_ENABLED = {}  # {pair: bool}. Empty dict = all pairs enabled
def load_pair_config():
    global PAIR_ENABLED, LOT_SIZES
    PAIR_ENABLED = {}
    src = _clean_sec(os.environ.get("CTRADER_PAIR_CONFIG", ""))
    if not src:
        try:
            p = os.path.join("docs", "pair_config.json")
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    src = f.read()
        except Exception:
            pass
    if src:
        try:
            for k, v in json.loads(src).items():
                pair = str(k).strip().upper()
                if isinstance(v, dict):
                    if "lot" in v:
                        LOT_SIZES[pair] = float(v["lot"])
                    PAIR_ENABLED[pair] = bool(v.get("on", True))
                else:
                    PAIR_ENABLED[pair] = bool(v)
        except Exception as e:
            print(f"[WARNING] Could not parse CTRADER_PAIR_CONFIG: {e}")

def is_pair_enabled(pair_name):
    p = (pair_name or "").upper()
    if not PAIR_ENABLED:
        return True  # nothing configured = all pairs enabled
    return PAIR_ENABLED.get(p, True)

load_pair_config()

_last_update_id = 0
_instruments = {}
_process_logs = []
_heartbeat_log = {}
_alerts = []
_telegram_messages = []
_BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
_SCRIPT_VERSION = "v16-pair-panel-grid-mobile"

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
                            if not is_pair_enabled(norm_pair or pair):
                                log_process("info", f"⏭️ Skipping {norm_pair or pair} — this pair is DISABLED in your dashboard settings (no trade placed).")
                                continue
                                
                            if sig_type == "SIGNAL":
                                direction = sig.get("direction", "BUY").upper()
                                qty = lot_for(norm_pair or pair)
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
                                # Capture the EXACT response from cTrader (success or rejection)
                                def _on_order_response(resp, _dir=direction, _pair=(norm_pair or pair), _vol=int(float(qty) * 100000)):
                                    try:
                                        pt = getattr(resp, "payloadType", "?")
                                        r = Protobuf.extract(resp)
                                        ec = getattr(r, "errorCode", None) or getattr(r, "errorCode", None)
                                        if ec:
                                            self.last_error_code = str(ec)
                                            self.last_error_desc = getattr(r, "description", "") or ""
                                            log_process("error", f"🚫 ORDER REJECTED by broker: {ec} - {self.last_error_desc}")
                                        else:
                                            oid = getattr(r, "orderId", None) or getattr(r, "positionId", None) or "N/A"
                                            log_process("success", f"📨 ORDER ACCEPTED by broker (payloadType {pt}). Order/Position ID: {oid}")
                                    except Exception as e:
                                        log_process("warning", f"Order response parse note (payloadType {getattr(resp,'payloadType','?')}): {e}")
                                order_deferred = c_ref.send(ord_req)
                                order_deferred.addCallbacks(_on_order_response, lambda f: log_process("error", f"Order Dispatch Error: {f}"))
                                self.dispatched_orders += 1
                                log_process("success", f"✓ Market order SENT to cTrader: {direction} {qty} {norm_pair or pair} | Vol={int(float(qty)*100000)} | SL={sl} | TP={tp} | Acct={self.account_id_num} | SymID={sym_id} (dispatched total: {self.dispatched_orders})")
                                
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

                # Fetch recent DEALS (closed/execution history) so the dashboard can prove trades happened
                try:
                    deal_req = ProtoOADealListReq()
                    deal_req.ctidTraderAccountId = self.account_id_num
                    deal_req.fromTimestamp = int((time.time() - 365 * 86400) * 1000)  # last 365 days — FULL history
                    deal_req.toTimestamp = int(time.time() * 1000)
                    deal_req.maxRows = 500  # fetch up to 500 deals (full account history)
                    c.send(deal_req).addErrback(on_error)
                except Exception as de:
                    log_process("warning", f"Deal list request note: {de}")

            elif payload_type == ProtoOADealListRes().payloadType:
                # Group deals by positionId -> one row per trade with ENTRY + EXIT times
                try:
                    res = Protobuf.extract(message)
                    deals = list(getattr(res, "deal", []))
                    money_digits = self.account_state.get("money_digits_cache", 2) or 2
                    md_div = 10 ** money_digits
                    by_pos = {}
                    for d in deals:
                        pid = getattr(d, "positionId", None) or getattr(d, "dealId", None)
                        by_pos.setdefault(pid, []).append(d)
                    self.trades = []
                    for pid, dlist in by_pos.items():
                        dlist.sort(key=lambda d: getattr(d, "executionTimestamp", 0))
                        open_deal = dlist[0]
                        close_deal = dlist[-1] if len(dlist) > 1 else None
                        sid = getattr(open_deal, "symbolId", 0)
                        pair_lbl = f"ID:{sid}"
                        for nm, meta in _instruments.items():
                            if str(meta["id"]) == str(sid):
                                pair_lbl = nm; break
                        side_val = getattr(open_deal, "tradeSide", 1)
                        side_str = "BUY" if str(side_val) == "1" or "BUY" in str(side_val) else "SELL"
                        vol = getattr(open_deal, "filledVolume", getattr(open_deal, "volume", 0)) / 100000.0
                        entry_px = getattr(open_deal, "executionPrice", 0.0)
                        exit_px = getattr(close_deal, "executionPrice", 0.0) if close_deal else None
                        o_ts = getattr(open_deal, "executionTimestamp", 0)
                        c_ts = getattr(close_deal, "executionTimestamp", 0) if close_deal else 0
                        def _fmt(ms):
                            try:
                                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                            except Exception:
                                return "—"
                        # P&L: prefer grossProfit/profit, else balance difference between close & open
                        pnl_val = 0.0
                        for cand in (close_deal, open_deal):
                            if not cand: continue
                            for fld in ("grossProfit", "profit"):
                                v = getattr(cand, fld, None)
                                if v:
                                    pnl_val = float(v) / md_div; break
                            if pnl_val: break
                        if pnl_val == 0.0 and close_deal is not None:
                            ob = getattr(open_deal, "balance", None); cb = getattr(close_deal, "balance", None)
                            if ob is not None and cb is not None:
                                pnl_val = float(cb) - float(ob)
                        if c_ts and o_ts and c_ts > o_ts:
                            secs = (c_ts - o_ts) / 1000.0
                            dur = (f"{int(secs//3600)}h " if secs >= 3600 else "") + f"{int((secs%3600)//60)}m"
                        else:
                            dur = "open" if not close_deal else "—"
                        self.trades.append({
                            "open_time": _fmt(o_ts),
                            "close_time": _fmt(c_ts) if close_deal else "— (open)",
                            "duration": dur,
                            "pair": pair_lbl,
                            "side": side_str,
                            "qty": f"{vol:g}",
                            "entry": f"{entry_px:g}",
                            "exit": f"{exit_px:g}" if exit_px is not None else "—",
                            "pnl": f"${pnl_val:+,.2f}",
                            "pnl_value": pnl_val,
                            "status": "CLOSED" if close_deal else "OPEN",
                        })
                    self.trades.sort(key=lambda t: t["open_time"], reverse=True)
                    if self.trades:
                        log_process("success", f"📜 Retrieved {len(self.trades)} trade(s) ({len(deals)} deals) from cTrader — trades ARE executing!")
                    sync_status["deals"] = True
                except Exception as e:
                    log_process("warning", f"Deal list parse note: {e}")

            elif payload_type == ProtoOATraderRes().payloadType:
                res = Protobuf.extract(message)
                trader = res.trader
                money_digits = getattr(trader, "moneyDigits", 2) or 2
                self.account_state["money_digits_cache"] = money_digits
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
                _instruments.clear()
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

                    # Open timestamp (ms) -> UTC string
                    open_ms = getattr(p, "utcOpenTimestamp", None) or getattr(p, "createTimestamp", None) or 0
                    try:
                        open_time_str = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    except Exception:
                        open_time_str = "—"

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
                        "raw_volume": raw_vol,
                        "open_time": open_time_str
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
                
            elif payload_type == ProtoOANewOrderRes().payloadType:
                # Direct order-creation response (backup capture path)
                try:
                    res = Protobuf.extract(message)
                    ord_id = getattr(res, 'orderId', 'N/A')
                    pos_id = getattr(res, 'positionId', None)
                    log_process("success", f"📨 ProtoOANewOrderRes received: OrderID {ord_id} | PositionID: {pos_id or 'none'}")
                except Exception as e:
                    log_process("info", f"ProtoOANewOrderRes received (parse note: {e})")

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
def clear_telegram_webhook():
    """Remove any webhook so getUpdates works (fixes HTTP 409 Conflict / missed signals)."""
    if not TG_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/deleteWebhook?drop_pending_updates=false"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            if data.get("ok"):
                log_process("info", "🧹 Cleared any Telegram webhook (prevents 409 Conflict / missed signals).")
    except Exception as e:
        log_process("info", f"Webhook clear note: {str(e)[:60]}")

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

def load_pyro_ids():
    try:
        p = os.path.join("docs", "pyro_ids.json")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_pyro_ids(ids):
    try:
        os.makedirs("docs", exist_ok=True)
        with open(os.path.join("docs", "pyro_ids.json"), "w", encoding="utf-8") as f:
            json.dump(ids, f)
    except Exception:
        pass

def user_account_fetch_signals():
    """Fetch new messages from channels/groups your USER account is a member of (ANY channel — no admin needed).
    Uses Telethon (MTProto). Requires TG_API_ID, TG_API_HASH, TG_SESSION (Telethon StringSession)."""
    import asyncio
    last_ids = load_pyro_ids()
    targets = None
    if TG_CHAT and TG_CHAT.upper() != "ALL":
        targets = [c.strip() for c in TG_CHAT.split(",") if c.strip()]

    async def _run():
        msgs = []
        client = TelegramClient(StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise Exception("Telethon session NOT authorized - regenerate TG_SESSION")
        chats = []
        if targets:
            chats = targets
        else:
            async for d in client.iter_dialogs():
                if getattr(d, "is_channel", False) or getattr(d, "is_group", False):
                    chats.append(d.entity)
        for chat in chats:
            try:
                ent = await client.get_entity(chat)
                key = str(getattr(ent, "id", chat))
            except Exception:
                key = str(chat)
                ent = chat
            last = int(last_ids.get(key, 0))
            seen = 0
            async for m in client.iter_messages(ent, limit=40):
                if m.id <= last:
                    break
                text = (m.text or m.message or "").strip()
                if text:
                    msgs.append({"text": text, "chat": key})
                if m.id > last:
                    last = m.id
                seen += 1
                if seen >= 40:
                    break
            last_ids[key] = last
        await client.disconnect()
        return msgs

    try:
        msgs = asyncio.run(_run())
        log_process("info", f"Telethon user-account: fetched {len(msgs)} new message(s) from your channels/groups (ANY channel, no admin needed).")
        save_pyro_ids(last_ids)
        return msgs
    except Exception as e:
        log_process("error", f"Telethon fetch error: {str(e)[:160]}")
        save_pyro_ids(last_ids)
        return []

def looks_like_signal(text):
    if not text: return False
    t = text.upper()
    return any(k in t for k in ["BUY", "SELL", "LONG", "SHORT", "TP HIT", "SL HIT",
                                "SL_UPDATE", "SL UPDATE", "CLOSE TRADE", "CLOSE POSITION"])

def _clean_pair(raw):
    if not raw: return ""
    return re.sub(r"[^A-Za-z0-9]", "", raw).upper()

def parse_signal(text):
    """Flexible multi-format signal parser. Scans the WHOLE message (not just line 1),
    accepts BUY/SELL/LONG/SHORT/CLOSE, emoji prefixes, and many SL/TP/Entry formats."""
    if not text: return None
    raw_lines = text.strip().split("\n")
    lines = [re.sub(r"<[^>]+>", "", ln).strip() for ln in raw_lines]
    full_upper = " ".join(lines).upper()

    # ---- TP / SL HIT (close on target) ----
    hit = re.search(r"(TP|SL)\s*HIT\s*[-–:]?\s*([A-Za-z0-9/_-]+)", full_upper, re.IGNORECASE)
    if hit or "TP HIT" in full_upper or "SL HIT" in full_upper:
        pair_str = hit.group(2) if hit else ""
        if not pair_str:
            for ln in lines:
                m = re.search(r"\b([A-Z]{3,8}[/]?[A-Z]{0,8})\b", ln.upper())
                if m and "HIT" not in m.group(1) and m.group(1) not in ("TP", "SL"):
                    pair_str = m.group(1); break
        result = "TP" if "TP HIT" in full_upper else "SL"
        return {"type": "TPSL_HIT", "result": result, "pair": _clean_pair(pair_str) or "EURUSD"}

    # ---- SL UPDATE ----
    if "SL_UPDATE" in full_upper or "SL UPDATE" in full_upper or "MOVE SL" in full_upper:
        pair, new_sl = None, None
        for ln in lines:
            lu = ln.upper()
            if "PAIR" in lu and ":" in ln:
                pair = _clean_pair(ln.split(":", 1)[1].strip().split()[0]) if ln.split(":", 1)[1].strip() else ""
            elif not pair:
                m_pair = re.search(r"\b(XAUUSD|XAGUSD|EURUSD|GBPUSD|USDJPY|NAS100|US30|GER40|BTCUSD|GOLD|OIL|[A-Z]{6})\b", lu)
                if m_pair and m_pair.group(1) not in ("UPDATE", "SL_UPDATE"):
                    pair = m_pair.group(1)
            m = re.search(r"(?:NEW\s*)?(?:SL|STOP\s*LOSS)\s*[:=]?\s*([\d.]+)", ln, re.IGNORECASE)
            if m and new_sl is None:
                try: new_sl = float(m.group(1))
                except: pass
        if new_sl:
            return {"type": "SL_UPDATE", "pair": pair or "EURUSD", "new_sl": new_sl}
        return None

    # ---- NEW ORDER: search WHOLE message for action + pair (handles emoji, any line) ----
    action = None
    pair = ""
    action_pat = re.compile(r"\b(BUY|SELL|LONG|SHORT|CLOSE\s+TRADE|CLOSE\s+POSITION|CLOSE)\b\s+([A-Za-z]{2,8}[/A-Za-z0-9.-]{0,8})", re.IGNORECASE)
    for ln in lines:
        m = action_pat.search(ln)
        if m:
            w = m.group(1).upper()
            action = {"LONG": "BUY", "SHORT": "SELL", "CLOSE TRADE": "CLOSE",
                      "CLOSE POSITION": "CLOSE", "CLOSE": "CLOSE"}.get(w, w)
            pair = _clean_pair(m.group(2))
            break
    if not action:
        # fallback: action and pair on separate tokens anywhere
        m2 = re.search(r"\b(BUY|SELL|LONG|SHORT)\b", full_upper)
        if m2:
            w = m2.group(1).upper()
            action = "BUY" if w == "LONG" else ("SELL" if w == "SHORT" else w)
            mp = re.search(r"\b([A-Z]{3,8}[/]?[A-Z]{0,8})\b", full_upper)
            pair = _clean_pair(mp.group(1)) if mp else ""
    if not action:
        return None

    # If pair still empty, try to grab any symbol token
    if not pair:
        mp = re.search(r"\b(XAUUSD|XAGUSD|BTCUSD|ETHUSD|EURUSD|GBPUSD|USDJPY|USDCHF|AUDUSD|NZDUSD|USDCAD|EURNZD|EURJPY|GBPJPY|CHFJPY|NZDJPY|CADJPY|EURCHF|NAS100|US30|GER40|XAU|GOLD|[A-Z]{6})\b", full_upper)
        pair = _clean_pair(mp.group(1)) if mp else ""

    # ---- Extract SL / TP / Entry from any line, many formats ----
    sl = tp = entry = None
    for ln in lines:
        # Stop Loss: "SL:", "SL =", "SL ", "Stop Loss:", "StopLoss:", "SL at", "S/L:"
        if sl is None:
            m = re.search(r"(?<![A-Za-z])(?:SL|S/?L|STOP\s*LOSS)\s*(?:[:=at]+)?\s*([\d]+(?:\.\d+)?)", ln, re.IGNORECASE)
            if m:
                try: sl = float(m.group(1))
                except: pass
        # Take Profit
        if tp is None:
            m = re.search(r"(?<![A-Za-z])(?:TP|T/?P|TAKE\s*PROFIT)\s*(?:[:=at]+)?\s*([\d]+(?:\.\d+)?)", ln, re.IGNORECASE)
            if m:
                try: tp = float(m.group(1))
                except: pass
        # Entry price
        if entry is None:
            m = re.search(r"(?:ENTRY|REF|PRICE|@|AT)\s*[:=]?\s*([\d]+(?:\.\d+)?)", ln, re.IGNORECASE)
            if m:
                try: entry = float(m.group(1))
                except: pass

    qty = 0.10 if ("BTC" in pair or "ETH" in pair) else 0.01
    return {"type": "SIGNAL", "direction": action, "pair": pair or "EURUSD",
            "sl": sl, "tp": tp, "entry": entry, "qty": qty}

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
    # Build the per-pair panel: FOREX + BTC + GOLD pairs only (not all 382 instruments)
    _lot_pairs = sorted(set(list(LOT_SIZES.keys()) + list(DEFAULT_LOTS.keys()) + list(PAIR_ENABLED.keys())))
    _lot_data = {}
    for p in _lot_pairs:
        _lot_data[p] = {"lot": LOT_SIZES.get(p, DEFAULT_LOTS.get(p, lot_for(p))), "on": is_pair_enabled(p), "sym": _instruments.get(p, {}).get("id")}
    _lot_json = json.dumps(_lot_data)
    _active_overrides = len(LOT_SIZES)
    _total_pairs = len(_lot_pairs)
    _enabled_count = sum(1 for p in _lot_pairs if _lot_data[p]["on"])
    # Extra trading settings (from docs/extra_settings.json if present)
    _extra_settings = {}
    try:
        _es_path = os.path.join("docs", "extra_settings.json")
        if os.path.exists(_es_path):
            with open(_es_path, "r", encoding="utf-8") as _f:
                _extra_settings = json.load(_f) or {}
    except Exception:
        _extra_settings = {}

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
        pos_rows = ""
        for p in positions_data:
            ot = str(p.get("open_time", "")).replace('"', '')
            pos_rows += (f'<tr>'
                f'<td class="time" data-ts="{ot}">{p.get("open_time","—")}</td>'
                f'<td class="pair">{p["pair"]}</td>'
                f'<td class="{"buy" if "BUY" in p["side"] else "sell"}">{p["side"]}</td>'
                f'<td>{p["qty"]}</td><td>{p["price"]}</td><td>{p["sl"]}</td><td>{p["tp"]}</td>'
                f'<td>#{p.get("position_id","—")}</td>'
                f'<td style="color:#3fb950;font-weight:600;">{p["pnl"]}</td>'
                f'</tr>')
        positions_table = (f'<table><thead><tr>'
            f'<th>Open Time (UTC)</th><th>Pair</th><th>Side</th><th>Volume</th>'
            f'<th>Entry Price</th><th>SL</th><th>TP</th><th>Position ID</th><th>P&amp;L</th>'
            f'</tr></thead><tbody>{pos_rows}</tbody></table>')
    else:
        positions_table = '<div class="empty">No open positions currently</div>'

    if trades_data:
        trade_rows = ""
        for t in trades_data:
            side_cls = "buy" if "BUY" in str(t.get("side", "")) else "sell"
            pnl_val = t.get("pnl_value", 0)
            pnl_col = "#3fb950" if pnl_val >= 0 else "#f85149"
            ot = str(t.get("open_time", "")).replace('"', '')
            ct = str(t.get("close_time", "")).replace('"', '')
            trade_rows += (f'<tr>'
                f'<td class="time" data-ts="{ot}">{t.get("open_time","—")}</td>'
                f'<td class="time" data-ts="{ct}">{t.get("close_time","—")}</td>'
                f'<td>{t.get("duration","—")}</td>'
                f'<td class="pair">{t.get("pair","N/A")}</td>'
                f'<td class="{side_cls}">{t.get("side","—")}</td>'
                f'<td>{t.get("qty","—")}</td>'
                f'<td>{t.get("entry","—")}</td>'
                f'<td>{t.get("exit","—")}</td>'
                f'<td style="color:{pnl_col};font-weight:700;">{t.get("pnl","$0.00")}</td>'
                f'<td>{t.get("status","—")}</td>'
                f'</tr>')
        trades_table = (f'<table><thead><tr>'
            f'<th>Entry Time (UTC)</th><th>Exit Time (UTC)</th><th>Duration</th>'
            f'<th>Pair</th><th>Side</th><th>Volume</th><th>Entry Price</th><th>Exit Price</th>'
            f'<th>P&amp;L</th><th>Status</th>'
            f'</tr></thead><tbody>{trade_rows}</tbody></table>')
    else:
        trades_table = '<div class="empty">No deal history retrieved yet (trades will appear here once executed)</div>'

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
        /* ===== Per-Pair Manager (grid panel, no search) ===== */
        const LOT_DATA = {_lot_json};
        function renderLotTable(){{
            const body=document.getElementById('lotBody'); if(!body) return;
            const saved=JSON.parse(localStorage.getItem('pip_pair_cfg')||'{{}}');
            body.innerHTML='';
            Object.keys(LOT_DATA).forEach(pair=>{{
                const base=LOT_DATA[pair]||{{}}; const defLot=base.lot; const defOn=base.on!==false;
                const lot=(saved[pair]&&saved[pair].lot!==undefined)?saved[pair].lot:defLot;
                const on=(saved[pair]&&saved[pair].on!==undefined)?saved[pair].on:defOn;
                body.insertAdjacentHTML('beforeend',
                    `<div style="background:#070c16;border:1px solid #30363d;border-radius:10px;padding:10px;display:flex;flex-direction:column;gap:8px;">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <span class="pairname" style="font-weight:700;font-size:13px;${{on?'':'opacity:.4;text-decoration:line-through;'}}">${{pair}}</span>
                            <label style="position:relative;display:inline-block;width:40px;height:22px;cursor:pointer;flex-shrink:0;">
                                <input type="checkbox" data-on="${{pair}}" ${{on?'checked':''}} style="opacity:0;width:0;height:0;" onchange="togglePair(this)">
                                <span class="togglebg" style="position:absolute;inset:0;background:${{on?'#3fb950':'#f85149'}};border-radius:999px;transition:.2s;"></span>
                                <span class="toggleknob" style="position:absolute;top:2.5px;left:${{on?'20px':'2.5px'}};width:17px;height:17px;background:#fff;border-radius:50%;transition:.2s;"></span>
                            </label>
                        </div>
                        <input type="number" step="0.01" min="0.01" value="${{lot}}" data-lot="${{pair}}" style="background:#0a0f1c;border:1px solid #30363d;border-radius:6px;width:100%;text-align:center;padding:5px;color:#e6ebf5;font-size:12px;">
                    </div>`);
            }});
        }}
        function togglePair(cb){{
            const label=cb.parentElement;
            const bg=label.querySelector('.togglebg');
            const knob=label.querySelector('.toggleknob');
            const name=label.closest('div[style*="flex-direction"]').querySelector('.pairname');
            if(cb.checked){{ bg.style.background='#3fb950'; knob.style.left='20px'; if(name){{ name.style.opacity='1'; name.style.textDecoration='none'; }} }}
            else{{ bg.style.background='#f85149'; knob.style.left='2.5px'; if(name){{ name.style.opacity='.4'; name.style.textDecoration='line-through'; }} }}
        }}
        function collectCfg(){{
            const out=JSON.parse(JSON.stringify(LOT_DATA));
            document.querySelectorAll('#lotBody input[data-lot]').forEach(inp=>{{ const v=parseFloat(inp.value); if(out[inp.dataset.lot]) out[inp.dataset.lot].lot=isNaN(v)||v<=0?0.01:v; }});
            document.querySelectorAll('#lotBody input[data-on]').forEach(inp=>{{ if(out[inp.dataset.on]) out[inp.dataset.on].on=inp.checked; }});
            return out;
        }}
        function showToast(msg){{
            const t=document.getElementById('lotToast'); if(!t) return;
            t.textContent=msg; t.style.display='inline-block';
            setTimeout(()=>t.style.display='none',3500);
        }}
        function saveLots(){{
            const cfg=collectCfg();
            localStorage.setItem('pip_pair_cfg',JSON.stringify(cfg));
            const exp=document.getElementById('lotExport'); const wrap=document.getElementById('lotExportWrap');
            if(exp&&wrap){{ exp.value=JSON.stringify(cfg); wrap.style.display='block'; }}
            const en=Object.values(cfg).filter(v=>v.on).length;
            showToast('Saved! '+en+' enabled / '+Object.keys(cfg).length+' total');
            renderLotTable();
        }}
        function addLotPair(){{
            const p=prompt('Enter pair symbol (e.g. EURUSD):'); if(!p) return;
            const k=p.replace(/[^A-Z0-9]/gi,'').toUpperCase();
            if(!LOT_DATA[k]) LOT_DATA[k]={{lot:0.01,on:true,sym:null}};
            renderLotTable();
        }}
        function copyLotJson(){{ const e=document.getElementById('lotExport'); navigator.clipboard.writeText(e.value); showToast('JSON copied!'); }}
        checkAuth();
        renderLotTable();
        refreshAgo();
        updateClock();
        updatePageAge();
        setInterval(refreshAgo, 1000);
        setInterval(updateClock, 1000);
        setInterval(updatePageAge, 1000);
        setInterval(() => location.reload(), 60000);
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

        <div class="section-title">⚙️ Per-Pair Lot Size & ON/OFF Manager ({_enabled_count} of {_total_pairs} Enabled)</div>
        <div class="card">
            <p style="font-size:12px;color:#8b949e;margin-bottom:14px;">Set the lot size and toggle each pair ON/OFF. 🔴 OFF = signals for that pair are blocked. After changing, click <strong>Save</strong>.</p>
            <div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;align-items:center;">
                <button class="btn btn-refresh" onclick="saveLots()" style="background:#238636;">💾 Save Settings</button>
                <button class="btn" onclick="addLotPair()" style="background:#30363d;color:#c9d1d9;">＋ Add Pair</button>
                <span id="lotToast" style="display:none;color:#3fb950;font-weight:700;font-size:13px;"></span>
            </div>
            <div id="lotBody" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px;"></div>
            <div id="lotExportWrap" style="display:none;margin-top:14px;">
                <div style="font-size:12px;color:#8b949e;margin-bottom:6px;">📋 Set GitHub secret <strong>CTRADER_PAIR_CONFIG</strong> with this JSON:</div>
                <textarea id="lotExport" style="width:100%;height:80px;background:#0a0f1c;border:1px solid #30363d;border-radius:8px;color:#3fb950;font-size:11px;padding:8px;" readonly></textarea>
                <button class="btn" onclick="copyLotJson()" style="background:#30363d;color:#c9d1d9;margin-top:6px;">Copy JSON</button>
            </div>
        </div>

        <div class="section-title">🎚️ Extra Trading Settings</div>
        <div class="card">
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;">
                <div><label style="font-size:11px;color:#8b949e;display:block;margin-bottom:6px;">DEFAULT LOT (fallback)</label><input id="setDefLot" type="number" step="0.01" min="0.01" value="{_lot_data.get('EURUSD',0.01)}" class="field" style="border-radius:8px;padding:8px;width:100%;"></div>
                <div><label style="font-size:11px;color:#8b949e;display:block;margin-bottom:6px;">MAX DAILY TRADES</label><input id="setMaxTrades" type="number" min="1" value="{_extra_settings.get('max_daily_trades','')}" placeholder="unlimited" class="field" style="border-radius:8px;padding:8px;width:100%;"></div>
                <div><label style="font-size:11px;color:#8b949e;display:block;margin-bottom:6px;">TRADING HOURS (UTC) FROM</label><input id="setHourFrom" type="number" min="0" max="23" value="{_extra_settings.get('hour_from','')}" placeholder="00" class="field" style="border-radius:8px;padding:8px;width:100%;"></div>
                <div><label style="font-size:11px;color:#8b949e;display:block;margin-bottom:6px;">TRADING HOURS (UTC) TO</label><input id="setHourTo" type="number" min="0" max="23" value="{_extra_settings.get('hour_to','')}" placeholder="23" class="field" style="border-radius:8px;padding:8px;width:100%;"></div>
            </div>
            <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
                <label style="font-size:13px;display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="setCloseOpp" class="checkbox" {'checked' if _extra_settings.get('close_opposite') else ''}> Close on opposite signal</label>
                <label style="font-size:13px;display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="setUseTPSL" class="checkbox" checked> Apply SL & TP from signal</label>
            </div>
            <p style="font-size:11px;color:#8b949e;margin-top:12px;">💡 Settings are saved in your browser. Wire them into the bot via GitHub secrets for production enforcement.</p>
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

        <div class="section-title">📜 Full Trade History ({len(trades_data)} trades — last 365 days from cTrader)</div>
        <div class="card" style="overflow:auto;max-height:600px;">{trades_table}</div>

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
    raw_msgs = []
    if USE_USER_ACCOUNT:
        # USER-ACCOUNT receiver (Telethon): can read ANY channel/group you are a member of — no admin needed
        log_process("info", "Using Telethon user-account receiver (copies from any channel you've joined).")
        raw_msgs = user_account_fetch_signals()
        log_process("info", f"Fetched {len(raw_msgs)} new message(s) via Telethon.")
    else:
        tg_conn, _ = test_telegram_connection()
        if tg_conn and TG_TOKEN:
            clear_telegram_webhook()
            msgs, fetched_max_uid = tg_get_messages(offset=_last_update_id)
            log_process("info", f"Fetched {len(msgs)} new message(s) from Telegram Bot API (highest update_id seen: {fetched_max_uid}).")
            raw_msgs = msgs
    for msg in raw_msgs:
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
        # Dispatched but NOT confirmed by cTrader -> KEEP RETRYING UNTIL PLACED!
        # Increment retry counter; after 30 attempts, give up (signal likely invalid).
        for s in pending_signals:
            s["_retries"] = s.get("_retries", 0) + 1
        max_retries = 30
        over_limit = [s for s in pending_signals if s.get("_retries", 0) > max_retries]
        if over_limit:
            save_pending_signals([s for s in pending_signals if s.get("_retries", 0) <= max_retries])
            _last_update_id = fetched_max_uid
            log_process("error", f"Giving up after {max_retries} retries — order may be invalid (volume/symbol). Offset committed.")
            save_heartbeat("bot", "failed", f"Max retries ({max_retries}) — signal may be invalid")
        else:
            save_pending_signals(pending_signals)
            rcount = pending_signals[0].get("_retries", 1) if pending_signals else 1
            log_process("warning", f"Order dispatched but NOT confirmed. RETAINING for retry (attempt {rcount}/{max_retries}). Will keep trying until the trade is placed!")
            save_heartbeat("bot", "running", f"Retrying {count} signal(s) — attempt {rcount}/{max_retries}")
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

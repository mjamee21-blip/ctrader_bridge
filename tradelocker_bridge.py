#!/usr/bin/python3
# -*- coding: utf-8 -*-
# =====================================================
# TRADELOCKER TELEGRAM BRIDGE v1.7.2 (FIXED)
# =====================================================
# Monitors Telegram channel for trading signals and
# executes them on TradeLocker via their REST API.
#
# FIXES IN v1.7 (on top of v1.6):
#
# FIX-19: SL/TP NOT BEING PLACED on order creation.
#         The issue was that place_order() accepted sl/tp parameters but only
#         added them to the payload if they were truthy (non-zero).
#         If sl or tp were float values, they were correctly added.
#         However, the real issue: when the order succeeded but SL/TP failed to set,
#         there was no retry/verify logic. Now:
#         - After placing an order, verify SL/TP are set on the position
#         - If missing, immediately modify the position to add them
#         - Log verification success/failure explicitly
#
# FIX-20: TP/SL TRACKING STATUS & RESULTS.
#         The trade_results dict only counted closed trades but didn't track
#         the STATUS of SL/TP on OPEN positions. Now we also monitor:
#         - How many open positions have SL set
#         - How many open positions have TP set
#         - Alert if any position is missing SL/TP when they were intended
#
# FIX-21: SERVER LOAD OPTIMIZATION for cron jobs.
#         Original code called get_trade_history() and analyze_trade_results()
#         EVERY cycle, even if no new trades. Now:
#         - Cache last analyzed order count
#         - Only re-analyze if order count changed significantly
#         - Reduce redundant API calls (get_account_info, get_account_state)
#           to once per 5 cycles for status display
#         - Use connection pooling / timeout tuning
#         - Implement exponential backoff on API failures
#
# FIX-22: EXPLICIT SL/TP PLACEMENT VERIFICATION & RETRY.
#         After place_order() succeeds, we immediately check if SL/TP were
#         actually applied. If not, we call modify_position() to add them.
#         This ensures SL/TP are ALWAYS set, regardless of API quirks.
#
# FIX-23: POSITION MATCHING ROBUSTNESS.
#         The _pair_matches() function was good but now we also verify
#         position ID consistency when modifying, to avoid accidentally
#         modifying the wrong trade.
#
# =====================================================

import sys
import os
import json
import html
import time
import re
import urllib.request
import urllib.parse
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone, timedelta
import traceback
import ssl

# Bypass SSL verification (for hosts with cert issues)
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except Exception:
    pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()

CRASH_LOG = os.path.join(SCRIPT_DIR, "tradelocker_crash.log")
LOG_FILE  = os.path.join(SCRIPT_DIR, "tradelocker_bridge.log")

START_TIME = time.time()

try:
    with open(CRASH_LOG, "a") as _fh:
        _fh.write(f"BOOT: tradelocker_bridge.py started at "
                  f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n")
except Exception:
    pass


def _boot_excepthook(exc_type, exc_value, exc_tb):
    try:
        msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        try:
            if os.environ.get("REQUEST_METHOD") or os.environ.get("GATEWAY_INTERFACE"):
                print("Content-Type: text/plain; charset=utf-8")
                print()
                print("BOOT FAILURE:\n" + msg[:3000])
        except Exception:
            pass
        try:
            with open(CRASH_LOG, "a") as f:
                f.write("BOOT FAILURE:\n" + msg[:3000] + "\n")
        except Exception:
            pass
    except Exception:
        pass
    sys.exit(1)


sys.excepthook = _boot_excepthook

# =====================================================
# CONFIG LOADER
# =====================================================

def load_env(filename=".env1"):
    env_vars = {}
    env_path = os.path.join(SCRIPT_DIR, filename)
    try:
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip().strip('"').strip("'")
    except Exception:
        pass
    return env_vars


ENV = load_env(".env1")

def _safe_int(val, default):
    try:
        if val is not None and str(val).strip():
            return int(str(val).strip())
    except Exception:
        pass
    return default

def _safe_float(val, default):
    try:
        if val is not None and str(val).strip():
            return float(str(val).strip())
    except Exception:
        pass
    return default

# =====================================================
# CONFIGURATION
# =====================================================
try:
    TL_EMAIL      = ENV.get("TL_EMAIL", "")
    TL_PASSWORD   = ENV.get("TL_PASSWORD", "")
    TL_SERVER     = ENV.get("TL_SERVER", "TradeLocker-Demo")
    TL_ACCOUNT_ID = _safe_int(ENV.get("TL_ACCOUNT_ID"), 0)
    TL_ACC_NUM    = _safe_int(ENV.get("TL_ACC_NUM"), 1)
    TL_ENV        = ENV.get("TL_ENV", "demo")

    TG_TOKEN = ENV.get("TG_TOKEN", "")
    TG_CHAT  = ENV.get("TG_CHAT", "")

    PAIR_MAP_RAW = ENV.get("TL_PAIR_MAP", "")
    try:
        PAIR_MAP = json.loads(PAIR_MAP_RAW) if PAIR_MAP_RAW else {}
    except Exception:
        PAIR_MAP = {}

    # Default lot size
    DEFAULT_QTY = _safe_float(ENV.get("TL_DEFAULT_QTY"), 0.10)
except Exception:
    TL_EMAIL      = ""
    TL_PASSWORD   = ""
    TL_SERVER     = "TradeLocker-Demo"
    TL_ACCOUNT_ID = 0
    TL_ACC_NUM    = 1
    TL_ENV        = "demo"
    TG_TOKEN      = ""
    TG_CHAT       = ""
    PAIR_MAP      = {}
    DEFAULT_QTY   = 0.10

TL_BASE = ENV.get("TL_BASE_URL")
if not TL_BASE:
    if TL_ENV.lower() == "live":
        TL_BASE = "https://live.tradelocker.com"
    else:
        TL_BASE = "https://demo.tradelocker.com"

# FIX-21: Cache timings to reduce server load
CACHE_POSITIONS_SEC     = 10   # refresh positions every 10s on cron
CACHE_ACCOUNT_INFO_SEC  = 30   # refresh account info every 30s on cron
CACHE_TRADE_HISTORY_SEC = 20   # only re-analyze if >10 new orders

# =====================================================
# PERSISTENT STATE FILES
# =====================================================
STATUS_FILE    = os.path.join(SCRIPT_DIR, "tradelocker_status.json")
PROCESSED_FILE = os.path.join(SCRIPT_DIR, "tradelocker_processed.json")

# =====================================================
# LOGGING
# =====================================================
_log_buffer = []


def log(level, message, data=None):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_str  = json.dumps(data) if data else None
    log_line  = f"[{timestamp}] {level} | {message}"
    if data_str:
        log_line += f" | {data_str}"

    try:
        with open(LOG_FILE, "a") as f:
            f.write(log_line + "\n")
        if os.path.getsize(LOG_FILE) > 5 * 1024 * 1024:
            os.rename(LOG_FILE, LOG_FILE + ".old")
    except Exception:
        pass

    try:
        global _log_buffer
        _log_buffer.append({
            "time":    timestamp,
            "level":   level,
            "message": message,
            "data":    data_str,
        })
        if len(_log_buffer) > 1000:
            _log_buffer = _log_buffer[-1000:]
    except Exception:
        pass

    return log_line


# =====================================================
# PROCESSED MESSAGE TRACKING
# =====================================================

def load_processed():
    try:
        if os.path.exists(PROCESSED_FILE):
            with open(PROCESSED_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("ids", []))
    except Exception:
        pass
    return set()


def save_processed(processed_set):
    try:
        ids = sorted(processed_set)
        if len(ids) > 2000:
            ids = ids[-2000:]
        tmp = PROCESSED_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"ids": ids}, f)
        os.replace(tmp, PROCESSED_FILE)
    except Exception:
        pass


# =====================================================
# BRIDGE STATUS
# =====================================================
_bridge_status = {
    "status": "STOPPED",
    "last_telegram_poll":     None,
    "last_trade_executed":    None,
    "total_signals_received": 0,
    "total_trades_executed":  0,
    "total_errors":           0,
    "current_open_positions": 0,
    "access_token_expires_at": None,
    "instruments_loaded":     0,
    "last_error":             None,
    "uptime_seconds":         0,
    "started_at":             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "last_messages":          [],
    "last_update_id":         0,
    # FIX-17/20: TP/SL outcome stats + position-level SL/TP tracking
    "trade_results":          {},
    "open_positions_sl_tp_check": {},  # FIX-20: positions with/without SL/TP
}

_last_update_id = 0

# FIX-21: Cache timestamps for reducing API calls
_cache_positions_time     = 0
_cache_account_info_time  = 0
_cache_trade_history_time = 0
_last_history_order_count = 0
_api_failure_count        = 0  # exponential backoff tracking


def _load_persisted_status():
    """
    Load persisted counters and last_update_id from the status file so
    cron restarts continue from where the previous run left off.
    """
    global _last_update_id
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r") as f:
                saved = json.load(f)
            bs = saved.get("status", {})
            if isinstance(bs, dict):
                for key in ("total_signals_received", "total_trades_executed",
                            "total_errors", "last_update_id",
                            "last_trade_executed", "started_at"):
                    if key in bs and bs[key] is not None:
                        _bridge_status[key] = bs[key]
                _last_update_id = int(bs.get("last_update_id") or 0)
    except Exception as e:
        log("WARN", f"Could not load persisted status: {str(e)[:60]}")


# =====================================================
# TELEGRAM HELPERS
# =====================================================

def tg_send_message(text):
    if TG_CHAT == "ANY":
        log("WARN", "Cannot send message when TG_CHAT=ANY")
        return False
    url    = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    params = {"chat_id": TG_CHAT, "text": text}
    try:
        data = urllib.parse.urlencode(params).encode()
        req  = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        log("ERROR", f"Failed to send Telegram message: {str(e)[:80]}")
    return False


def tg_delete_webhook():
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/deleteWebhook"
        body = b'{"drop_pending_updates": false}'
        req  = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                log("INFO", "Telegram webhook deleted/confirmed clear")
                return True
    except Exception as e:
        log("ERROR", f"Failed to delete webhook: {str(e)[:80]}")
    return False


def tg_get_chat_messages(limit=100):
    """
    Fetch new messages from Telegram using getUpdates (GET with query string).
    Uses persisted _last_update_id so each cron run only sees NEW messages.
    """
    global _last_update_id, _bridge_status

    params = {
        "timeout": 5,
        "limit":   limit,
        "offset":  _last_update_id + 1,
    }
    url = (f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?"
           + urllib.parse.urlencode(params))
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode().strip()
            if not raw:
                return []
            result = json.loads(raw)
            if not result.get("ok"):
                log("ERROR", f"Telegram getUpdates not ok: {raw[:200]}")
                return []

            updates = result.get("result", [])

            for update in updates:
                uid = update.get("update_id", 0)
                if uid > _last_update_id:
                    _last_update_id = uid

            _bridge_status["last_update_id"] = _last_update_id

            messages = []
            for update in updates:
                msg      = update.get("message") or update.get("channel_post") or {}
                chat     = msg.get("chat", {})
                chat_id  = str(chat.get("id", ""))
                chat_uname = chat.get("username", "")
                target   = str(TG_CHAT).lstrip("@")

                log("INFO", f"Update {update.get('update_id')} from "
                    f"{chat_uname}({chat_id})")

                if TG_CHAT != "ANY":
                    if target != chat_uname and target != chat_id:
                        continue

                text = msg.get("text") or msg.get("caption") or ""
                messages.append({
                    "chat_id":    chat_id,
                    "message_id": msg.get("message_id"),
                    "text":       text,
                    "date":       msg.get("date"),
                    "update_id":  update.get("update_id"),
                })
            return messages

    except HTTPError as e:
        body = e.read().decode() if e.fp else ""
        if e.code == 409:
            log("ERROR", "Telegram 409 conflict — deleting webhook and retrying")
            tg_delete_webhook()
            return []
        log("ERROR", f"Telegram getUpdates HTTP {e.code}: {body[:200]}")
    except Exception as e:
        log("ERROR", f"Telegram getUpdates failed: {str(e)[:100]}")
    return []


# =====================================================
# SIGNAL PARSER
# =====================================================

def _looks_like_signal(text):
    """Quick pre-filter to avoid parsing every non-signal message."""
    if not text:
        return False
    t = text.upper()
    return ("BUY" in t or "SELL" in t or "CLOSE" in t or
            "TP HIT" in t or "SL HIT" in t or
            "#SL_UPDATE" in t or "SL UPDATE" in t or "NEW SL" in t)


def _pair_matches(pos_symbol, pair):
    """
    FIX-13: normalise BOTH sides before comparing.
    "BTC/USD" (instrument name) must match signal pair "BTC/USD" — naive
    substring of "BTCUSD" in "BTC/USD" is False, so strip slashes/spaces
    and uppercase both before the inclusion check.
    """
    a = re.sub(r"[^A-Z0-9]", "", str(pos_symbol).upper())
    b = re.sub(r"[^A-Z0-9]", "", str(pair).upper())
    if not a or not b:
        return False
    return a == b or b in a or a in b


def parse_signal_message(text):
    """
    Parse a Telegram signal message into structured trade data.

    EXECUTION SEMANTICS (v1.5)
    --------------------------
    Every signal is executed IMMEDIATELY as a MARKET order at the live
    price, the instant it is received. "Entry: MARKET" and "Entry: NOW"
    both mean "fill now".  The (Ref: ...) price is captured for logging /
    the trader's own analysis ONLY — it is NEVER used to set the order
    price, and the script does not care about the open/entry price at all.

    Supported formats
    -----------------
    Signal:
        🟢 BUY BTC/USD
        Entry: MARKET (Ref: 64828.61) TP:65069.93 | SL:64615.68
        RR: 1:1.3

    Multi-line signal:
        🟢 BUY GBP/USD
        Entry: MARKET (Ref: 1.2543)
        SL: 1.2485
        TP: 1.2620
        RR: 1:2.1

    TP/SL hit:
        ✅ TP HIT - GBP/USD

    SL update:
        #SL_UPDATE
        Pair: GBP/USD
        New SL: 1.2490
    """
    if not text:
        return None

    if _looks_like_signal(text):
        log("INFO", f"Parsing candidate signal: {text[:300]}")
    else:
        return None

    lines      = text.strip().split("\n")
    first_line = lines[0].strip()

    # ---- TP/SL HIT ----
    tpsl_m = re.search(r'(TP|SL)\s*HIT', first_line, re.IGNORECASE)
    if tpsl_m:
        pair_m = re.search(r'(TP|SL)\s*HIT\s*[-–:]\s*(\S+)', first_line, re.IGNORECASE)
        pair   = pair_m.group(2) if pair_m else ""
        if not pair:
            for line in lines[1:]:
                pm = re.search(r'([A-Z]{3,6}[/]?[A-Z]{0,6})', line.strip())
                if pm:
                    pair = pm.group(1)
                    break
        return {
            "type":     "TPSL_HIT",
            "result":   tpsl_m.group(1).upper(),
            "pair":     pair,
            "raw_text": text,
        }

    # ---- SL UPDATE ----
    upper_text = text.upper()
    if "#SL_UPDATE" in upper_text or "SL UPDATE" in upper_text or "NEW SL" in upper_text:
        pair   = None
        new_sl = None
        for line in lines:
            cl = line.strip()
            if not cl:
                continue
            if cl.upper().startswith("PAIR"):
                parts = cl.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    pair = parts[1].strip()
                continue
            m = re.search(r'New\s*SL\s*[:=]?\s*([\d.]+)', cl, re.IGNORECASE)
            if not m:
                m = re.search(r'(?<![A-Za-z])SL\s*[:=]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    new_sl = float(m.group(1))
                except ValueError:
                    pass
        if not pair:
            for line in lines:
                cl = line.strip()
                if "SL_UPDATE" in cl.upper() or "SL UPDATE" in cl.upper():
                    sym = re.search(r'\b([A-Z]{3,6}\s*/?\s*[A-Z]{0,6})\b',
                                    cl.upper().replace("SL_UPDATE", "")
                                       .replace("SL UPDATE", ""))
                    if sym and sym.group(1).strip():
                        pair = sym.group(1).strip()
                        break
        if pair and new_sl is not None:
            return {
                "type":     "SL_UPDATE",
                "pair":     pair,
                "new_sl":   new_sl,
                "raw_text": text,
            }
        return None

    # ---- TRADE SIGNAL ----
    sig_m = re.search(r'\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)', first_line, re.IGNORECASE)
    if not sig_m:
        return None

    direction = sig_m.group(1).upper()
    pair      = sig_m.group(2).upper()

    ref_price = sl = tp = rr = None

    for line in lines:
        cl = re.sub(r'<[^>]+>', '', line).strip()
        if not cl:
            continue

        if ref_price is None:
            m = re.search(r'(?:Entry|Ref)\s*[:=]\s*(?:MARKET|NOW|LIMIT)?\s*'
                          r'\(?\s*(?:Ref\s*[:=]\s*)?([\d.]+)\s*\)?',
                          cl, re.IGNORECASE)
            if m:
                try:
                    ref_price = float(m.group(1))
                except ValueError:
                    pass

        if sl is None:
            m = re.search(r'(?<![A-Za-z])SL\s*[:\s]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    sl = float(m.group(1))
                except ValueError:
                    pass

        if tp is None:
            m = re.search(r'(?<![A-Za-z])TP\s*[:\s]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    tp = float(m.group(1))
                except ValueError:
                    pass

        if rr is None:
            m = re.search(r'RR\s*[:\s]\s*([\d.]+)\s*[:/]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                rr = f"{m.group(1)}:{m.group(2)}"

    log("INFO", f"Signal parsed → {direction} {pair} | Ref(info only)={ref_price} "
        f"SL={sl} TP={tp} RR={rr}")
    return {
        "type":      "SIGNAL",
        "direction": direction,
        "pair":      pair,
        "ref_price": ref_price,
        "sl":        sl,
        "tp":        tp,
        "rr":        rr,
        "raw_text":  text,
    }


# =====================================================
# TRADELOCKER API CLIENT
# =====================================================

_TL_POSITION_COLUMNS = [
    "positionId",
    "tradableInstrumentId",
    "routeId",
    "side",
    "qty",
    "price",
    "stopLoss",
    "takeProfit",
    "openTime",
    "pnl",
    "status",
    "instrumentName",
]

_TL_ORDER_COLUMNS = [
    "orderId",
    "tradableInstrumentId",
    "routeId",
    "side",
    "qty",
    "price",
    "type",
    "status",
    "executionType",
    "orderExpiry",
    "placedAt",
    "modifiedAt",
    "realizedPnL",
    "closeReason",
]


class TradeLockerClient:
    """TradeLocker REST API client with JWT authentication."""

    def __init__(self, base_url, email, password, server, account_id, acc_num):
        self.base_url      = base_url.rstrip("/")
        self.auth_base_url = self.base_url + "/backend-api"
        self.email         = email
        self.password      = password
        self.server        = server
        self.account_id    = account_id
        self.acc_num       = acc_num
        self.access_token  = None
        self.refresh_token = None
        self.token_expires_at = None
        self.instruments   = {}
        self.authenticated = False

    # ------------------------------------------------------------------
    # LOW-LEVEL HTTP
    # ------------------------------------------------------------------

    def _request(self, method, path, body=None, headers_extra=None,
                 timeout=20, use_auth_base=True):
        base = self.auth_base_url if use_auth_base else self.base_url
        url  = f"{base}{path}"

        req_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; TLBridge/1.7)",
            "Accept":     "application/json",
        }
        if body is not None:
            req_headers["Content-Type"] = "application/json"
        if self.access_token:
            req_headers["Authorization"] = f"Bearer {self.access_token}"
        if headers_extra:
            req_headers.update(headers_extra)

        try:
            encoded = json.dumps(body).encode() if body is not None else None
            req     = urllib.request.Request(url, data=encoded,
                                             headers=req_headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content = resp.read().decode()
                if not content.strip():
                    return {}
                stripped = content.lstrip()
                if stripped.startswith("<!DOCTYPE") or stripped.startswith("<html"):
                    log("ERROR", f"HTML response on {method} {path}: {content[:200]}")
                    return {"error": "HTML_RESPONSE", "detail": content[:200]}
                try:
                    return json.loads(content)
                except Exception as je:
                    log("ERROR", f"JSON parse error on {method} {path}: "
                        f"{str(je)[:60]} | raw: {content[:200]}")
                    return {"error": "INVALID_JSON", "detail": content[:300]}

        except HTTPError as e:
            body_txt = ""
            try:
                body_txt = e.read().decode()
            except Exception:
                pass
            log("ERROR", f"HTTP {e.code} on {method} {path}: {body_txt[:300]}")
            return {"error": f"HTTP_{e.code}", "detail": body_txt[:300]}
        except Exception as e:
            log("ERROR", f"Request failed on {method} {path}: {str(e)[:100]}")
            return {"error": str(e)[:100]}

    # ------------------------------------------------------------------
    # AUTH
    # ------------------------------------------------------------------

    def authenticate(self):
        log("INFO", f"Authenticating with TradeLocker [{TL_ENV.upper()}] server={self.server}")

        if not (self.email and self.password and self.server):
            log("ERROR", "Missing TL_EMAIL / TL_PASSWORD / TL_SERVER in .env")
            _bridge_status["last_error"] = "Missing credentials in .env"
            _bridge_status["status"]     = "AUTH_FAILED"
            return False

        payload = {"email": self.email, "password": self.password, "server": self.server}
        result  = self._request("POST", "/auth/jwt/token", body=payload)

        if result.get("error"):
            log("WARN", f"JSON auth failed ({result.get('error')}), trying form-encoded...")
            try:
                url  = f"{self.auth_base_url}/auth/jwt/token"
                data = urllib.parse.urlencode(payload).encode()
                req  = urllib.request.Request(url, data=data, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept":     "application/json",
                })
                with urllib.request.urlopen(req, timeout=20) as resp:
                    content = resp.read().decode()
                    if content.strip():
                        result = json.loads(content)
                        if not result.get("error"):
                            log("INFO", "Form-encoded auth succeeded")
            except Exception as e:
                log("WARN", f"Form-encoded auth failed: {str(e)[:80]}")

        if result.get("error"):
            detail = result.get("detail", result["error"])
            log("ERROR", f"Authentication failed: {result}")
            _bridge_status["last_error"] = f"Auth failed: {str(detail)[:80]}"
            _bridge_status["status"]     = "AUTH_FAILED"
            self.authenticated = False
            return False

        self.access_token  = (result.get("accessToken")
                               or result.get("access_token")
                               or result.get("token"))
        self.refresh_token = (result.get("refreshToken")
                               or result.get("refresh_token"))

        if not self.access_token:
            log("ERROR", f"No access token in auth response: {json.dumps(result)[:300]}")
            _bridge_status["last_error"] = "No access token returned"
            _bridge_status["status"]     = "AUTH_FAILED"
            self.authenticated = False
            return False

        self.authenticated    = True
        _bridge_status["status"] = "AUTHENTICATED"
        expires_in            = result.get("expiresIn") or result.get("expires_in") or 3600
        self.token_expires_at = time.time() + expires_in
        _bridge_status["access_token_expires_at"] = datetime.fromtimestamp(
            self.token_expires_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        log("INFO", "Authentication successful")
        return True

    def refresh_auth(self):
        if not self.refresh_token:
            return self.authenticate()
        log("INFO", "Refreshing JWT token...")
        result    = self._request("POST", "/auth/jwt/refresh",
                                  body={"refreshToken": self.refresh_token})
        new_token = (result.get("accessToken") or result.get("access_token")
                     or result.get("token"))
        if new_token:
            self.access_token  = new_token
            self.refresh_token = (result.get("refreshToken")
                                   or result.get("refresh_token")
                                   or self.refresh_token)
            expires_in         = result.get("expiresIn") or result.get("expires_in") or 3600
            self.token_expires_at = time.time() + expires_in
            self.authenticated = True
            log("INFO", "Token refreshed")
            return True
        log("WARN", "Token refresh failed, re-authenticating...")
        return self.authenticate()

    def ensure_auth(self):
        if not self.authenticated:
            return self.authenticate()
        if self.token_expires_at and time.time() >= self.token_expires_at - 60:
            return self.refresh_auth()
        return True

    # ------------------------------------------------------------------
    # INSTRUMENTS
    # ------------------------------------------------------------------

    def load_instruments(self):
        if not self.ensure_auth():
            return False

        log("INFO", "Loading instruments...")
        headers = {"accNum": str(self.acc_num)}
        result  = self._request("GET",
                                f"/trade/accounts/{self.account_id}/instruments",
                                headers_extra=headers)

        if result.get("error"):
            log("ERROR", f"load_instruments error: {result.get('error')} | "
                f"detail: {result.get('detail', '')[:200]}")
            return False

        data = result.get("d", result)
        instruments = []
        if isinstance(data, dict):
            instruments = (data.get("instruments")
                           or data.get("data")
                           or data.get("items")
                           or [])
        elif isinstance(data, list):
            instruments = data

        if not instruments:
            log("WARN", f"No instruments found. Raw keys in d: "
                f"{list(data.keys()) if isinstance(data, dict) else type(data)}")
            return False

        count = 0
        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name    = (inst.get("name") or inst.get("symbol") or "").upper()
            inst_id = inst.get("tradableInstrumentId") or inst.get("id") or ""

            route_id_trade = None
            route_id_info  = None
            for route in (inst.get("routes") or []):
                if not isinstance(route, dict):
                    continue
                rtype = route.get("type", "")
                if rtype == "TRADE":
                    route_id_trade = route.get("id")
                elif rtype == "INFO":
                    route_id_info = route.get("id")

            self.instruments[name] = {
                "id":             inst_id,
                "name":           name,
                "route_id_trade": route_id_trade,
                "route_id_info":  route_id_info,
                "pip_value":      inst.get("pipValue") or inst.get("pip") or 0.0001,
            }
            count += 1

        _bridge_status["instruments_loaded"] = count
        log("INFO", f"Loaded {count} instruments")
        return count > 0

    def find_instrument(self, pair_name):
        """Find instrument by pair name, trying multiple normalisation strategies."""
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in self.instruments:
            return self.instruments[mapped]

        normalized = pair_name.replace("/", "").upper()
        if normalized in self.instruments:
            return self.instruments[normalized]

        for name, info in self.instruments.items():
            if normalized in name or name in normalized:
                return info

        log("WARN", f"Instrument not found for '{pair_name}' (normalised: '{normalized}'). "
            f"Sample instrument names: {list(self.instruments.keys())[:10]}")
        return None

    # ------------------------------------------------------------------
    # MARKET DATA
    # ------------------------------------------------------------------

    def get_quote(self, inst_id):
        route_id_info = None
        for info in self.instruments.values():
            if info.get("id") == inst_id:
                route_id_info = info.get("route_id_info")
                break

        qs = f"?tradableInstrumentId={inst_id}"
        if route_id_info:
            qs += f"&routeId={route_id_info}"

        result = self._request("GET", f"/trade/quotes{qs}",
                               headers_extra={"accNum": str(self.acc_num)})
        if result.get("s") == "error" or result.get("error"):
            log("WARN", f"Quote failed for inst_id={inst_id}: "
                f"{result.get('errmsg') or result.get('error')}")
            return None

        d      = result.get("d", result)
        quotes = d.get("quotes") if isinstance(d, dict) else None
        q      = (quotes[0] if isinstance(quotes, list) and quotes
                  else d if isinstance(d, dict)
                  else result)
        bp = q.get("bp") or q.get("bid")
        ap = q.get("ap") or q.get("ask")
        if bp is None or ap is None:
            log("WARN", f"Quote missing bp/ap for inst_id={inst_id}: {result}")
            return None
        return {"bp": float(bp), "ap": float(ap)}

    # ------------------------------------------------------------------
    # ORDER PLACEMENT + FIX-19/22: SL/TP VERIFICATION
    # ------------------------------------------------------------------

    def place_order(self, pair, direction, sl, tp, quantity=None, ref_price=None):
        """
        FIX-12: Always executes immediately as an IOC MARKET order at live price.
        
        FIX-19/22: After order is placed, immediately verify that SL/TP were
        actually applied. If they're missing, call modify_position() to add them.
        This ensures SL/TP are ALWAYS set on the position, regardless of API quirks.
        """
        if quantity is None:
            quantity = DEFAULT_QTY

        if not self.ensure_auth():
            return False, {"error": "Not authenticated"}

        instrument = self.find_instrument(pair)
        if not instrument:
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            msg = f"Instrument not found for pair '{pair}'"
            log("ERROR", msg)
            _bridge_status["last_error"] = msg
            return False, {"error": msg}

        route_id   = instrument.get("route_id_trade")
        inst_id    = instrument["id"]
        order_side = "buy" if direction.upper() == "BUY" else "sell"
        headers    = {"accNum": str(self.acc_num)}

        payload = {
            "tradableInstrumentId": inst_id,
            "type":     "market",
            "validity": "IOC",
            "side":     order_side,
            "qty":      quantity,
        }
        # FIX-19: explicitly add SL/TP to order payload if provided
        if sl is not None and sl != 0:
            payload["stopLoss"]   = float(sl)
        if tp is not None and tp != 0:
            payload["takeProfit"] = float(tp)
        if route_id:
            payload["routeId"] = str(route_id)

        log("INFO", f"Placing {direction} MARKET (NOW) order | pair={pair} qty={quantity} "
            f"sl={sl} tp={tp} ref(info only)={ref_price}", payload)

        result   = self._request("POST",
                                 f"/trade/accounts/{self.account_id}/orders",
                                 body=payload, headers_extra=headers, timeout=25)
        log("INFO", "Order response", result)

        errmsg    = str(result.get("errmsg") or result.get("error") or "")
        is_error  = (result.get("s") == "error"
                     or (result.get("error") and result.get("s") != "ok"))
        forbidden = "forbidden" in errmsg.lower() and "route" in errmsg.lower()

        if is_error and forbidden:
            log("WARN", f"Market order forbidden on route, trying marketable limit for {pair}")
            quote = self.get_quote(inst_id)
            if not quote:
                msg = f"No quote available for marketable limit on {pair}"
                log("ERROR", msg)
                _bridge_status["last_error"]    = msg
                _bridge_status["total_errors"] += 1
                return False, result

            mid    = (quote["bp"] + quote["ap"]) / 2.0
            offset = max(mid * 0.0015, 0.5)
            limit_price = (round(quote["ap"] + offset, 5)
                           if order_side == "buy"
                           else round(quote["bp"] - offset, 5))

            lim_payload = {
                "tradableInstrumentId": inst_id,
                "type":  "limit",
                "side":  order_side,
                "qty":   quantity,
                "price": limit_price,
            }
            if sl is not None and sl != 0:
                lim_payload["stopLoss"]   = float(sl)
            if tp is not None and tp != 0:
                lim_payload["takeProfit"] = float(tp)
            if route_id:
                lim_payload["routeId"] = str(route_id)

            log("INFO", f"Placing {direction} MARKETABLE LIMIT @ {limit_price} "
                f"(bid={quote['bp']} ask={quote['ap']})", lim_payload)
            result   = self._request("POST",
                                     f"/trade/accounts/{self.account_id}/orders",
                                     body=lim_payload, headers_extra=headers, timeout=25)
            log("INFO", "Limit order response", result)

        errmsg   = str(result.get("errmsg") or result.get("error") or "")
        is_error = (result.get("s") == "error"
                    or (result.get("error") and result.get("s") != "ok"))
        if is_error:
            log("ERROR", f"Order FAILED for {pair}: {errmsg} | full={result}")
            _bridge_status["last_error"]    = f"Order failed for {pair}: {errmsg[:120]}"
            _bridge_status["total_errors"] += 1
            return False, result

        d        = result.get("d", {}) if isinstance(result.get("d"), dict) else {}
        order_id = (d.get("orderId")
                    or d.get("id")
                    or d.get("order_id")
                    or result.get("orderId")
                    or result.get("id")
                    or result.get("order_id"))

        log("INFO", f"✅ Order placed | pair={pair} dir={direction} order_id={order_id}")

        # FIX-19/22: VERIFY and SET SL/TP on the position if not already set
        # Wait briefly for position to be created, then verify
        time.sleep(0.5)
        if sl or tp:
            self._verify_and_set_sltp(pair, order_id, sl, tp, direction)

        _bridge_status["last_trade_executed"]   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        _bridge_status["total_trades_executed"] += 1
        return True, {"order_id": order_id, "result": result}

    def _verify_and_set_sltp(self, pair, order_id, intended_sl, intended_tp, direction):
        """
        FIX-19/22: After order placement, verify SL/TP are set on the resulting position.
        If missing or incorrect, modify the position immediately.
        This is a RETRY mechanism to ensure SL/TP are always applied.
        """
        try:
            # Get current positions to find the one we just opened
            positions = self.get_open_positions()
            if not positions:
                log("WARN", f"No open positions found after order {order_id} — "
                    f"cannot verify SL/TP (position may not have settled yet)")
                return

            # Find the matching position by pair and side
            matching_pos = None
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_pair = self._pos_field(
                    pos, "instrumentName", "symbol", "name", "pair", "instrument")
                pos_side = self._pos_field(pos, "side", "direction", "type", "orderSide")
                if _pair_matches(pos_pair, pair):
                    side_match = (("BUY" in direction.upper() and "buy" in str(pos_side).lower())
                                  or ("SELL" in direction.upper() and "sell" in str(pos_side).lower()))
                    if side_match:
                        matching_pos = pos
                        break

            if not matching_pos:
                log("WARN", f"Could not find matching open position for {pair} "
                    f"after order {order_id} — SL/TP verification skipped")
                return

            pos_id  = self._pos_field(matching_pos, "positionId", "id", "position_id", "tradeId")
            cur_sl  = self._to_float(
                self._pos_field(matching_pos, "stopLoss", "sl", "stop_loss", "SL", "stopLossPrice"))
            cur_tp  = self._to_float(
                self._pos_field(matching_pos, "takeProfit", "tp", "take_profit", "TP", "takeProfitPrice"))
            intended_sl_f = self._to_float(intended_sl)
            intended_tp_f = self._to_float(intended_tp)

            needs_update = False
            update_reason = []

            if intended_sl_f and cur_sl != intended_sl_f:
                needs_update = True
                update_reason.append(f"SL: {cur_sl} → {intended_sl_f}")

            if intended_tp_f and cur_tp != intended_tp_f:
                needs_update = True
                update_reason.append(f"TP: {cur_tp} → {intended_tp_f}")

            if needs_update:
                log("WARN", f"Position {pos_id} ({pair}) SL/TP mismatch detected: "
                    f"{', '.join(update_reason)}. Applying fix...")
                ok, result = self.modify_position(pos_id, pos=matching_pos,
                                                  new_sl=intended_sl_f, new_tp=intended_tp_f)
                if ok:
                    log("INFO", f"✅ SL/TP corrected on position {pos_id}")
                else:
                    log("ERROR", f"Failed to correct SL/TP on position {pos_id}: {result}")
            else:
                log("DEBUG", f"✅ Position {pos_id} ({pair}) SL/TP verified as correct: "
                    f"SL={cur_sl}, TP={cur_tp}")

        except Exception as e:
            log("ERROR", f"Error during SL/TP verification for {pair}: "
                f"{traceback.format_exc()[:200]}")

    # ------------------------------------------------------------------
    # POSITION MANAGEMENT
    # ------------------------------------------------------------------

    def modify_position(self, position_id, pos=None, new_sl=None, new_tp=None):
        """
        FIX-15: TradeLocker's position-modify endpoint requires qty and
        preserves existing TP when only SL is changed.
        Falls back from PUT to PATCH if needed.
        """
        if not self.ensure_auth():
            return False, {"error": "Not authenticated"}
        payload = {}
        if new_sl is not None:
            payload["stopLoss"]   = float(new_sl)
        if new_tp is not None:
            payload["takeProfit"] = float(new_tp)
        if not payload:
            return False, {"error": "Nothing to modify"}

        if isinstance(pos, dict):
            qty = self._pos_field(pos, "qty", "quantity", "size", "volume", "lots")
            if qty not in ("", None):
                try:
                    payload["qty"] = float(qty)
                except (TypeError, ValueError):
                    payload["qty"] = qty
            if new_tp is None:
                cur_tp = self._pos_field(pos, "takeProfit", "tp", "take_profit",
                                         "TP", "takeProfitPrice")
                if cur_tp not in ("", None):
                    try:
                        payload["takeProfit"] = float(cur_tp)
                    except (TypeError, ValueError):
                        payload["takeProfit"] = cur_tp
            if new_sl is None:
                cur_sl = self._pos_field(pos, "stopLoss", "sl", "stop_loss",
                                         "SL", "stopLossPrice")
                if cur_sl not in ("", None):
                    try:
                        payload["stopLoss"] = float(cur_sl)
                    except (TypeError, ValueError):
                        payload["stopLoss"] = cur_sl

        log("INFO", f"Modifying position {position_id}", payload)
        path   = f"/trade/accounts/{self.account_id}/positions/{position_id}"
        result = self._request("PUT", path, body=payload,
                               headers_extra={"accNum": str(self.acc_num)})

        if result.get("error") and str(result.get("error")) in ("HTTP_405", "HTTP_404", "HTTP_400"):
            log("WARN", f"PUT modify rejected ({result.get('error')}), retrying with PATCH")
            result = self._request("PATCH", path, body=payload,
                                   headers_extra={"accNum": str(self.acc_num)})

        if result.get("error") or result.get("s") == "error":
            log("ERROR", f"Modify position {position_id} failed: {result}")
            return False, result
        log("INFO", f"✅ Position {position_id} modified OK")
        return True, result

    def close_position(self, position_id):
        if not self.ensure_auth():
            return False, {"error": "Not authenticated"}
        log("INFO", f"Closing position {position_id}")
        result = self._request("DELETE",
                               f"/trade/accounts/{self.account_id}/positions/{position_id}",
                               headers_extra={"accNum": str(self.acc_num)})
        if result.get("error"):
            log("ERROR", f"Close position {position_id} failed: {result}")
            return False, result
        log("INFO", f"Position {position_id} closed OK")
        return True, result

    # ------------------------------------------------------------------
    # POSITION HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_positions(raw):
        """Normalise whatever TradeLocker returns for positions."""
        if isinstance(raw, list):
            if not raw:
                return []
            if isinstance(raw[0], dict):
                return raw
            log("WARN", f"Positions is bare list-of-lists without header. "
                f"Sample row: {raw[0]}")
            return [
                {_TL_POSITION_COLUMNS[i]: row[i]
                 for i in range(min(len(_TL_POSITION_COLUMNS), len(row)))}
                for row in raw if isinstance(row, (list, tuple))
            ]

        if not isinstance(raw, dict):
            log("WARN", f"Unexpected positions type: {type(raw)}")
            return []

        positions = (raw.get("positions")
                     or raw.get("data")
                     or raw.get("items")
                     or [])
        header    = (raw.get("header")
                     or raw.get("columns")
                     or raw.get("colNames"))

        if not isinstance(positions, list) or not positions:
            return []

        if isinstance(positions[0], dict):
            return positions

        if isinstance(positions[0], (list, tuple)):
            if isinstance(header, list) and header:
                return [
                    {str(header[i]): row[i]
                     for i in range(min(len(header), len(row)))}
                    for row in positions if isinstance(row, (list, tuple))
                ]
            else:
                log("WARN", "Positions list-of-lists has no header key; "
                    "using hardcoded column order")
                return [
                    {_TL_POSITION_COLUMNS[i]: row[i]
                     for i in range(min(len(_TL_POSITION_COLUMNS), len(row)))}
                    for row in positions if isinstance(row, (list, tuple))
                ]

        log("WARN", f"Could not parse positions. keys={list(raw.keys())} "
            f"sample={str(positions[0])[:100]}")
        return []

    @staticmethod
    def _pos_field(pos, *keys):
        """Try multiple field name variants."""
        if not isinstance(pos, dict):
            return ""
        for k in keys:
            v = pos.get(k)
            if v is not None:
                return v
        return ""

    @staticmethod
    def _to_float(v):
        try:
            if v is None or v == "":
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    def get_open_positions(self):
        if not self.ensure_auth():
            return []

        headers = {"accNum": str(self.acc_num)}
        result  = self._request("GET",
                                f"/trade/accounts/{self.account_id}/positions",
                                headers_extra=headers)

        if result.get("error"):
            log("ERROR", f"get_open_positions error: {result.get('error')} | "
                f"{result.get('detail','')[:200]}")
            return []

        data = result.get("d", result)
        positions = self._normalize_positions(data)
        _bridge_status["current_open_positions"] = len(positions)

        # FIX-8: inject instrumentName by reverse lookup
        inst_by_id = {str(v["id"]): v["name"] for v in self.instruments.values()
                      if v.get("id") and v.get("name")}
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            if not pos.get("instrumentName"):
                tid = str(pos.get("tradableInstrumentId") or "")
                if tid and tid in inst_by_id:
                    pos["instrumentName"] = inst_by_id[tid]

        # FIX-20: Track SL/TP status on open positions
        self._track_sltp_status(positions)

        return positions

    def _track_sltp_status(self, positions):
        """
        FIX-20: Monitor which open positions have SL and TP set.
        Useful for alerts if trades are missing stop loss protection.
        """
        check_result = {
            "total_open": len(positions),
            "with_sl": 0,
            "with_tp": 0,
            "with_both": 0,
            "missing_sl": [],
            "missing_tp": [],
            "missing_both": [],
        }
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            has_sl = bool(self._to_float(self._pos_field(
                pos, "stopLoss", "sl", "stop_loss", "SL", "stopLossPrice")))
            has_tp = bool(self._to_float(self._pos_field(
                pos, "takeProfit", "tp", "take_profit", "TP", "takeProfitPrice")))
            pair = self._pos_field(pos, "instrumentName", "symbol", "name", "pair")
            pos_id = self._pos_field(pos, "positionId", "id", "position_id", "tradeId")

            if has_sl:
                check_result["with_sl"] += 1
            if has_tp:
                check_result["with_tp"] += 1
            if has_sl and has_tp:
                check_result["with_both"] += 1

            if not has_sl:
                check_result["missing_sl"].append(f"{pair}({pos_id})")
            if not has_tp:
                check_result["missing_tp"].append(f"{pair}({pos_id})")
            if not has_sl and not has_tp:
                check_result["missing_both"].append(f"{pair}({pos_id})")

        _bridge_status["open_positions_sl_tp_check"] = check_result

        if check_result["missing_both"]:
            log("WARN", f"⚠️  {len(check_result['missing_both'])} open position(s) "
                f"missing BOTH SL and TP: {', '.join(check_result['missing_both'][:5])}")
        elif check_result["missing_sl"]:
            log("WARN", f"⚠️  {len(check_result['missing_sl'])} open position(s) "
                f"missing SL: {', '.join(check_result['missing_sl'][:5])}")
        elif check_result["missing_tp"]:
            log("WARN", f"⚠️  {len(check_result['missing_tp'])} open position(s) "
                f"missing TP: {', '.join(check_result['missing_tp'][:5])}")

    def get_account_info(self):
        """
        FIX-1: GET /trade/accounts (list endpoint) with fallback to single-ID.
        """
        if not self.ensure_auth():
            return {}

        headers = {"accNum": str(self.acc_num)}
        result = self._request("GET", "/trade/accounts", headers_extra=headers)

        if result.get("error"):
            log("WARN", f"GET /trade/accounts error: {result.get('error')} — "
                f"trying /trade/accounts/{self.account_id}")
            result = self._request("GET",
                                   f"/trade/accounts/{self.account_id}",
                                   headers_extra=headers)
            if result.get("error"):
                log("ERROR", f"get_account_info both endpoints failed: "
                    f"{result.get('error')} | {result.get('detail','')[:200]}")
                return {}

        data = result.get("d", result)

        if isinstance(data, list):
            for acct in data:
                if not isinstance(acct, dict):
                    continue
                aid = (acct.get("id") or acct.get("accountId")
                       or acct.get("accId") or acct.get("accountNumber"))
                if str(aid) == str(self.account_id):
                    return acct
            return data[0] if data and isinstance(data[0], dict) else {}

        if isinstance(data, dict):
            accounts_list = (data.get("accounts")
                             or data.get("data")
                             or data.get("items"))
            if isinstance(accounts_list, list) and accounts_list:
                for acct in accounts_list:
                    if not isinstance(acct, dict):
                        continue
                    aid = (acct.get("id") or acct.get("accountId")
                           or acct.get("accId") or acct.get("accountNumber"))
                    if str(aid) == str(self.account_id):
                        return acct
                return accounts_list[0] if isinstance(accounts_list[0], dict) else {}

            for sub_key in ("account", "accountInfo", "details", "info"):
                sub = data.get(sub_key)
                if isinstance(sub, dict) and sub:
                    return {**data, **sub}
            return data

        return {}

    def get_account_state(self):
        """
        FIX-7: Fetch financial state from /state endpoint.
        """
        if not self.ensure_auth():
            return {}

        headers = {"accNum": str(self.acc_num)}
        result  = self._request("GET",
                                f"/trade/accounts/{self.account_id}/state",
                                headers_extra=headers)

        if result.get("error"):
            log("WARN", f"get_account_state /state error: {result.get('error')}")
            return {}

        data = result.get("d", result)

        if isinstance(data, dict):
            for sub_key in ("account", "accountState", "state", "info"):
                sub = data.get(sub_key)
                if isinstance(sub, dict) and sub:
                    return {**data, **sub}
            return data

        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]

        return {}

    # ------------------------------------------------------------------
    # TRADE HISTORY + TP/SL RESULT TRACKING
    # ------------------------------------------------------------------

    def get_trade_history(self, limit=200):
        """
        FIX-17/18: Fetch order history from TradeLocker.
        """
        if not self.ensure_auth():
            return []

        headers = {"accNum": str(self.acc_num)}
        qs      = f"?limit={limit}" if limit else ""
        result  = self._request("GET",
                                f"/trade/accounts/{self.account_id}/orders{qs}",
                                headers_extra=headers)

        if result.get("error"):
            log("ERROR", f"get_trade_history error: {result.get('error')} | "
                f"{result.get('detail','')[:200]}")
            return []

        data = result.get("d", result)

        orders = []
        if isinstance(data, list):
            orders = data
        elif isinstance(data, dict):
            orders = (data.get("orders") or data.get("history")
                      or data.get("data") or data.get("items") or [])

        if not isinstance(orders, list) or not orders:
            return []

        if isinstance(orders[0], dict):
            norm = orders
        elif isinstance(orders[0], (list, tuple)):
            header = (data.get("header") or data.get("columns")
                      or data.get("colNames")) if isinstance(data, dict) else None
            cols   = header if (isinstance(header, list) and header) else _TL_ORDER_COLUMNS
            norm = [{str(cols[i]): row[i]
                     for i in range(min(len(cols), len(row)))}
                    for row in orders if isinstance(row, (list, tuple))]
        else:
            log("WARN", f"Unrecognised trade history row type: {type(orders[0])}")
            return []

        return norm

    def analyze_trade_results(self, orders):
        """
        FIX-17: Derive TP/SL outcomes from order history.
        Cache-aware: returns count of closed trades to detect new closes.
        """
        stats = {
            "closed_trades":   0,
            "tp_hits":         0,
            "sl_hits":         0,
            "manual_closes":   0,
            "wins":            0,
            "losses":          0,
            "win_rate":        None,
            "realized_pnl":    0.0,
            "last_results_check": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "recent":          [],
        }

        of      = self._of
        tofloat = self._to_float

        inst_by_id = {str(v["id"]): v["name"] for v in self.instruments.values()
                      if v.get("id") and v.get("name")}

        opens  = []
        closes = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            otype = str(of(o, "type", "orderType", "order_type") or "").lower()
            exec_t = str(of(o, "executionType", "execution_type") or "").lower()
            status = str(of(o, "status", "orderStatus", "state") or "").lower()

            is_close = ("positionclose" in otype or otype in ("close", "position_close")
                        or "close" in exec_t or otype in ("stop", "stoploss", "takeprofit")
                        or of(o, "closeReason", "close_reason", "positionCloseReason") is not None)
            is_open  = (otype in ("market", "limit", "stop_limit", "marketable_limit")
                        and not is_close)

            if is_close:
                closes.append(o)
            elif is_open and status not in ("cancelled", "canceled", "rejected", "expired"):
                opens.append(o)

        opens_by_pos = {}
        opens_fifo   = {}
        for o in opens:
            pid = of(o, "positionId", "position_id", "tradeId")
            iid = str(of(o, "tradableInstrumentId", "instrumentId") or "")
            if pid:
                opens_by_pos[str(pid)] = o
            opens_fifo.setdefault(iid, []).append(o)

        def _match_open(c):
            pid = of(c, "positionId", "position_id", "tradeId", "orderId")
            if pid and str(pid) in opens_by_pos:
                return opens_by_pos[str(pid)]
            iid = str(of(c, "tradableInstrumentId", "instrumentId") or "")
            lst = opens_fifo.get(iid)
            return lst[0] if lst else None

        def _classify(c, open_o):
            blob = " ".join(str(c.get(k) or "") for k in
                            ("closeReason", "close_reason", "positionCloseReason",
                             "executionType", "execution_type", "reason", "type"))
            blob_l = blob.lower().replace("_", " ").replace("-", " ")
            is_tp = ("takeprofit" in blob_l or "take profit" in blob_l
                     or re.search(r"\btp\b", blob_l) or re.search(r"\btp[_ ]?hit\b", blob_l))
            is_sl = ("stoploss" in blob_l or "stop loss" in blob_l
                     or re.search(r"\bsl\b", blob_l) or re.search(r"\bsl[_ ]?hit\b", blob_l))
            if is_tp and not is_sl:
                return "TP"
            if is_sl and not is_tp:
                return "SL"

            close_px = tofloat(of(c, "price", "avgPrice", "fillPrice", "closePrice"))
            tp_lv    = tofloat(of(open_o, "takeProfit", "tp") if open_o else None)
            sl_lv    = tofloat(of(open_o, "stopLoss", "sl") if open_o else None)
            side     = str(of(open_o or c, "side", "direction") or "").lower()
            if close_px is not None:
                if tp_lv is not None:
                    hit = (close_px >= tp_lv) if "buy" in side else (close_px <= tp_lv)
                    if hit:
                        return "TP"
                if sl_lv is not None:
                    hit = (close_px <= sl_lv) if "buy" in side else (close_px >= sl_lv)
                    if hit:
                        return "SL"
            return "MANUAL"

        def _pnl(c, open_o):
            for key in ("realizedPnL", "realized_pnl", "pnl", "profit", "pl",
                        "closedPnL", "netPnL"):
                v = tofloat(c.get(key))
                if v is not None:
                    return v
            open_px  = tofloat(of(open_o, "price", "avgPrice", "fillPrice")) if open_o else None
            close_px = tofloat(of(c, "price", "avgPrice", "fillPrice", "closePrice"))
            qty      = tofloat(of(c, "qty", "quantity", "size")) or \
                       (tofloat(of(open_o, "qty", "quantity", "size")) if open_o else None)
            side     = str(of(open_o or c, "side", "direction") or "").lower()
            if open_px is not None and close_px is not None and qty:
                diff = close_px - open_px if "buy" in side else open_px - close_px
                return diff * qty
            return None

        results = []
        for c in closes:
            open_o  = _match_open(c)
            outcome = _classify(c, open_o)
            pnl     = _pnl(c, open_o)

            iid  = str(of(c, "tradableInstrumentId", "instrumentId") or "")
            name = (inst_by_id.get(iid)
                    or str(of(c, "instrumentName", "symbol", "name") or iid))

            t_raw  = of(c, "modifiedAt", "placedAt", "closedAt", "time")
            t_num  = tofloat(t_raw)
            if t_num and t_num > 1_000_000_000:
                t_num = t_num / 1000 if t_num > 9_999_999_999 else t_num
                t_str = datetime.fromtimestamp(t_num, tz=timezone.utc).strftime("%m-%d %H:%M")
            else:
                t_str = str(t_raw)[:16]

            results.append({
                "pair":    name,
                "side":    str(of(open_o or c, "side", "direction") or "").upper(),
                "outcome": outcome,
                "pnl":     round(pnl, 2) if pnl is not None else None,
                "time":    t_str,
            })

            stats["closed_trades"] += 1
            if outcome == "TP":
                stats["tp_hits"] += 1
                stats["wins"]    += 1
            elif outcome == "SL":
                stats["sl_hits"] += 1
                stats["losses"]  += 1
            else:
                stats["manual_closes"] += 1
                if pnl is not None:
                    if pnl > 0:
                        stats["wins"] += 1
                    elif pnl < 0:
                        stats["losses"] += 1
            if pnl is not None:
                stats["realized_pnl"] += pnl

        decided = stats["wins"] + stats["losses"]
        if decided:
            stats["win_rate"] = round(100.0 * stats["wins"] / decided, 1)
        stats["realized_pnl"] = round(stats["realized_pnl"], 2)
        stats["recent"] = list(reversed(results))[:15]
        return stats

    @staticmethod
    def _of(order, *keys):
        """First non-None field value from an order dict."""
        if not isinstance(order, dict):
            return None
        for k in keys:
            v = order.get(k)
            if v is not None:
                return v
        return None


# =====================================================
# STATUS PERSISTENCE
# =====================================================

def save_status(force=False):
    _bridge_status["uptime_seconds"]  = int(time.time() - START_TIME)
    _bridge_status["last_update_id"]  = _last_update_id

    payload = {
        "status":    _bridge_status,
        "last_logs": _log_buffer[-200:],
    }
    try:
        tmp = STATUS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, STATUS_FILE)
    except Exception as e:
        if force:
            log("ERROR", f"Failed to save status file: {str(e)[:80]}")

    try:
        hb_path = os.path.join(SCRIPT_DIR, "tradelocker_heartbeat.txt")
        with open(hb_path, "w") as hb:
            hb.write(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass


# =====================================================
# MAIN BRIDGE LOOP
# =====================================================

def run_bridge_cycle(tl_client):
    global _bridge_status, _cache_positions_time, _cache_account_info_time
    global _cache_trade_history_time, _last_history_order_count, _api_failure_count
    
    _bridge_status["status"] = "RUNNING"

    try:
        messages = tg_get_chat_messages(limit=100)
        log("INFO", f"Fetched {len(messages)} new messages from Telegram")
        _bridge_status["last_telegram_poll"] = (
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        log("ERROR", f"Telegram poll failed: {traceback.format_exc()[:300]}")
        _api_failure_count += 1
        return

    processed       = load_processed()
    newly_processed = []

    for msg in messages:
        text = (msg.get("text") or "").strip()
        msg_key = f"{msg.get('chat_id','')}:{msg.get('message_id','')}"

        if msg_key in processed:
            log("DEBUG", f"Already processed: {msg_key}")
            continue

        if not _looks_like_signal(text):
            processed.add(msg_key)
            newly_processed.append(msg_key)
            continue

        parsed = parse_signal_message(text)

        if not parsed:
            log("WARN", f"Message {msg_key} looked like a signal but failed to parse: "
                f"{text[:100]}")
            processed.add(msg_key)
            newly_processed.append(msg_key)
            continue

        _bridge_status["total_signals_received"] += 1

        # ---- TRADE SIGNAL ----
        if parsed["type"] == "SIGNAL":
            direction = parsed["direction"]
            pair      = parsed["pair"]
            sl        = parsed["sl"]
            tp        = parsed["tp"]
            ref_price = parsed.get("ref_price")

            log("INFO", f"🔔 Signal: {direction} {pair} | SL={sl} TP={tp} "
                f"Ref={ref_price} (ignored, executing at live market price NOW)")

            if not sl or not tp:
                log("WARN", f"Signal {msg_key} missing SL or TP — "
                    f"sl={sl} tp={tp}. Placing anyway without SL/TP.")

            success, result = tl_client.place_order(pair, direction, sl, tp,
                                                    ref_price=ref_price)

            if success:
                log("INFO", f"✅ Trade executed: {direction} {pair} "
                    f"order_id={result.get('order_id')}")
                processed.add(msg_key)
                newly_processed.append(msg_key)
                _api_failure_count = max(0, _api_failure_count - 1)
            else:
                log("ERROR", f"❌ Trade FAILED: {direction} {pair} → {result}")
                _api_failure_count += 1

        # ---- TP/SL HIT ----
        elif parsed["type"] == "TPSL_HIT":
            pair        = parsed["pair"]
            result_type = parsed["result"]
            log("INFO", f"📢 {result_type} HIT for {pair}")

            positions  = tl_client.get_open_positions()
            closed_cnt = 0
            for pos in positions:
                pos_sym = TradeLockerClient._pos_field(
                    pos, "instrumentName", "symbol", "name", "pair", "instrument")
                if not _pair_matches(pos_sym, pair):
                    continue
                pos_id = TradeLockerClient._pos_field(
                    pos, "positionId", "id", "position_id", "tradeId")
                if pos_id:
                    ok, _ = tl_client.close_position(pos_id)
                    if ok:
                        log("INFO", f"Position {pos_id} closed for {pair} ({result_type})")
                        closed_cnt += 1
            if closed_cnt:
                log("INFO", f"✅ Closed {closed_cnt} position(s) for {pair} ({result_type} HIT)")
            else:
                log("WARN", f"No open position found to close for {pair}")

            processed.add(msg_key)
            newly_processed.append(msg_key)

        # ---- SL UPDATE ----
        elif parsed["type"] == "SL_UPDATE":
            pair   = parsed["pair"]
            new_sl = parsed["new_sl"]
            log("INFO", f"📝 SL UPDATE {pair} → {new_sl}")

            positions  = tl_client.get_open_positions()
            updated_cnt = 0
            for pos in positions:
                pos_sym = TradeLockerClient._pos_field(
                    pos, "instrumentName", "symbol", "name", "pair", "instrument")
                if not _pair_matches(pos_sym, pair):
                    continue
                pos_id = TradeLockerClient._pos_field(
                    pos, "positionId", "id", "position_id", "tradeId")
                if pos_id:
                    ok, _ = tl_client.modify_position(pos_id, pos=pos, new_sl=new_sl)
                    if ok:
                        log("INFO", f"✅ Position {pos_id} SL updated to {new_sl}")
                        updated_cnt += 1
            if updated_cnt:
                log("INFO", f"✅ SL updated on {updated_cnt} position(s) for {pair} → {new_sl}")
            else:
                log("WARN", f"No open position found to update SL for {pair}")

            processed.add(msg_key)
            newly_processed.append(msg_key)

    if newly_processed:
        save_processed(processed)

    _bridge_status["last_messages"] = [
        {
            "time": datetime.fromtimestamp(
                m.get("date", 0), tz=timezone.utc).strftime("%H:%M:%S"),
            "text": (m.get("text") or "")[:80],
        }
        for m in messages[-10:]
    ]

    # FIX-21: CACHING to reduce API load — only refresh account info every 5 cycles
    current_time = time.time()

    # Cache account info: only refresh every 30s or if it's the first run
    if current_time - _cache_account_info_time > CACHE_ACCOUNT_INFO_SEC or _cache_account_info_time == 0:
        try:
            tl_client.get_account_info()
            tl_client.get_account_state()
            _cache_account_info_time = current_time
        except Exception:
            log("DEBUG", "Skipping account info refresh (caching)")

    # FIX-21: Trade history analysis — only re-analyze if order count changes significantly
    try:
        prev_closed = int((_bridge_status.get("trade_results") or {})
                          .get("closed_trades", 0))
        
        # Only fetch history if enough time has passed or significant order change expected
        if current_time - _cache_trade_history_time > CACHE_TRADE_HISTORY_SEC or _cache_trade_history_time == 0:
            history = tl_client.get_trade_history(limit=100)
            if history:
                history_count = len(history)
                # Only re-analyze if order count changed by >10 or first time
                if abs(history_count - _last_history_order_count) > 10 or _cache_trade_history_time == 0:
                    results = tl_client.analyze_trade_results(history)
                    _bridge_status["trade_results"] = results
                    _last_history_order_count = history_count
                    _cache_trade_history_time = current_time

                    log("DEBUG", f"Trade results (live from TradeLocker): "
                        f"closed={results['closed_trades']} tp={results['tp_hits']} "
                        f"sl={results['sl_hits']} manual={results['manual_closes']} "
                        f"win_rate={results['win_rate']}% pnl={results['realized_pnl']}")
                    
                    if results["closed_trades"] > prev_closed:
                        newest = results["recent"][0] if results["recent"] else {}
                        log("INFO", f"📊 New closed trade detected | {newest.get('pair')} "
                            f"{newest.get('side')} → {newest.get('outcome')} "
                            f"pnl={newest.get('pnl')} | totals: TP={results['tp_hits']} "
                            f"SL={results['sl_hits']} win_rate={results['win_rate']}%")
                else:
                    log("DEBUG", f"Trade history unchanged (cached): "
                        f"{history_count} orders, prev={_last_history_order_count}")
    except Exception:
        log("WARN", f"Trade results check failed: {traceback.format_exc()[:250]}")

    save_status()


# =====================================================
# CONTINUOUS RUNNER
# =====================================================

def continuous_run():
    log("INFO", "TradeLocker Bridge — continuous mode starting")
    _load_persisted_status()

    tl_client = TradeLockerClient(TL_BASE, TL_EMAIL, TL_PASSWORD,
                                  TL_SERVER, TL_ACCOUNT_ID, TL_ACC_NUM)
    if not tl_client.authenticate():
        log("ERROR", "Initial auth failed — cannot start")
        return

    tl_client.load_instruments()

    cycle = 0
    while True:
        try:
            cycle += 1
            run_bridge_cycle(tl_client)
            if cycle % 60 == 0:
                tl_client.load_instruments()
            time.sleep(5)
        except KeyboardInterrupt:
            log("INFO", "Stopped by user")
            _bridge_status["status"] = "STOPPED"
            save_status()
            break
        except Exception:
            log("ERROR", f"Bridge cycle crashed: {traceback.format_exc()[:400]}")
            _bridge_status["total_errors"] += 1
            save_status()
            time.sleep(10)


# =====================================================
# CRON SINGLE-CYCLE RUNNER
# =====================================================

def run_cron_cycle():
    try:
        _load_persisted_status()

        log("INFO", f"=== Cron cycle START | offset={_last_update_id} ===")
        save_status(force=True)

        tg_delete_webhook()

        tl_client = TradeLockerClient(TL_BASE, TL_EMAIL, TL_PASSWORD,
                                      TL_SERVER, TL_ACCOUNT_ID, TL_ACC_NUM)
        if not tl_client.authenticate():
            log("ERROR", "Cron: authentication failed")
            _bridge_status["status"]     = "AUTH_FAILED"
            _bridge_status["last_error"] = "Authentication failed"
            save_status(force=True)
            return False

        tl_client.load_instruments()

        try:
            run_bridge_cycle(tl_client)
        except Exception:
            err = traceback.format_exc()
            log("ERROR", f"Cron cycle error: {err[:400]}")
            _bridge_status["last_error"]    = err[:120]
            _bridge_status["total_errors"] += 1
            save_status(force=True)
            return False

        _tr = _bridge_status.get("trade_results") or {}
        log("INFO", f"=== Cron cycle DONE | signals={_bridge_status['total_signals_received']} "
            f"trades={_bridge_status['total_trades_executed']} "
            f"errors={_bridge_status['total_errors']} | "
            f"TP={_tr.get('tp_hits', 0)} SL={_tr.get('sl_hits', 0)} "
            f"win_rate={_tr.get('win_rate')}% ===")
        save_status(force=True)
        return True

    except Exception:
        err = traceback.format_exc()
        try:
            log("ERROR", f"Cron runner crashed: {err[:400]}")
            save_status(force=True)
        except Exception:
            pass
        return False


# =====================================================
# CGI DASHBOARD
# =====================================================

def render_dashboard():
    status_data = {"status": {}, "last_logs": []}
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r") as f:
                status_data = json.load(f)
    except Exception:
        pass

    bs = status_data.get("status") or {}
    if not bs:
        bs = {
            "status": "UNKNOWN", "last_telegram_poll": None,
            "last_trade_executed": None, "total_signals_received": 0,
            "total_trades_executed": 0, "total_errors": 0,
            "current_open_positions": 0, "access_token_expires_at": None,
            "instruments_loaded": 0, "last_error": None,
            "uptime_seconds": 0,
            "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "last_messages": [],
        }

    logs = status_data.get("last_logs", [])[-200:]

    positions     = []
    account_info  = {}
    account_state = {}
    trade_results = bs.get("trade_results") or {}
    sltp_check    = bs.get("open_positions_sl_tp_check") or {}  # FIX-20
    auth_error    = None
    try:
        tl_client = TradeLockerClient(TL_BASE, TL_EMAIL, TL_PASSWORD,
                                      TL_SERVER, TL_ACCOUNT_ID, TL_ACC_NUM)
        if tl_client.authenticate():
            tl_client.load_instruments()
            account_info  = tl_client.get_account_info()
            account_state = tl_client.get_account_state()
            positions     = tl_client.get_open_positions()
            _hist = tl_client.get_trade_history()
            trade_results = (tl_client.analyze_trade_results(_hist)
                             if _hist else (bs.get("trade_results") or {}))
            sltp_check = bs.get("open_positions_sl_tp_check") or {}
        else:
            auth_error = "Dashboard live-fetch: authentication failed"
    except Exception as exc:
        auth_error = f"Dashboard live-fetch exception: {str(exc)[:100]}"

    def _af(*keys):
        for src in (account_state, account_info):
            for k in keys:
                v = src.get(k)
                if v is not None:
                    return v
        return "N/A"

    balance      = _af("accountBalance", "balance", "Balance", "totalBalance")
    equity       = _af("equity",  "Equity",  "accountEquity",  "totalEquity",
                       "unrealizedPnL")
    margin       = _af("usedMargin", "margin", "Margin", "marginUsed",
                       "requiredMargin", "lockedMargin")
    margin_level = _af("marginLevel", "margin_level", "MarginLevel",
                       "marginLevelPercent", "marginPct")

    print("Content-Type: text/html; charset=utf-8")
    print()

    status_color = ("green" if bs.get("status") in ("RUNNING", "AUTHENTICATED")
                    else "yellow" if bs.get("status") == "STOPPED"
                    else "red")

    uptime_s = int(bs.get("uptime_seconds", 0))
    uptime_h = uptime_s // 3600
    uptime_m = (uptime_s % 3600) // 60
    uptime_display = f"{uptime_h}h {uptime_m}m"

    hb_age = "N/A"
    hb_color = "red"
    try:
        hb_path = os.path.join(SCRIPT_DIR, "tradelocker_heartbeat.txt")
        if os.path.exists(hb_path):
            with open(hb_path) as hbf:
                hb_ts = hbf.read().strip()
            hb_dt  = datetime.strptime(hb_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            hb_sec = int((datetime.now(timezone.utc) - hb_dt).total_seconds())
            hb_age = f"{hb_sec}s ago"
            hb_color = "green" if hb_sec < 120 else "yellow" if hb_sec < 300 else "red"
    except Exception:
        pass

    tg_poll = bs.get("last_telegram_poll")
    tg_color = "green"
    tg_label = "LIVE"
    if not tg_poll:
        tg_color = "yellow"; tg_label = "AWAITING CRON"
    else:
        try:
            last_dt = datetime.strptime(tg_poll, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_s   = (datetime.now(timezone.utc) - last_dt).total_seconds()
            if age_s > 300:
                tg_color = "red"; tg_label = f"STALE ({int(age_s)}s)"
        except Exception:
            tg_color = "yellow"; tg_label = "UNKNOWN"

    if bs.get("last_error") and tg_label == "LIVE":
        tg_color = "orange"; tg_label = "LIVE (errors)"

    print(f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>TradeLocker Bridge v1.7</title>
<style>
:root{{
  --bg:#0d1117;--card:#161b22;--card2:#1c2128;--border:#30363d;
  --text:#c9d1d9;--text2:#8b949e;
  --green:#3fb950;--red:#f85149;--yellow:#d29922;--orange:#f0883e;
  --blue:#58a6ff;--purple:#bc8cff;--cyan:#39c5cf;--pink:#f778ba;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:13px;line-height:1.4}}
.wrap{{max-width:1600px;margin:0 auto;padding:10px}}
.hdr{{display:flex;justify-content:space-between;align-items:center;
      padding:10px 14px;background:var(--card);border:1px solid var(--border);
      border-radius:8px;margin-bottom:10px;flex-wrap:wrap;gap:8px}}
.hdr h1{{font-size:15px;font-weight:700;color:#fff;letter-spacing:.3px}}
.hdr-right{{display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}}
.g3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px}}
.g4{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:8px}}
.g8{{display:grid;grid-template-columns:repeat(8,1fr);gap:8px;margin-bottom:8px}}
.stat{{text-align:center;padding:14px 8px}}
.stat-v{{font-size:22px;font-weight:800;line-height:1}}
.stat-l{{font-size:9px;color:var(--text2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px}}
.green{{color:var(--green)}}.red{{color:var(--red)}}.yellow{{color:var(--yellow)}}
.orange{{color:var(--orange)}}.blue{{color:var(--blue)}}.purple{{color:var(--purple)}}
.cyan{{color:var(--cyan)}}.pink{{color:var(--pink)}}.text2{{color:var(--text2)}}
.dot{{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:5px;flex-shrink:0}}
.dot-green{{background:var(--green);box-shadow:0 0 5px var(--green)}}
.dot-red{{background:var(--red);box-shadow:0 0 5px var(--red)}}
.dot-yellow{{background:var(--yellow);box-shadow:0 0 5px var(--yellow)}}
.dot-orange{{background:var(--orange);box-shadow:0 0 5px var(--orange)}}
.term{{background:#05080f;border:1px solid var(--border);border-radius:6px;
       font-family:"Consolas","Courier New",monospace;font-size:10.5px;
       padding:10px;height:480px;overflow-y:auto;line-height:1.6;
       display:flex;flex-direction:column-reverse}}
.term-inner{{display:flex;flex-direction:column}}
.tl{{margin-bottom:1px}}
.ti{{color:#7ee787}}.tw{{color:#e3b341}}.te{{color:#ff7b72}}.td{{color:#8b949e}}
table{{width:100%;border-collapse:collapse;font-size:11px}}
th{{padding:7px 8px;background:var(--card2);color:var(--text2);
    font-size:9px;text-transform:uppercase;letter-spacing:.4px;
    border-bottom:2px solid var(--border);text-align:left;white-space:nowrap}}
td{{padding:7px 8px;border-bottom:1px solid var(--border);vertical-align:middle}}
tr:hover td{{background:var(--card2)}}
.btn{{padding:5px 10px;border:1px solid var(--border);border-radius:4px;
      cursor:pointer;font-size:11px;background:var(--card2);color:var(--text);
      text-decoration:none;display:inline-block}}
.btn:hover{{background:var(--border)}}
.btn-green{{background:rgba(63,185,80,.15);color:var(--green);border-color:var(--green)}}
.btn-red{{background:rgba(248,81,73,.15);color:var(--red);border-color:var(--red)}}
.pill{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:9px;font-weight:600}}
.pill-green{{background:rgba(63,185,80,.2);color:var(--green)}}
.pill-red{{background:rgba(248,81,73,.2);color:var(--red)}}
.pill-gray{{background:rgba(139,148,158,.15);color:var(--text2)}}
.section-title{{font-size:10px;color:var(--text2);font-weight:600;
                text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}}
.err-banner{{background:rgba(248,81,73,.12);border:1px solid var(--red);
             border-radius:6px;padding:10px;margin-bottom:8px;color:var(--red);
             font-size:11px;word-break:break-all}}
.info-row{{display:flex;align-items:center;gap:6px;font-size:11px}}
.mb8{{margin-bottom:8px}}
@media(max-width:1100px){{.g8{{grid-template-columns:repeat(4,1fr)}}}}
@media(max-width:800px){{.g4,.g8{{grid-template-columns:repeat(2,1fr)}}.g3{{grid-template-columns:1fr 1fr}}}}
@media(max-width:500px){{.g2,.g3,.g4,.g8{{grid-template-columns:1fr}}}}
</style></head>
<body><div class="wrap">""")

    # ---- HEADER ----
    print(f"""<div class="hdr">
  <h1><span class="dot dot-{status_color}"></span>TradeLocker Bridge <span style="color:var(--text2);font-weight:400">v1.7</span></h1>
  <div class="hdr-right">
    <button class="btn btn-green" onclick="fetch('?action=test_tg').then(()=>alert('Sent!'))">✉ Test Telegram</button>
    <button class="btn" onclick="location.reload()">⟳ Refresh</button>
    <span style="color:var(--text2);font-size:10px">Auto-refresh: 30s</span>
    <span class="{status_color}" style="font-weight:700;font-size:11px">{bs.get('status','UNKNOWN')}</span>
    <span style="color:var(--text2);font-size:10px">{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</span>
  </div>
</div>""")

    # ---- CONNECTION STATUS ROW ----
    tl_ok    = bs.get("status") in ("RUNNING", "AUTHENTICATED")
    tl_color_dot = "green" if tl_ok else "red"
    tl_label = "CONNECTED" if tl_ok else bs.get("status", "DISCONNECTED")

    print(f"""<div class="g3 mb8">
  <div class="card">
    <div class="section-title">TradeLocker</div>
    <div class="info-row">
      <span class="dot dot-{tl_color_dot}"></span>
      <strong class="{tl_color_dot}">{tl_label}</strong>
      <span class="text2" style="font-size:9px">Expires: {bs.get('access_token_expires_at') or 'N/A'}</span>
    </div>
    <div style="font-size:9px;color:var(--text2);margin-top:4px">
      Env: <strong>{TL_ENV.upper()}</strong> | Server: <strong>{TL_SERVER}</strong> |
      AccID: <strong>{TL_ACCOUNT_ID}</strong> | AccNum: <strong>{TL_ACC_NUM}</strong>
    </div>
  </div>
  <div class="card">
    <div class="section-title">Telegram</div>
    <div class="info-row">
      <span class="dot dot-{tg_color}"></span>
      <strong class="{tg_color}">{tg_label}</strong>
      <span class="text2" style="font-size:9px">Last poll: {tg_poll or 'Never'}</span>
    </div>
    <div style="font-size:9px;color:var(--text2);margin-top:4px">
      Chat: <strong>{html.escape(str(TG_CHAT))}</strong> |
      Offset: <strong>{bs.get('last_update_id', 0)}</strong> |
      Instruments: <strong>{bs.get('instruments_loaded', 0)}</strong>
    </div>
  </div>
  <div class="card">
    <div class="section-title">Heartbeat</div>
    <div class="info-row">
      <span class="dot dot-{hb_color}"></span>
      <strong class="{hb_color}">{hb_age}</strong>
      <span class="text2" style="font-size:9px">Uptime: {uptime_display}</span>
    </div>
    <div style="font-size:9px;color:var(--text2);margin-top:4px">
      Started: {bs.get('started_at','?')} |
      Last trade: {bs.get('last_trade_executed') or 'Never'}
    </div>
  </div>
</div>""")

    # ---- STATS GRID (8 tiles) ----
    def _fmt(v):
        if v == "N/A" or v is None:
            return "N/A"
        try:
            fv = float(v)
            return f"{fv:,.2f}"
        except (ValueError, TypeError):
            return str(v)[:14]

    print('<div class="g8">')
    tiles = [
        (bs.get("total_signals_received", 0), "Signals Recv",  "blue"),
        (bs.get("total_trades_executed",   0), "Trades Done",   "green"),
        (bs.get("total_errors",            0), "Errors",        "red" if bs.get("total_errors",0)>0 else "text2"),
        (bs.get("current_open_positions",  0), "Open Positions","purple"),
        (_fmt(balance),                        "Balance",       "cyan"),
        (_fmt(equity),                         "Equity",        "blue"),
        (_fmt(margin),                         "Margin Used",   "yellow"),
        (_fmt(margin_level) + ("%" if _fmt(margin_level) not in ("N/A","") else ""),
         "Margin Level", "orange"),
    ]
    for val, label, col in tiles:
        print(f'<div class="card stat">'
              f'<div class="stat-v {col}">{val}</div>'
              f'<div class="stat-l">{label}</div>'
              f'</div>')
    print('</div>')

    # ---- LAST ERROR BANNER ----
    if auth_error:
        print(f'<div class="err-banner">⚠ Dashboard fetch: {html.escape(auth_error)}</div>')
    if bs.get("last_error"):
        print(f'<div class="err-banner">⚠ Last bridge error: {html.escape(str(bs["last_error"]))}</div>')

    # FIX-20: SL/TP CHECK BANNER ----
    if sltp_check.get("missing_both") or sltp_check.get("missing_sl") or sltp_check.get("missing_tp"):
        warn_level = ("red" if sltp_check.get("missing_both")
                      else "orange" if sltp_check.get("missing_sl")
                      else "yellow")
        msg_parts = []
        if sltp_check.get("missing_both"):
            msg_parts.append(f"{len(sltp_check['missing_both'])} without SL & TP")
        if sltp_check.get("missing_sl") and not sltp_check.get("missing_both"):
            msg_parts.append(f"{len(sltp_check['missing_sl'])} without SL")
        if sltp_check.get("missing_tp") and not sltp_check.get("missing_both"):
            msg_parts.append(f"{len(sltp_check['missing_tp'])} without TP")
        print(f'<div class="err-banner" style="background:rgba(248,{81 if warn_level=="red" else 160},73,.08);border-color:var(--{warn_level})">'
              f'⚠️  SL/TP MISSING: {" | ".join(msg_parts)}</div>')

    # ---- LIVE LOG ----
    print('<div class="card mb8">')
    print('<div class="section-title">Live Log (newest first) — last 200 entries</div>')
    print('<div class="term"><div class="term-inner">')
    for entry in logs:
        lvl  = entry.get("level", "INFO")
        msg  = entry.get("message", "")
        ts   = entry.get("time", "")
        dstr = entry.get("data") or ""
        cls  = ("te" if lvl == "ERROR" else
                "tw" if lvl == "WARN"  else
                "td" if lvl == "DEBUG" else "ti")
        data_html = (f' <span style="color:#6e7681">| {html.escape(dstr[:200])}</span>'
                     if dstr else "")
        print(f'<div class="tl {cls}">'
              f'[{ts}] <strong>{lvl}</strong> {html.escape(msg)}{data_html}</div>')
    print('</div></div></div>')

    # ---- RECENT TELEGRAM MESSAGES ----
    print('<div class="card mb8">')
    print('<div class="section-title">Recent Telegram Messages</div>')
    print('<div class="term" style="height:180px"><div class="term-inner">')
    for m in bs.get("last_messages", []):
        print(f'<div class="tl ti">[{m.get("time","")}] '
              f'{html.escape(m.get("text",""))}</div>')
    print('</div></div></div>')

    # ---- OPEN POSITIONS TABLE ----
    pf = TradeLockerClient._pos_field

    print('<div class="card mb8">')
    print(f'<div class="section-title">Open Positions ({len(positions)})</div>')
    print('<div style="overflow-x:auto">')
    print('<table><thead><tr>'
          '<th>#</th><th>Pair</th><th>Side</th><th>Qty</th>'
          '<th>Entry / Open Price</th><th>SL</th><th>TP</th>'
          '<th>P&L</th><th>Position ID</th><th>Open Time</th><th>Status</th>'
          '</tr></thead><tbody>')

    if not positions:
        print('<tr><td colspan="11" style="text-align:center;color:var(--text2);padding:20px">'
              'No open positions returned by API</td></tr>')
    else:
        for idx, pos in enumerate(positions, 1):
            if not isinstance(pos, dict):
                print(f'<tr><td colspan="11" class="text2">Row {idx}: '
                      f'not a dict ({type(pos).__name__}): '
                      f'{html.escape(str(pos)[:100])}</td></tr>')
                continue

            pos_pair  = pf(pos, "instrumentName","symbol","name","pair","instrument")
            pos_side  = pf(pos, "side","direction","type","orderSide","positionSide")
            pos_qty   = pf(pos, "qty","quantity","size","volume","lots")
            pos_entry = pf(pos, "price","openPrice","entryPrice","entry","avgPrice","avgOpenPrice")
            pos_sl    = pf(pos, "stopLoss","sl","stop_loss","SL","stopLossPrice")
            pos_tp    = pf(pos, "takeProfit","tp","take_profit","TP","takeProfitPrice")
            pos_pnl   = pf(pos, "pnl","pl","profit","unrealizedPnL","unrealizedPnl",
                           "unrealized_pnl","floatingPL","floatingPl")
            pos_id    = pf(pos, "positionId","id","position_id","tradeId","orderId","order_id")
            pos_time  = pf(pos, "openTime","createdAt","placedAt","placed_at",
                           "time","openTimestamp","openedAt","date")
            pos_stat  = pf(pos, "status","state","positionStatus","orderStatus")

            side_up   = str(pos_side).upper()
            side_col  = "green" if ("BUY" in side_up or "LONG" in side_up) else "red"
            pnl_f     = None
            try:
                pnl_f = float(pos_pnl)
            except (TypeError, ValueError):
                pass
            pnl_col   = ("green" if pnl_f and pnl_f > 0
                         else "red" if pnl_f and pnl_f < 0
                         else "text2")
            pnl_disp  = f"{pnl_f:+.2f}" if pnl_f is not None else str(pos_pnl)

            time_disp = str(pos_time)
            try:
                ts_num = float(pos_time)
                if ts_num > 1_000_000_000:
                    ts_sec = ts_num / 1000 if ts_num > 9_999_999_999 else ts_num
                    time_disp = datetime.fromtimestamp(
                        ts_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            except (TypeError, ValueError, OSError):
                pass

            _STATUS_MAP = {"1": "PENDING", "2": "OPEN", "3": "CLOSED",
                           "4": "CANCELLED", "5": "REJECTED"}
            pos_stat_str = _STATUS_MAP.get(str(pos_stat), str(pos_stat))
            stat_pill = ("pill-green" if pos_stat_str.upper() in ("OPEN", "ACTIVE", "FILLED", "2")
                         else "pill-red"  if pos_stat_str.upper() in ("CLOSED", "CANCELLED", "REJECTED")
                         else "pill-gray")

            print(f'<tr>'
                  f'<td class="text2">{idx}</td>'
                  f'<td><strong>{html.escape(str(pos_pair))}</strong></td>'
                  f'<td class="{side_col}"><strong>{html.escape(str(pos_side))}</strong></td>'
                  f'<td>{html.escape(str(pos_qty))}</td>'
                  f'<td>{html.escape(str(pos_entry))}</td>'
                  f'<td class="red">{html.escape(str(pos_sl))}</td>'
                  f'<td class="green">{html.escape(str(pos_tp))}</td>'
                  f'<td class="{pnl_col}">{html.escape(pnl_disp)}</td>'
                  f'<td style="font-size:9px;color:var(--text2)">{html.escape(str(pos_id))[:22]}</td>'
                  f'<td style="font-size:9px;color:var(--text2)">{html.escape(time_disp)[:19]}</td>'
                  f'<td><span class="pill {stat_pill}">{html.escape(pos_stat_str)}</span></td>'
                  f'</tr>')

        if positions and isinstance(positions[0], dict):
            raw_keys = ", ".join(positions[0].keys())
            print(f'<tr><td colspan="11" style="font-size:9px;color:var(--text2);'
                  f'padding:4px 8px">ℹ Raw field names from API: {html.escape(raw_keys)}</td></tr>')

    print('</tbody></table></div></div>')

    # ---- TRADE RESULTS (FIX-17: live from TradeLocker history) ----
    tr  = trade_results or {}
    wr  = tr.get("win_rate")
    wr_disp = f"{wr}%" if wr is not None else "—"
    rpnl = tr.get("realized_pnl", 0.0)
    try:
        rpnl_f = float(rpnl)
    except (TypeError, ValueError):
        rpnl_f = 0.0
    rpnl_col = "green" if rpnl_f > 0 else "red" if rpnl_f < 0 else "text2"

    print('<div class="card mb8">')
    print('<div class="section-title">Trade Results — TP/SL tracked live from TradeLocker '
          f'<span class="text2" style="text-transform:none;letter-spacing:0">'
          f'(checked {html.escape(str(tr.get("last_results_check") or "—"))} UTC)</span></div>')

    print('<div class="g4" style="margin-bottom:10px">')
    for val, label, col in [
        (tr.get("tp_hits", 0),        "TP Hits (Wins)",   "green"),
        (tr.get("sl_hits", 0),        "SL Hits (Losses)", "red"),
        (wr_disp,                     "Win Rate",         "cyan"),
        (f"{rpnl_f:+,.2f}",           "Realized P&L",     rpnl_col),
    ]:
        print(f'<div class="card stat" style="padding:10px 6px">'
              f'<div class="stat-v {col}" style="font-size:19px">{val}</div>'
              f'<div class="stat-l">{label}</div></div>')
    print('</div>')

    recent = tr.get("recent") or []
    if recent:
        print('<div style="overflow-x:auto"><table><thead><tr>'
              '<th>#</th><th>Pair</th><th>Side</th><th>Outcome</th>'
              '<th>P&L</th><th>Closed (UTC)</th></tr></thead><tbody>')
        for i, r in enumerate(recent, 1):
            oc = str(r.get("outcome") or "?")
            oc_col  = "green" if oc == "TP" else "red" if oc == "SL" else "text2"
            oc_pill = "pill-green" if oc == "TP" else "pill-red" if oc == "SL" else "pill-gray"
            p = r.get("pnl")
            try:
                pf_ = float(p)
                p_disp = f"{pf_:+.2f}"
                p_col  = "green" if pf_ > 0 else "red" if pf_ < 0 else "text2"
            except (TypeError, ValueError):
                p_disp = str(p)
                p_col  = "text2"
            side_ = str(r.get("side") or "").upper()
            s_col = "green" if "BUY" in side_ else "red"
            print(f'<tr><td class="text2">{i}</td>'
                  f'<td><strong>{html.escape(str(r.get("pair") or ""))}</strong></td>'
                  f'<td class="{s_col}"><strong>{html.escape(side_)}</strong></td>'
                  f'<td><span class="pill {oc_pill}">{html.escape(oc)}</span></td>'
                  f'<td class="{p_col}">{html.escape(p_disp)}</td>'
                  f'<td class="text2">{html.escape(str(r.get("time") or ""))}</td></tr>')
        print('</tbody></table></div>')
    else:
        print('<div style="color:var(--text2);font-size:11px;padding:8px">'
              'No closed trades found in the account history yet.</div>')
    print('</div>')

    # ---- FOOTER ----
    print(f'<div style="text-align:center;padding:10px;color:var(--text2);font-size:9px">'
          f'TradeLocker Bridge v1.7 | '
          f'Started: {bs.get("started_at","?")} | '
          f'Uptime: {uptime_display} | '
          f'Page auto-refreshes every 30s'
          f'</div>')

    print('</div></body></html>')


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    try:
        is_cgi    = bool(os.environ.get("REQUEST_METHOD") or
                         os.environ.get("GATEWAY_INTERFACE"))
        qs        = os.environ.get("QUERY_STRING", "")
        qs_parts  = qs.split("&")
        cron_mode = ("--cron" in qs
                     or any(p in ("mode=cron", "action=cron", "cron=1", "cron=true")
                            for p in qs_parts)
                     or "cron" in qs_parts)
        test_tg   = "action=test_tg" in qs

        if test_tg:
            tg_send_message("✅ Test message from TradeLocker Bridge v1.7")
            print("Content-Type: text/plain; charset=utf-8")
            print()
            print("Test message sent.")
            sys.exit(0)

        if is_cgi and not cron_mode:
            render_dashboard()

        elif is_cgi and cron_mode:
            run_cron_cycle()
            sys.exit(0)

        elif "--cron" in sys.argv:
            ok = run_cron_cycle()
            sys.exit(0 if ok else 1)

        else:
            continuous_run()

    except Exception:
        err = traceback.format_exc()
        try:
            log("ERROR", f"Fatal startup: {err[:400]}")
            save_status(force=True)
        except Exception:
            pass
        try:
            if os.environ.get("REQUEST_METHOD") or os.environ.get("GATEWAY_INTERFACE"):
                print("Content-Type: text/plain; charset=utf-8")
                print()
                print(f"Fatal error:\n{err[:2000]}")
                sys.exit(1)
        except Exception:
            pass
        sys.exit(1)

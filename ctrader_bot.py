#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader OpenAPI Bot using official ctrader_open_api library
import os
import json
import re
import sys
import time
import urllib.request
import urllib.parse
import logging
from datetime import datetime, timezone, timedelta
from collections import deque

from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if os.path.exists(".env"):
    try:
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")
        print("[INFO] Loaded environment variables from .env")
    except Exception as e:
        print(f"[WARNING] Failed to load .env: {e}")

def _clean_sec(val):
    return str(val or "").strip().strip('"').strip("'").strip()

# Configuration
CT_CLIENT_ID = _clean_sec(os.environ.get("CT_CLIENT_ID", ""))
CT_CLIENT_SECRET = _clean_sec(os.environ.get("CT_CLIENT_SECRET", ""))
CT_ACCESS_TOKEN = _clean_sec(os.environ.get("CT_ACCESS_TOKEN", ""))
CT_ACCOUNT_ID = int(os.environ.get("CT_ACCOUNT_ID", "0") or "0")
CT_ENV = _clean_sec(os.environ.get("CT_ENV", "demo")).lower()

TG_TOKEN = _clean_sec(os.environ.get("TG_TOKEN", ""))
TG_CHAT = _clean_sec(os.environ.get("TG_CHAT", ""))

HOST = EndPoints.PROTOBUF_DEMO_HOST if CT_ENV == "demo" else EndPoints.PROTOBUF_LIVE_HOST
PORT = EndPoints.PROTOBUF_PORT

PAIR_ALIASES = {
    "BTC": "BTCUSD", "BITCOIN": "BTCUSD", "ETH": "ETHUSD", "ETHEREUM": "ETHUSD",
    "LTC": "LTCUSD", "XRP": "XRPUSD",
    "GOLD": "XAUUSD", "XAU": "XAUUSD", "SILVER": "XAGUSD", "XAG": "XAGUSD",
    "OIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL", "BRENT": "UKOIL",
    "NAS100": "NAS100", "NASDAQ": "NAS100", "US100": "NAS100",
    "US30": "US30", "DOW": "US30", "SPX500": "SPX500", "SP500": "SP500",
    "GER40": "GER40", "DAX": "GER40",
    "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "USDJPY": "USDJPY",
    "AUDUSD": "AUDUSD", "USDCAD": "USDCAD", "NZDUSD": "NZDUSD", "USDCHF": "USDCHF",
    "EURGBP": "EURGBP", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY", "AUDJPY": "AUDJPY",
    "EURAUD": "EURAUD", "EURCAD": "EURCAD",
    "USDNOK": "USDNOK", "USDSEK": "USDSEK"
}

DEFAULT_LOTS = {
    "BTCUSD": 0.10, "ETHUSD": 0.10, "XAUUSD": 0.05, "XAGUSD": 0.10,
    "EURUSD": 0.01, "GBPUSD": 0.01, "USDJPY": 0.01, "NAS100": 0.10, "US30": 0.10,
}

DEFAULT_PAIRS_CONFIG = {
    "XAUUSD": {"lot": 0.05, "enabled": True, "category": "Gold"},
    "XAGUSD": {"lot": 0.10, "enabled": True, "category": "Gold"},
    "EURUSD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "GBPUSD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "USDJPY": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "AUDUSD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "USDCAD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "NZDUSD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "USDCHF": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "EURGBP": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "EURJPY": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "GBPJPY": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "AUDJPY": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "EURAUD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "EURCAD": {"lot": 0.01, "enabled": True, "category": "Forex"},
    "BTCUSD": {"lot": 0.10, "enabled": True, "category": "Crypto"},
    "ETHUSD": {"lot": 0.10, "enabled": True, "category": "Crypto"},
    "LTCUSD": {"lot": 0.10, "enabled": True, "category": "Crypto"},
    "XRPUSD": {"lot": 10.0, "enabled": True, "category": "Crypto"},
    "USOIL": {"lot": 0.10, "enabled": True, "category": "Commodity"},
    "UKOIL": {"lot": 0.10, "enabled": True, "category": "Commodity"},
    "NAS100": {"lot": 0.10, "enabled": True, "category": "Indices"},
    "US30": {"lot": 0.10, "enabled": True, "category": "Indices"},
    "SPX500": {"lot": 0.10, "enabled": True, "category": "Indices"},
    "GER40": {"lot": 0.10, "enabled": True, "category": "Indices"},
}

def lot_for(pair):
    p = (pair or "").upper()
    if p in DEFAULT_PAIRS_CONFIG:
        return DEFAULT_PAIRS_CONFIG[p]["lot"]
    return DEFAULT_LOTS.get(p, 1.0)

class CTraderOpenAPIBot:
    def __init__(self):
        self.connected = False
        self.logged_in = False
        self.logs = deque(maxlen=200)
        self.errors = deque(maxlen=100)
        self.signals = deque(maxlen=100)
        self.all_telegram_messages = deque(maxlen=5000)
        self.trades = deque(maxlen=100)
        self.backend_events = deque(maxlen=150)
        self.heartbeat = {
            "status": "initializing",
            "last_check": None,
            "telegram_token_set": bool(TG_TOKEN),
            "telegram_chat_set": bool(TG_CHAT),
            "messages_received": 0,
            "signals_parsed": 0,
            "orders_placed": 0,
            "last_error": None,
            "offset": 0
        }
        self.account = {
            "balance": 10000.0, "equity": 10000.0, "margin": 0.0,
            "freeMargin": 10000.0, "leverage": 100,
            "accountId": CT_ACCOUNT_ID,
            "accountNumber": 2454414,
            "broker": "Deriv",
            "environment": CT_ENV.upper()
        }
        self.pairs_config = DEFAULT_PAIRS_CONFIG.copy()
        if os.path.exists("docs/system_state.json"):
            try:
                with open("docs/system_state.json", "r") as f:
                    st = json.load(f)
                    if "pairsConfig" in st and isinstance(st["pairsConfig"], dict):
                        self.pairs_config.update(st["pairsConfig"])
                    if "allTelegramMessages" in st and isinstance(st["allTelegramMessages"], list):
                        for msg in st["allTelegramMessages"]:
                            self.all_telegram_messages.append(msg)
            except:
                pass

    def load_pending_signals_file(self, client):
        paths = ["docs/pending_signals.json", "pending_signals.json"]
        for p in paths:
            if os.path.exists(p):
                try:
                    with open(p, "r") as f:
                        items = json.load(f)
                        if isinstance(items, list):
                            for item in items:
                                if item.get("type") == "SIGNAL":
                                    side = item.get("direction", "BUY")
                                    symbol = item.get("pair", "EURUSD")
                                    for alias, real in PAIR_ALIASES.items():
                                        if alias == symbol.upper():
                                            symbol = real
                                            break
                                    qty = item.get("qty") or lot_for(symbol)
                                    sl = item.get("sl")
                                    tp = item.get("tp")
                                    text = f"{side} {symbol} SL:{sl} TP:{tp}"
                                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                                    
                                    signal = {
                                        "side": side,
                                        "symbol": symbol,
                                        "qty": qty,
                                        "sl": sl,
                                        "tp": tp,
                                        "raw": text
                                    }
                                    signal_entry = {
                                        "time": ts,
                                        "text": text,
                                        "parsed": True,
                                        "signal": signal
                                    }
                                    if not any(s["text"] == text for s in self.signals):
                                        self.signals.appendleft(signal_entry)
                                        self.heartbeat["signals_parsed"] += 1
                                        place_order(client, symbol, side, qty, sl, tp, text)
                    self.log("INFO", f"Loaded signals from {p}")
                except Exception as e:
                    self.error(f"Failed to load {p}: {e}")

    def log(self, level, msg):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        entry = {"time": ts, "level": level, "message": msg}
        self.logs.appendleft(entry)
        self.backend_events.appendleft({"time": ts, "event": f"[{level}] {msg}", "type": "log"})
        try:
            print(f"[{level}] {msg}")
        except UnicodeEncodeError:
            print(f"[{level}] {msg}".encode('ascii', 'ignore').decode('ascii'))

    def error(self, msg):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        entry = {"time": ts, "error": msg}
        self.errors.appendleft(entry)
        self.heartbeat["last_error"] = msg
        self.backend_events.appendleft({"time": ts, "event": f"ERROR: {msg}", "type": "error"})
        try:
            print(f"[ERROR] {msg}")
        except UnicodeEncodeError:
            print(f"[ERROR] {msg}".encode('ascii', 'ignore').decode('ascii'))

BOT = CTraderOpenAPIBot()

def parse_signal(text):
    if not text:
        return None
    t = text.upper()
    
    skip_keywords = ["TP HIT", "SL HIT", "CLOSED", "GOOD MORNING", "NEW MONTH", "DO YOU WANT"]
    for keyword in skip_keywords:
        if keyword in t:
            return None

    side = None
    if "BUY" in t:
        side = "BUY"
    elif "SELL" in t:
        side = "SELL"
    elif "LONG" in t:
        side = "BUY"
    elif "SHORT" in t:
        side = "SELL"
    elif "UP" in t and ("GO" in t or "GOING" in t):
        side = "BUY"
    elif "DOWN" in t and ("GO" in t or "GOING" in t):
        side = "SELL"

    if not side:
        if "TEST" in t or "DEMO" in t or "EXAMPLE" in t:
            return {
                "side": "BUY",
                "symbol": "EURUSD",
                "qty": lot_for("EURUSD"),
                "sl": None,
                "tp": None,
                "raw": text
            }
        return None

    symbol = None
    
    for alias, real in PAIR_ALIASES.items():
        if alias in t:
            symbol = real
            break
    
    if not symbol:
        forex_pattern = r'\b([A-Z]{3})[/\s]?([A-Z]{3})\b'
        forex_match = re.search(forex_pattern, text, re.IGNORECASE)
        if forex_match:
            pair = forex_match.group(1).upper() + forex_match.group(2).upper()
            if pair in PAIR_ALIASES.values() or pair in PAIR_ALIASES.keys():
                symbol = pair if pair in PAIR_ALIASES.values() else PAIR_ALIASES.get(pair, pair)
    
    if not symbol:
        possible_pairs = [pair for pair in PAIR_ALIASES.values() if pair in t]
        if possible_pairs:
            symbol = possible_pairs[0]
        else:
            symbol = "EURUSD"

    sl = None
    tp = None
    sl_match = re.search(r'(?:SL|STOP[-_\s]*LOSS|STOP)[:\s]*([0-9.]+)', text, re.IGNORECASE)
    if sl_match:
        try:
            sl = float(sl_match.group(1))
        except:
            pass
    tp_match = re.search(r'(?:TP|TAKE[-_\s]*PROFIT|TAKE)[:\s]*([0-9.]+)', text, re.IGNORECASE)
    if tp_match:
        try:
            tp = float(tp_match.group(1))
        except:
            pass
            
    return {
        "side": side,
        "symbol": symbol,
        "qty": lot_for(symbol),
        "sl": sl,
        "tp": tp,
        "raw": text
    }

def is_valid_trading_account():
    return CT_ACCOUNT_ID != 0 and CT_ACCESS_TOKEN and CT_CLIENT_ID and CT_CLIENT_SECRET

def place_order(client, symbol, side, qty, sl=None, tp=None, raw_signal=None):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    p_cfg = BOT.pairs_config.get(symbol.upper(), {})
    if p_cfg.get("enabled", True) is False:
        BOT.log("WARNING", f"⚠️ Pair {symbol} is DISABLED in settings. Skipping order.")
        return False

    if symbol.upper() in BOT.pairs_config:
        qty = BOT.pairs_config[symbol.upper()]["lot"]

    SYMBOL_IDS = {
        "EURUSD": 1, "GBPUSD": 2, "USDJPY": 3, "AUDUSD": 4, "USDCAD": 5,
        "NZDUSD": 6, "USDCHF": 7, "EURGBP": 8, "EURJPY": 9, "GBPJPY": 10,
        "XAUUSD": 38, "XAGUSD": 39, "BTCUSD": 22, "ETHUSD": 28, "USOIL": 50,
        "NAS100": 10001, "US30": 10002, "SPX500": 10003, "GER40": 10004
    }
    symbol_id = SYMBOL_IDS.get(symbol.upper(), 1)
    volume = int(float(qty) * 1000000)
    trade_side = 1 if side.upper() == "BUY" else 2

    order_id = f"BOT_{int(time.time()*1000)}"
    BOT.log("INFO", f"🚀 Sending order to cTrader OpenAPI: {side} {qty} lots ({volume} units) of {symbol} (ID: {symbol_id})")

    d = client.send(
        "ProtoOANewOrderReq",
        ctidTraderAccountId=CT_ACCOUNT_ID,
        symbolId=symbol_id,
        orderType="MARKET",
        tradeSide=trade_side,
        volume=volume,
        responseTimeoutInSeconds=20
    )

    trade = {
        "orderId": order_id,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "status": "PENDING",
        "sentTime": ts,
        "filledTime": None,
        "closedTime": None,
        "rejectionReason": None,
        "rawSignal": raw_signal or ""
    }
    BOT.trades.appendleft(trade)
    BOT.backend_events.appendleft({"time": ts, "event": f"Order {order_id} sent: {side} {qty} {symbol}", "type": "order"})
    BOT.heartbeat["orders_placed"] += 1

    def on_success(msg):
        msg_str = str(msg)
        close_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if "TOO_MANY_POSITIONS" in msg_str:
            reason = "Open positions limit exceeded (TOO_MANY_POSITIONS)"
            BOT.error(f"Order {order_id} failed: {reason} | Details: {msg_str}")
            trade["status"] = "REJECTED"
            trade["closedTime"] = close_ts
            trade["rejectionReason"] = reason
        elif "SYMBOL_NOT_FOUND" in msg_str:
            reason = f"Symbol not found on cTrader (SYMBOL_NOT_FOUND) for {symbol}"
            BOT.error(f"Order {order_id} failed: {reason} | Details: {msg_str}")
            trade["status"] = "REJECTED"
            trade["closedTime"] = close_ts
            trade["rejectionReason"] = reason
        elif "ERROR" in msg_str.upper() or "REFUSED" in msg_str.upper():
            reason = f"Order refused by broker: {msg_str}"
            BOT.error(f"Order {order_id} failed: {reason}")
            trade["status"] = "REJECTED"
            trade["closedTime"] = close_ts
            trade["rejectionReason"] = reason
        else:
            BOT.log("SUCCESS", f"✅ Order executed successfully: {msg}")
            trade["status"] = "FILLED"
            trade["filledTime"] = close_ts
        save_state()

    def on_error(err):
        err_str = str(err)
        close_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        reason = err_str
        if "TimedOutError" in err_str or "Deferred" in err_str or "convertCancelled" in err_str:
            reason = "cTrader API connection timed out (Deferred timeout)"
            BOT.error(f"Order execution timed out: cTrader server did not respond in time.")
        elif "Trading account is not authorized" in err_str or "INVALID_REQUEST" in err_str:
            reason = "Trading account not authorized or missing TRADING scope in cTrader Access Token"
            BOT.error(f"Order execution failed: {err}")
            BOT.error("⚠️ CRITICAL FIX: Your cTrader OpenAPI Access Token lacks TRADING permissions. Please generate a new Access Token in the cTrader ID Developer Portal with both 'Account Information' and 'Trading' scopes enabled.")
        else:
            BOT.error(f"Order execution failed: {err}")
        trade["status"] = "REJECTED"
        trade["closedTime"] = close_ts
        trade["rejectionReason"] = reason
        save_state()

    d.addCallback(on_success)
    d.addErrback(on_error)
    return True

def get_last_offset():
    if os.path.exists("telegram_offset.txt"):
        try:
            with open("telegram_offset.txt", "r") as f:
                offset = int(f.read().strip() or "0")
                BOT.heartbeat["offset"] = offset
                return offset
        except:
            return 0
    return 0

def save_last_offset(offset):
    try:
        with open("telegram_offset.txt", "w") as f:
            f.write(str(offset))
        BOT.heartbeat["offset"] = offset
    except:
        pass

def check_telegram_history(client):
    """Fetch telegram channel history for the last 30 days"""
    if not TG_TOKEN:
        BOT.log("WARNING", "⚠️ TG_TOKEN not set, skipping Telegram history check")
        BOT.error("TG_TOKEN environment variable is not set!")
        return
    
    try:
        BOT.log("INFO", "📚 Fetching Telegram channel history (last 30 days) - USING NEGATIVE OFFSET...")
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        
        # Use positive offset starting from last known position
        last_offset = get_last_offset()
        params_dict = {"timeout": "30", "limit": "100", "offset": str(last_offset)}
        
        import ssl
        try:
            ssl_context = ssl._create_unverified_context()
        except AttributeError:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        
        all_messages = []
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        offset = get_last_offset()
        max_iterations = 30
        iteration = 0
        
        BOT.log("INFO", "🔄 Starting history fetch with negative offset...")
        
        while iteration < max_iterations:
            iteration += 1
            params_dict["offset"] = str(offset)
            params = urllib.parse.urlencode(params_dict)
            req = urllib.request.Request(f"{url}?{params}", headers={"User-Agent": "Mozilla/5.0"})
            
            try:
                with urllib.request.urlopen(req, timeout=35, context=ssl_context) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    
                    # Update offset for next iteration
                    if data.get("result"):
                        highest_id = max([r.get("update_id", 0) for r in data["result"]])
                        offset = highest_id + 1
                    
                    if not data.get("ok"):
                        BOT.log("WARNING", f"Failed to fetch history: {data.get('description')}")
                        break
                    
                    results = data.get("result", [])
                    if not results:
                        BOT.log("INFO", "✅ Reached end of Telegram history")
                        break
                    
                    messages_batch_count = 0
                    for result in results:
                        msg = result.get("message") or result.get("channel_post")
                        if not msg or "text" not in msg:
                            continue
                        
                        timestamp = msg.get("date", 0)
                        msg_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        
                        if msg_datetime < thirty_days_ago:
                            BOT.log("INFO", "✅ Reached messages older than 30 days")
                            iteration = max_iterations
                            break
                        
                        text = msg["text"]
                        chat_id = msg.get("chat", {}).get("id", "Unknown")
                        
                        if TG_CHAT:
                            try:
                                expected_chat = int(TG_CHAT)
                                if chat_id != expected_chat:
                                    continue
                            except ValueError:
                                pass
                        
                        signal = parse_signal(text)
                        
                        message_entry = {
                            "update_id": result.get("update_id", 0),
                            "timestamp": timestamp,
                            "datetime": msg_datetime.strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "chat_id": chat_id,
                            "text": text,
                            "is_signal": signal is not None,
                            "signal": signal
                        }
                        
                        all_messages.append(message_entry)
                        BOT.all_telegram_messages.append(message_entry)
                        messages_batch_count += 1
                        
                        if signal:
                            BOT.heartbeat["signals_parsed"] += 1
                    
                    BOT.log("INFO", f"📥 Batch {iteration}: Fetched {messages_batch_count} messages")
                    
                    if messages_batch_count == 0:
                        break
                    
                    offset -= 100
                    
            except Exception as e:
                BOT.error(f"Error fetching batch {iteration}: {str(e)}")
                break
        
        BOT.log("INFO", f"📚 Total Telegram messages loaded: {len(all_messages)}")
        BOT.log("INFO", f"💾 Storing all {len(BOT.all_telegram_messages)} messages in memory")
        BOT.heartbeat["messages_received"] = len(BOT.all_telegram_messages)
        
    except Exception as e:
        BOT.error(f"Telegram history fetch failed: {str(e)}")
        import traceback
        BOT.error(f"Full traceback: {traceback.format_exc()}")

def check_telegram(client):
    """Check for new Telegram messages since last check"""
    if not TG_TOKEN:
        BOT.log("WARNING", "⚠️ TG_TOKEN not set, skipping Telegram check")
        BOT.heartbeat["status"] = "error_no_token"
        return
    
    try:
        offset = get_last_offset()
        BOT.log("INFO", f"📱 Checking Telegram updates (offset: {offset})...")
        BOT.heartbeat["status"] = "checking_telegram"
        BOT.heartbeat["last_check"] = datetime.now(timezone.utc).isoformat()
        
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        params_dict = {"timeout": "30"}
        if offset > 0:
            params_dict["offset"] = str(offset)
        params = urllib.parse.urlencode(params_dict)
        req = urllib.request.Request(f"{url}?{params}", headers={"User-Agent": "Mozilla/5.0"})
        
        import ssl
        try:
            ssl_context = ssl._create_unverified_context()
        except AttributeError:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        
        try:
            with urllib.request.urlopen(req, timeout=35, context=ssl_context) as resp:
                raw_response = resp.read().decode('utf-8')
                BOT.log('DEBUG', f'Telegram API raw response: {raw_response}')
                data = json.loads(raw_response)
        except urllib.error.HTTPError as e:
            BOT.log('ERROR', f'Telegram API HTTP error: {e.code} - {e.reason}')
            try:
                error_response = e.read().decode('utf-8')
                BOT.log('DEBUG', f'Telegram API error response: {error_response}')
            except Exception:
                pass
            return
        
        if data.get("ok"):
            results = data.get("result", [])
            BOT.log("INFO", f"📥 Received {len(results)} Telegram update(s)")
            
            if not results:
                BOT.log("INFO", "✅ No new messages (up to date)")
                BOT.heartbeat["status"] = "ok_no_new_messages"
                save_state()
                return
            
            highest_update_id = offset - 1 if offset > 0 else -1
            
            for result in results:
                update_id = result.get("update_id", 0)
                highest_update_id = max(highest_update_id, update_id)
                
                msg = result.get("message") or result.get("channel_post")
                
                if not msg or "text" not in msg:
                    BOT.log("DEBUG", f"Update {update_id}: No text message, skipping")
                    continue
                
                text = msg["text"]
                chat_id = msg.get("chat", {}).get("id", "Unknown")
                timestamp = msg.get("date", 0)
                msg_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
                if TG_CHAT:
                    try:
                        expected_chat = int(TG_CHAT)
                        if chat_id != expected_chat:
                            BOT.log("DEBUG", f"Message from chat {chat_id} (expected {expected_chat}), skipping")
                            continue
                    except ValueError:
                        BOT.log("WARNING", f"TG_CHAT is not a valid number: {TG_CHAT}")
                
                BOT.log("INFO", f"💬 Received message from chat {chat_id}: '{text}'")
                BOT.heartbeat["messages_received"] += 1
                
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                signal = parse_signal(text)
                
                message_entry = {
                    "update_id": update_id,
                    "timestamp": timestamp,
                    "datetime": msg_datetime.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "chat_id": chat_id,
                    "text": text,
                    "is_signal": signal is not None,
                    "signal": signal
                }
                BOT.all_telegram_messages.append(message_entry)
                
                signal_entry = {
                    "time": ts,
                    "text": text,
                    "parsed": signal is not None,
                    "signal": signal
                }
                BOT.signals.appendleft(signal_entry)
                BOT.backend_events.appendleft({"time": ts, "event": f"Signal received: {text[:50]}...", "type": "signal"})
                
                if signal:
                    BOT.log("INFO", f"📨 Signal: {signal['side']} {signal['qty']} {signal['symbol']}")
                    BOT.heartbeat["signals_parsed"] += 1
                    place_order(client, signal['symbol'], signal['side'], signal['qty'], signal['sl'], signal['tp'], text)
                else:
                    BOT.log("INFO", f"📝 Message did not match signal format: '{text}'")
            
            if highest_update_id >= 0:
                new_offset = highest_update_id + 1
                save_last_offset(new_offset)
                BOT.log("INFO", f"📤 Updated offset to {new_offset}")
                
                # Also update in-memory offset for immediate use
                offset = new_offset
            
            BOT.heartbeat["status"] = "ok_messages_processed"
            save_state()
        else:
            error_msg = f"Telegram API error: {data.get('description', 'Unknown error')}"
            BOT.error(error_msg)
            BOT.heartbeat["status"] = "error_api"
    except Exception as e:
        error_msg = f"Telegram check failed: {str(e)}"
        BOT.error(error_msg)
        BOT.heartbeat["status"] = "error_exception"
        import traceback
        BOT.error(f"Full traceback: {traceback.format_exc()}")

def save_state():
    try:
        os.makedirs("docs", exist_ok=True)
        state = {
            "connected": BOT.connected,
            "loggedIn": BOT.logged_in,
            "heartbeat": BOT.heartbeat,
            "account": BOT.account,
            "trades": list(BOT.trades),
            "signals": list(BOT.signals),
            "logs": list(BOT.logs),
            "errors": list(BOT.errors),
            "backendEvents": list(BOT.backend_events),
            "allTelegramMessages": list(BOT.all_telegram_messages),
            "telegramMessageCount": len(BOT.all_telegram_messages),
            "pairsConfig": BOT.pairs_config,
            "lastUpdate": datetime.now(timezone.utc).isoformat()
        }
        with open("docs/system_state.json", "w") as f:
            json.dump(state, f, indent=2)
        BOT.log("INFO", "💾 State saved to docs/system_state.json")
        
        try:
            with open("docs/telegram_history.json", "w") as f:
                telegram_history = {
                    "total_messages": len(BOT.all_telegram_messages),
                    "messages": list(BOT.all_telegram_messages),
                    "last_update": datetime.now(timezone.utc).isoformat()
                }
                json.dump(telegram_history, f, indent=2)
            BOT.log("INFO", f"📚 Telegram history saved ({len(BOT.all_telegram_messages)} messages)")
        except Exception as e:
            BOT.error(f"Failed to save telegram history: {e}")
    except Exception as e:
        BOT.error(f"Failed to save state: {e}")

def main(single_run=False):
    BOT.log("INFO", "=" * 80)
    BOT.log("INFO", "🤖 cTrader OpenAPI Bot STARTING")
    BOT.log("INFO", "=" * 80)

    # Check if running in single-run mode and warn user
    if single_run:
        BOT.log("WARNING", "⚠️ Running in SINGLE RUN mode - bot will exit after one execution cycle. Set SINGLE_RUN=false for continuous operation")
    if not CT_CLIENT_ID or not CT_ACCESS_TOKEN or not CT_ACCOUNT_ID:
        BOT.error("Missing cTrader OpenAPI credentials (CT_CLIENT_ID, CT_ACCESS_TOKEN, CT_ACCOUNT_ID)")
        BOT.heartbeat["status"] = "error_missing_credentials"
        return False

    if not is_valid_trading_account():
        BOT.error("Invalid trading account configuration. Please check your environment variables.")
        BOT.heartbeat["status"] = "error_invalid_account"
        return False

    client = Client(HOST, PORT, TcpProtocol)
    
    app_auth_timeout = 30 if not single_run else 60
    acc_auth_timeout = 30 if not single_run else 60
    safety_timeout = 300 if not single_run else 180

    def on_connected(client):
        BOT.connected = True
        BOT.log("SUCCESS", f"✅ Connected to cTrader OpenAPI at {HOST}:{PORT}")

        d = client.send("ProtoOAApplicationAuthReq", clientId=CT_CLIENT_ID, clientSecret=CT_CLIENT_SECRET, responseTimeoutInSeconds=app_auth_timeout)
        
        def on_app_auth(msg):
            BOT.log("SUCCESS", "🔐 Application Authorized successfully")

            d2 = client.send("ProtoOAAccountAuthReq", ctidTraderAccountId=CT_ACCOUNT_ID, accessToken=CT_ACCESS_TOKEN, responseTimeoutInSeconds=acc_auth_timeout)
            
            def on_acc_auth(acc_msg):
                BOT.logged_in = True
                BOT.log("SUCCESS", "🔐 Account Authorized successfully")

                # Fetch history if offset is 0 or file doesn't exist
                current_offset = get_last_offset()
                if current_offset == 0:
                    BOT.log("INFO", "🔄 Resetting history - fetching all messages from start...")
                    if os.path.exists("telegram_offset.txt"):
                        os.remove("telegram_offset.txt")
                        save_last_offset(0)
                    check_telegram_history(client)
                
                # Check for new messages and pending signals file
                # Only run periodic checks if not in single-run mode
                if not single_run:
                    BOT.log("INFO", "🔄 Starting periodic Telegram checks (30-second interval)")
                check_telegram(client)
                BOT.load_pending_signals_file(client)
                save_state()
                BOT.cycle_completed = True
                
                if single_run:
                    BOT.log("INFO", "✅ Single run mode completed. Waiting 20 seconds for order responses before exiting... (Set SINGLE_RUN=false for continuous operation)")
                    reactor.callLater(20, lambda: reactor.stop() if reactor.running else None)
                else:
                    def periodic_telegram_check():
                        if BOT.connected and BOT.logged_in:
                            check_telegram(client)
                            save_state()
                            reactor.callLater(30, periodic_telegram_check)
                    
                    reactor.callLater(30, periodic_telegram_check)
                
            def on_acc_auth_err(err):
                err_str = str(err)
                BOT.heartbeat["status"] = "error_account_auth"
                if "TimeoutError" in err_str or "Deferred" in err_str:
                    BOT.error("Account auth timed out. This may be due to network issues or cTrader server delays.")
                else:
                    BOT.error(f"Account auth failed: {err}")
                reactor.callLater(1, reactor.stop)
            
            d2.addCallback(on_acc_auth)
            d2.addErrback(on_acc_auth_err)
        
        def on_app_auth_err(err):
            err_str = str(err)
            BOT.heartbeat["status"] = "error_app_auth"
            if "TimeoutError" in err_str or "Deferred" in err_str:
                BOT.error("Application auth timed out. This may be due to network issues or cTrader server delays.")
            else:
                BOT.error(f"App auth failed: {err}")
            reactor.callLater(1, reactor.stop)
        
        d.addCallback(on_app_auth)
        d.addErrback(on_app_auth_err)

    def on_disconnected(client, reason):
        BOT.connected = False
        BOT.heartbeat["status"] = "disconnected"
        if reactor.running:
            BOT.log("WARNING", f"Disconnected: {reason}")
        if reactor.running:
            reactor.stop()

    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.startService()

    reactor.callLater(safety_timeout, lambda: reactor.stop() if reactor.running else None)

    reactor.run()
    BOT.heartbeat["status"] = "completed"
    return BOT.logged_in

if __name__ == "__main__":
    single_run_mode = os.environ.get("SINGLE_RUN", "").lower() in ("true", "1", "yes", "on")
    
    if single_run_mode:
        BOT.log("INFO", "Running in SINGLE RUN mode (suitable for CI/CD)")
    
    success = main(single_run=single_run_mode)
    if single_run_mode:
        sys.exit(0 if (success or getattr(BOT, 'cycle_completed', False) or BOT.logged_in) else 1)
    else:
        sys.exit(0 if success else 1)

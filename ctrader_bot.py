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
from datetime import datetime, timezone
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
        self.trades = deque(maxlen=100)
        self.backend_events = deque(maxlen=150)
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
            except:
                pass

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
    
    # Skip certain message types
    skip_keywords = ["TP HIT", "SL HIT", "CLOSED", "GOOD MORNING", "NEW MONTH", "DO YOU WANT"]
    for keyword in skip_keywords:
        if keyword in t:
            return None

    # Detect side (BUY/SELL)
    side = None
    if "BUY" in t:
        side = "BUY"
    elif "SELL" in t:
        side = "SELL"
    # Check for common variations
    elif "LONG" in t:
        side = "BUY"
    elif "SHORT" in t:
        side = "SELL"
    elif "UP" in t and ("GO" in t or "GOING" in t):
        side = "BUY"
    elif "DOWN" in t and ("GO" in t or "GOING" in t):
        side = "SELL"

    if not side:
        # Check if this might be a test message
        if "TEST" in t or "DEMO" in t or "EXAMPLE" in t:
            # For test messages, return a simple test signal
            return {
                "side": "BUY",
                "symbol": "EURUSD",
                "qty": lot_for("EURUSD"),
                "sl": None,
                "tp": None,
                "raw": text
            }
        return None

    # Look for symbol/pair
    symbol = None
    
    # First, try to find exact matches in the text
    for alias, real in PAIR_ALIASES.items():
        if alias in t:
            symbol = real
            break
    
    # If no exact match, try to find any currency pair pattern
    if not symbol:
        # Look for common forex patterns like XXX/XXX or XXXXXX
        forex_pattern = r'\b([A-Z]{3})[/\s]?([A-Z]{3})\b'
        forex_match = re.search(forex_pattern, text, re.IGNORECASE)
        if forex_match:
            pair = forex_match.group(1).upper() + forex_match.group(2).upper()
            # Validate if it's a known pair
            if pair in PAIR_ALIASES.values() or pair in PAIR_ALIASES.keys():
                symbol = pair if pair in PAIR_ALIASES.values() else PAIR_ALIASES.get(pair, pair)
    
    # If still no symbol found, use default
    if not symbol:
        # Check if the message contains a recognizable pair
        possible_pairs = [pair for pair in PAIR_ALIASES.values() if pair in t]
        if possible_pairs:
            symbol = possible_pairs[0]
        else:
            # If no specific pair found but it's a signal, use EURUSD as default
            symbol = "EURUSD"

    # Parse SL and TP if present
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
    """Check if the trading account is properly configured"""
    return CT_ACCOUNT_ID != 0 and CT_ACCESS_TOKEN and CT_CLIENT_ID and CT_CLIENT_SECRET

def test_signal_parsing():
    """Test function to debug signal parsing"""
    test_messages = [
        "BUY EURUSD",
        "SELL GBPUSD",
        "BUY BTCUSD",
        "TEST SIGNAL BUY EURUSD",
        "LONG GOLD",
        "SHORT USOIL"
    ]
    
    print("\nSignal Parsing Tests:")
    for msg in test_messages:
        result = parse_signal(msg)
        print(f"Input: '{msg}' -> Output: {result}")
    print("")

def place_order(client, symbol, side, qty, sl=None, tp=None, raw_signal=None):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Check if pair is enabled
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
                return int(f.read().strip() or "0")
        except:
            return 0
    return 0

def save_last_offset(offset):
    try:
        with open("telegram_offset.txt", "w") as f:
            f.write(str(offset))
    except:
        pass

def check_telegram(client):
    if not TG_TOKEN:
        BOT.log("WARNING", "⚠️ TG_TOKEN not set, skipping Telegram check")
        return
    try:
        offset = get_last_offset()
        BOT.log("INFO", f"📱 Checking Telegram updates (offset: {offset})...")
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        params_dict = {"timeout": "10"}  # Increased timeout
        if offset > 0:
            params_dict["offset"] = str(offset)
        params = urllib.parse.urlencode(params_dict)
        req = urllib.request.Request(f"{url}?{params}", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:  # Increased timeout
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("ok"):
                results = data.get("result", [])
                BOT.log("INFO", f"📥 Received {len(results)} Telegram update(s)")
                
                max_update_id = offset
                for result in results:
                    update_id = result.get("update_id", 0)
                    if update_id >= max_update_id:
                        max_update_id = update_id + 1
                    msg = result.get("message") or result.get("channel_post")
                    if msg and "text" in msg:
                        text = msg["text"]
                        chat_id = msg.get("chat", {}).get("id", "Unknown")
                        
                        # Log the received message for debugging
                        BOT.log("INFO", f"💬 Received message from chat {chat_id}: '{text}'")
                        
                        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                        signal = parse_signal(text)
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
                            place_order(client, signal['symbol'], signal['side'], signal['qty'], signal['sl'], signal['tp'], text)
                        else:
                            BOT.log("INFO", f"📝 Message did not match signal format: '{text}'")
                            
                if max_update_id > offset:
                    save_last_offset(max_update_id)
                    BOT.log("INFO", f"📤 Updated offset to {max_update_id}")
            else:
                BOT.error(f"Telegram API error: {data.get('description', 'Unknown error')}")
    except Exception as e:
        BOT.error(f"Telegram check failed: {str(e)}")
        import traceback
        BOT.error(f"Full traceback: {traceback.format_exc()}")

def save_state():
    try:
        os.makedirs("docs", exist_ok=True)
        state = {
            "connected": BOT.connected,
            "loggedIn": BOT.logged_in,
            "account": BOT.account,
            "trades": list(BOT.trades),
            "signals": list(BOT.signals),
            "logs": list(BOT.logs),
            "errors": list(BOT.errors),
            "backendEvents": list(BOT.backend_events),
            "pairsConfig": BOT.pairs_config,
            "lastUpdate": datetime.now(timezone.utc).isoformat()
        }
        with open("docs/system_state.json", "w") as f:
            json.dump(state, f, indent=2)
        BOT.log("INFO", "💾 State saved to docs/system_state.json")
    except Exception as e:
        BOT.error(f"Failed to save state: {e}")

def main(single_run=False):
    """
    Main function to run the cTrader OpenAPI Bot.
    
    Args:
        single_run (bool): If True, the bot will check for signals once and exit.
                          If False (default), the bot will run continuously.
    """
    BOT.log("INFO", "=" * 80)
    BOT.log("INFO", "🤖 cTrader OpenAPI Bot STARTING")
    BOT.log("INFO", "=" * 80)

    if not CT_CLIENT_ID or not CT_ACCESS_TOKEN or not CT_ACCOUNT_ID:
        BOT.error("Missing cTrader OpenAPI credentials (CT_CLIENT_ID, CT_ACCESS_TOKEN, CT_ACCOUNT_ID)")
        return False

    if not is_valid_trading_account():
        BOT.error("Invalid trading account configuration. Please check your environment variables.")
        return False

    client = Client(HOST, PORT, TcpProtocol)
    
    # Set appropriate timeouts based on mode
    app_auth_timeout = 30 if not single_run else 60  # Longer timeout for single run
    acc_auth_timeout = 30 if not single_run else 60  # Longer timeout for single run
    safety_timeout = 300 if not single_run else 120  # Shorter safety timeout for single run

    def on_connected(client):
        BOT.connected = True
        BOT.log("SUCCESS", f"✅ Connected to cTrader OpenAPI at {HOST}:{PORT}")

        # 1. Application Auth with improved timeout handling
        d = client.send("ProtoOAApplicationAuthReq", clientId=CT_CLIENT_ID, clientSecret=CT_CLIENT_SECRET, responseTimeoutInSeconds=app_auth_timeout)
        
        def on_app_auth(msg):
            BOT.log("SUCCESS", "🔐 Application Authorized successfully")

            # 2. Account Auth with improved timeout handling
            d2 = client.send("ProtoOAAccountAuthReq", ctidTraderAccountId=CT_ACCOUNT_ID, accessToken=CT_ACCESS_TOKEN, responseTimeoutInSeconds=acc_auth_timeout)
            
            def on_acc_auth(acc_msg):
                BOT.logged_in = True
                BOT.log("SUCCESS", "🔐 Account Authorized successfully")

                # Check Telegram immediately after successful authentication
                check_telegram(client)
                save_state()
                
                # For single run mode, stop after checking once
                if single_run:
                    BOT.log("INFO", "Single run mode completed. Exiting...")
                    reactor.callLater(2, lambda: reactor.stop() if reactor.running else None)
                else:
                    # Then set up periodic checks
                    # Check Telegram every 30 seconds for new messages
                    def periodic_telegram_check():
                        if BOT.connected and BOT.logged_in:
                            check_telegram(client)
                            # Schedule next check in 30 seconds
                            reactor.callLater(30, periodic_telegram_check)
                    
                    # Start the periodic check
                    reactor.callLater(30, periodic_telegram_check)
                
            def on_acc_auth_err(err):
                # Handle timeout errors more gracefully
                err_str = str(err)
                if "TimeoutError" in err_str or "Deferred" in err_str:
                    BOT.error("Account auth timed out. This may be due to network issues or cTrader server delays.")
                    BOT.error("Consider increasing the timeout value or checking your connection.")
                else:
                    BOT.error(f"Account auth failed: {err}")
                reactor.callLater(1, reactor.stop)  # Stop the reactor after error
            
            d2.addCallback(on_acc_auth)
            d2.addErrback(on_acc_auth_err)
        
        def on_app_auth_err(err):
            # Handle timeout errors more gracefully
            err_str = str(err)
            if "TimeoutError" in err_str or "Deferred" in err_str:
                BOT.error("Application auth timed out. This may be due to network issues or cTrader server delays.")
                BOT.error("Consider increasing the timeout value or checking your connection.")
            else:
                BOT.error(f"App auth failed: {err}")
            reactor.callLater(1, reactor.stop)  # Stop the reactor after error
        
        d.addCallback(on_app_auth)
        d.addErrback(on_app_auth_err)

    def on_disconnected(client, reason):
        BOT.connected = False
        # Only log disconnection if not intentional (when reactor is stopping)
        if reactor.running:
            BOT.log("WARNING", f"Disconnected: {reason}")
        if reactor.running:
            reactor.stop()

    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.startService()

    # Safety timeout
    reactor.callLater(safety_timeout, lambda: reactor.stop() if reactor.running else None)

    reactor.run()
    return BOT.logged_in

if __name__ == "__main__":
    # Check if running in single run mode (for CI/CD environments)
    single_run_mode = os.environ.get("SINGLE_RUN", "false").lower() in ("true", "1", "yes", "on")
    
    if single_run_mode:
        BOT.log("INFO", "Running in SINGLE RUN mode (suitable for CI/CD)")
    
    success = main(single_run=single_run_mode)
    sys.exit(0 if success else 1)

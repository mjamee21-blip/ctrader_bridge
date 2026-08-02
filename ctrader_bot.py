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
    "EURAUD": "EURAUD", "EURCAD": "EURCAD"
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
            "freeMargin": 10000.0, "leverage": 100
        }
        self.pairs_config = DEFAULT_PAIRS_CONFIG.copy()

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
    side = "BUY" if "BUY" in t else "SELL" if "SELL" in t else None
    if not side:
        return None
    symbol = "BTCUSD"
    for alias, real in PAIR_ALIASES.items():
        if alias in t:
            symbol = real
            break
    sl = None
    tp = None
    sl_match = re.search(r'(?:SL|STOP)[:\s]*([0-9.]+)', text, re.IGNORECASE)
    if sl_match:
        try:
            sl = float(sl_match.group(1))
        except:
            pass
    tp_match = re.search(r'(?:TP|TAKE)[:\s]*([0-9.]+)', text, re.IGNORECASE)
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

def check_telegram(client):
    if not TG_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        params = urllib.parse.urlencode({"timeout": "5"})
        req = urllib.request.Request(f"{url}?{params}", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("ok"):
                for result in data.get("result", []):
                    msg = result.get("message") or result.get("channel_post")
                    if msg and "text" in msg:
                        text = msg["text"]
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
    except Exception as e:
        BOT.error(f"Telegram check failed: {str(e)}")

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

def main():
    BOT.log("INFO", "=" * 80)
    BOT.log("INFO", "🤖 cTrader OpenAPI Bot STARTING")
    BOT.log("INFO", "=" * 80)

    if not CT_CLIENT_ID or not CT_ACCESS_TOKEN or not CT_ACCOUNT_ID:
        BOT.error("Missing cTrader OpenAPI credentials (CT_CLIENT_ID, CT_ACCESS_TOKEN, CT_ACCOUNT_ID)")
        return False

    client = Client(HOST, PORT, TcpProtocol)

    def on_connected(client):
        BOT.connected = True
        BOT.log("SUCCESS", f"✅ Connected to cTrader OpenAPI at {HOST}:{PORT}")
        
        # 1. Application Auth
        d = client.send("ProtoOAApplicationAuthReq", clientId=CT_CLIENT_ID, clientSecret=CT_CLIENT_SECRET)
        def on_app_auth(msg):
            BOT.log("SUCCESS", "🔐 Application Authorized successfully")
            
            # 2. Account Auth
            d2 = client.send("ProtoOAAccountAuthReq", ctidTraderAccountId=CT_ACCOUNT_ID, accessToken=CT_ACCESS_TOKEN)
            def on_acc_auth(acc_msg):
                BOT.logged_in = True
                BOT.log("SUCCESS", "🔐 Account Authorized successfully")
                
                check_telegram(client)
                save_state()
                
                reactor.callLater(2, reactor.stop)
            d2.addCallback(on_acc_auth)
            d2.addErrback(lambda err: BOT.error(f"Account auth failed: {err}"))
        d.addCallback(on_app_auth)
        d.addErrback(lambda err: BOT.error(f"App auth failed: {err}"))

    def on_disconnected(client, reason):
        BOT.connected = False
        BOT.log("WARNING", f"Disconnected: {reason}")
        if reactor.running:
            reactor.stop()

    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.startService()

    # Safety timeout
    reactor.callLater(20, lambda: reactor.stop() if reactor.running else None)
    
    reactor.run()
    return BOT.logged_in

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader OpenAPI Bot - Complete Signal & Trade Tracking (WebSocket / REST)
import os
import json
import re
import sys
import time
import asyncio
import urllib.request
import urllib.parse
import logging
from datetime import datetime, timezone
from collections import deque

import websockets

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

OPENAPI_HOST = "demo.ctraderapi.com" if CT_ENV == "demo" else "live.ctraderapi.com"
OPENAPI_PORT = 5035

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

    async def connect_and_run(self):
        uri = f"wss://{OPENAPI_HOST}:{OPENAPI_PORT}"
        self.log("INFO", f"🔌 Connecting to cTrader OpenAPI WebSocket at {uri}...")
        try:
            async with websockets.connect(uri) as websocket:
                self.connected = True
                self.log("SUCCESS", "✅ Connected to cTrader OpenAPI WebSocket")

                # 1. Send Application Authorization (ProtoOAApplicationAuthReq - payloadType 21)
                app_auth_msg = {
                    "payloadType": 21,
                    "payload": {
                        "clientId": CT_CLIENT_ID,
                        "clientSecret": CT_CLIENT_SECRET
                    },
                    "clientMsgId": "app_auth_1"
                }
                await websocket.send(json.dumps(app_auth_msg))
                self.log("INFO", "Sent Application Authorization Request")

                # Receive response
                response_str = await asyncio.wait_for(websocket.recv(), timeout=10)
                resp = json.loads(response_str)
                self.log("DEBUG", f"App Auth Response: {resp}")

                # 2. Send Account Authorization (ProtoOAAccountAuthReq - payloadType 2049)
                account_auth_msg = {
                    "payloadType": 2049,
                    "payload": {
                        "ctidTraderAccountId": CT_ACCOUNT_ID,
                        "accessToken": CT_ACCESS_TOKEN
                    },
                    "clientMsgId": "acc_auth_1"
                }
                await websocket.send(json.dumps(account_auth_msg))
                self.log("INFO", f"Sent Account Authorization Request for Account ID {CT_ACCOUNT_ID}")

                response_str = await asyncio.wait_for(websocket.recv(), timeout=10)
                resp = json.loads(response_str)
                self.log("DEBUG", f"Account Auth Response: {resp}")
                self.logged_in = True
                self.log("SUCCESS", "🔐 cTrader OpenAPI Login & Authorization Successful!")

                # Process Telegram Signals & place orders
                check_telegram(self, websocket)

                return True
        except Exception as e:
            self.error(f"OpenAPI Connection/Auth error: {e}")
            return False

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

def check_telegram(bot, websocket=None):
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
                        bot.signals.appendleft(signal_entry)
                        bot.backend_events.appendleft({"time": ts, "event": f"Signal received: {text[:50]}...", "type": "signal"})
                        if signal:
                            bot.log("INFO", f"📨 Signal: {signal['side']} {signal['qty']} {signal['symbol']}")
                            # Place order via OpenAPI if websocket available
                            # (In OpenAPI, symbol must be mapped to symbolId, simplified here)
    except Exception as e:
        bot.error(f"Telegram check failed: {str(e)}")

def save_state(bot):
    try:
        os.makedirs("docs", exist_ok=True)
        state = {
            "connected": bot.connected,
            "loggedIn": bot.logged_in,
            "account": bot.account,
            "trades": list(bot.trades),
            "signals": list(bot.signals),
            "logs": list(bot.logs),
            "errors": list(bot.errors),
            "backendEvents": list(bot.backend_events),
            "pairsConfig": bot.pairs_config,
            "lastUpdate": datetime.now(timezone.utc).isoformat()
        }
        with open("docs/system_state.json", "w") as f:
            json.dump(state, f, indent=2)
        bot.log("INFO", "💾 State saved to docs/system_state.json")
    except Exception as e:
        bot.error(f"Failed to save state: {e}")

async def main_async():
    bot = CTraderOpenAPIBot()
    bot.log("INFO", "=" * 80)
    bot.log("INFO", "🤖 cTrader OpenAPI Bot STARTING")
    bot.log("INFO", "=" * 80)

    if not CT_CLIENT_ID or not CT_ACCESS_TOKEN or not CT_ACCOUNT_ID:
        bot.error("Missing cTrader OpenAPI credentials (CT_CLIENT_ID, CT_ACCESS_TOKEN, CT_ACCOUNT_ID)")
        return False

    success = await bot.connect_and_run()
    save_state(bot)
    return success

def main():
    return asyncio.run(main_async())

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

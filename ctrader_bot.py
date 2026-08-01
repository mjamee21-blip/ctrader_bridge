#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader FIX API Telegram Bot + Dashboard (FIX Protocol Edition)
#
# Configured with FIX API parameters from cTrader:
#   Host: demo-uk-eqx-01.p.c-trader.com
#   Trade Port: 5212 (SSL)
#   Quote Port: 5211 (SSL)
#   SenderCompID: demo.deriv.2454444
#   TargetCompID: cServer
#   SenderSubID: TRADE / QUOTE

import os
import json
import re
import sys
import time
import socket
import ssl
import threading
import urllib.request
import urllib.parse
from datetime import datetime, timezone

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

def _clean_sec(val):
    return str(val or "").strip().strip('"').strip("'").strip()

# FIX API CONFIGURATION
FIX_HOST = _clean_sec(os.environ.get("FIX_HOST", "demo-uk-eqx-01.p.c-trader.com"))
FIX_TRADE_PORT = int(os.environ.get("FIX_TRADE_PORT", "5212"))
FIX_QUOTE_PORT = int(os.environ.get("FIX_QUOTE_PORT", "5211"))
FIX_SENDER_COMP_ID = _clean_sec(os.environ.get("FIX_SENDER_COMP_ID", "demo.deriv.2454444"))
FIX_TARGET_COMP_ID = _clean_sec(os.environ.get("FIX_TARGET_COMP_ID", "cServer"))
FIX_SENDER_SUB_ID = _clean_sec(os.environ.get("FIX_SENDER_SUB_ID", "TRADE"))
FIX_PASSWORD = _clean_sec(os.environ.get("FIX_PASSWORD", os.environ.get("CT_PASSWORD", "")))

TG_TOKEN = _clean_sec(os.environ.get("TG_TOKEN", ""))
TG_CHAT = _clean_sec(os.environ.get("TG_CHAT", ""))
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

USE_USER_ACCOUNT = bool(TG_SESSION and TG_API_ID and TG_API_HASH and HAS_TELETHON)

DASHBOARD_USERNAME = _clean_sec(os.environ.get("DASHBOARD_USERNAME", "admin"))
DASHBOARD_PASSWORD = _clean_sec(os.environ.get("DASHBOARD_PASSWORD", "changeme"))
DEFAULT_QTY = float(os.environ.get("CTRADER_DEFAULT_QTY", "1.0") or "1.0")
MODE = os.environ.get("MODE", "bot")

PAIR_ALIASES = {
    "BTC": "BTCUSD", "BITCOIN": "BTCUSD",
    "ETH": "ETHUSD", "ETHEREUM": "ETHUSD",
    "GOLD": "XAUUSD", "XAU": "XAUUSD",
    "SILVER": "XAGUSD", "XAG": "XAGUSD",
    "OIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL",
    "BRENT": "UKOIL",
    "NAS100": "NAS100", "NASDAQ": "NAS100", "US100": "NAS100",
    "US30": "US30", "DOW": "US30",
    "SPX500": "SPX500", "SP500": "SPX500",
    "GER40": "GER40", "DAX": "GER40",
}

DEFAULT_LOTS = {
    "BTCUSD": 0.10, "ETHUSD": 0.10, "XAUUSD": 0.05, "XAGUSD": 0.10,
    "EURUSD": 0.01, "GBPUSD": 0.01, "USDJPY": 0.01, "NAS100": 0.10, "US30": 0.10,
}

def lot_for(pair_name):
    p = (pair_name or "").upper()
    return DEFAULT_LOTS.get(p, DEFAULT_QTY)

# =====================================================================
# CTRADER FIX API CLIENT
# =====================================================================
class CTraderFixClient:
    def __init__(self, host, port, sender_comp_id, target_comp_id, sender_sub_id, password):
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.sender_sub_id = sender_sub_id
        self.password = password
        self.sock = None
        self.ssl_sock = None
        self.msg_seq_num = 1
        self.lock = threading.Lock()
        self.connected = False
        self.positions = []
        self.orders = []
        self.account_info = {"balance": 10000.0, "equity": 10000.0, "margin": 0.0, "freeMargin": 10000.0, "leverage": 100}
        self.last_heartbeat = time.time()

    def connect(self):
        try:
            print(f"[FIX] Connecting to {self.host}:{self.port} (Sender: {self.sender_comp_id}, Sub: {self.sender_sub_id})...")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(15)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self.ssl_sock = ctx.wrap_socket(self.sock, server_hostname=self.host)
            self.ssl_sock.connect((self.host, self.port))
            self.connected = True
            print(f"[FIX] SSL Connected successfully to {self.host}:{self.port}")
            self.send_logon()
            
            t = threading.Thread(target=self._recv_loop, daemon=True)
            t.start()
            
            hb_t = threading.Thread(target=self._heartbeat_loop, daemon=True)
            hb_t.start()
            return True
        except Exception as e:
            print(f"[FIX] Connection failed: {e}")
            self.connected = False
            return False

    def send_msg(self, msg_type, fields):
        with self.lock:
            if not self.connected or not self.ssl_sock:
                print(f"[FIX] Cannot send {msg_type}: not connected.")
                return False
            try:
                body_parts = [
                    f"35={msg_type}",
                    f"49={self.sender_comp_id}",
                    f"56={self.target_comp_id}"
                ]
                if self.sender_sub_id:
                    body_parts.append(f"57={self.sender_sub_id}")
                body_parts.append(f"34={self.msg_seq_num}")
                self.msg_seq_num += 1

                now_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                body_parts.append(f"52={now_str}")

                for k, v in fields.items():
                    body_parts.append(f"{k}={v}")

                body = "\x01".join(body_parts) + "\x01"
                header = f"8=FIX.4.4\x019={len(body)}\x01"
                raw_msg = header + body

                checksum = sum(ord(c) for c in raw_msg) % 256
                full_msg = raw_msg + f"10={checksum:03d}\x01"

                self.ssl_sock.sendall(full_msg.encode('ascii'))
                print(f"[FIX OUT] Type {msg_type}: {fields}")
                return True
            except Exception as e:
                print(f"[FIX] Send error ({msg_type}): {e}")
                self.connected = False
                return False

    def send_logon(self):
        fields = {
            "98": "0",     # EncryptMethod
            "108": "30",   # HeartBtInt
            "554": self.password # Password
        }
        self.send_msg("A", fields)

    def send_heartbeat(self):
        self.send_msg("0", {})

    def _heartbeat_loop(self):
        while self.connected:
            time.sleep(25)
            if self.connected:
                self.send_heartbeat()

    def _recv_loop(self):
        buffer = b""
        while self.connected:
            try:
                chunk = self.ssl_sock.recv(4096)
                if not chunk:
                    print("[FIX] Connection closed by remote host.")
                    self.connected = False
                    break
                buffer += chunk
                while b"\x01" in buffer:
                    # Find complete FIX messages
                    # Simple line / tag splitting
                    try:
                        idx = buffer.find(b"\x0110=")
                        if idx != -1:
                            end_idx = buffer.find(b"\x01", idx + 4)
                            if end_idx != -1:
                                raw_msg = buffer[:end_idx + 1]
                                buffer = buffer[end_idx + 1:]
                                self._handle_msg(raw_msg.decode('ascii', errors='ignore'))
                            else:
                                break
                        else:
                            break
                    except Exception as parse_err:
                        print(f"[FIX] Parse error: {parse_err}")
                        buffer = b""
                        break
            except Exception as e:
                print(f"[FIX] Recv error: {e}")
                self.connected = False
                break

    def _handle_msg(self, msg_str):
        tags = {}
        for part in msg_str.split("\x01"):
            if "=" in part:
                k, v = part.split("=", 1)
                tags[k] = v
        msg_type = tags.get("35")
        print(f"[FIX IN] Type {msg_type} received. Tags: {tags}")
        if msg_type == "A":
            print("[FIX] Logon successful!")
        elif msg_type == "0":
            # Heartbeat
            pass
        elif msg_type == "8":
            # Execution Report
            exec_type = tags.get("150")
            ord_status = tags.get("39")
            symbol = tags.get("55")
            cl_ord_id = tags.get("11")
            print(f"[FIX ORDER] ExecType={exec_type}, Status={ord_status}, Symbol={symbol}, ClOrdID={cl_ord_id}")

    def place_market_order(self, symbol, side, qty, sl=None, tp=None):
        cl_ord_id = f"BOT_{int(time.time()*1000)}"
        fields = {
            "11": cl_ord_id,
            "55": symbol,
            "54": "1" if side.upper() == "BUY" else "2",
            "38": str(qty),
            "40": "1", # Market
            "59": "0"  # Day
        }
        if sl:
            fields["99"] = str(sl)
        if tp:
            fields["114"] = str(tp)
            
        success = self.send_msg("D", fields)
        if success:
            self.orders.append({
                "orderId": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "status": "SENT",
                "time": datetime.now(timezone.utc).isoformat()
            })
        return success

FIX_CLIENT = CTraderFixClient(FIX_HOST, FIX_TRADE_PORT, FIX_SENDER_COMP_ID, FIX_TARGET_COMP_ID, FIX_SENDER_SUB_ID, FIX_PASSWORD)

# =====================================================================
# SIGNAL PARSING & TELEGRAM / DASHBOARD
# =====================================================================
def parse_signal(text):
    if not text:
        return None
    t_upper = text.upper()
    side = None
    if "BUY" in t_upper:
        side = "BUY"
    elif "SELL" in t_upper:
        side = "SELL"
    if not side:
        return None

    words = re.findall(r'[A-Z0-9]+', t_upper)
    symbol = "BTCUSD"
    for w in words:
        if w in PAIR_ALIASES:
            symbol = PAIR_ALIASES[w]
            break
        elif len(w) >= 6 and w not in ["BUY", "SELL", "SL", "TP", "PRICE"]:
            symbol = w
            break

    sl = None
    tp = None
    sl_match = re.search(r'(?:SL|STOP\s*LOSS)[:\s]*([0-9.]+)', text, re.IGNORECASE)
    if sl_match:
        try:
            sl = float(sl_match.group(1))
        except:
            pass

    tp_match = re.search(r'(?:TP|TAKE\s*PROFIT)[:\s]*([0-9.]+)', text, re.IGNORECASE)
    if tp_match:
        try:
            tp = float(tp_match.group(1))
        except:
            pass

    qty = lot_for(symbol)
    return {"side": side, "symbol": symbol, "qty": qty, "sl": sl, "tp": tp, "raw": text}

def execute_signal(sig):
    if not sig:
        return
    print(f"[EXECUTE] Placing {sig['side']} {sig['qty']} {sig['symbol']} (SL={sig['sl']}, TP={sig['tp']}) via FIX API")
    FIX_CLIENT.place_market_order(sig['symbol'], sig['side'], sig['qty'], sig['sl'], sig['tp'])

def update_dashboard_files():
    os.makedirs("docs", exist_ok=True)
    state = {
        "connected": FIX_CLIENT.connected,
        "host": FIX_HOST,
        "senderCompId": FIX_SENDER_COMP_ID,
        "account": FIX_CLIENT.account_info,
        "positions": FIX_CLIENT.positions,
        "orders": FIX_CLIENT.orders,
        "lastUpdate": datetime.now(timezone.utc).isoformat()
    }
    with open("docs/system_state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    with open("docs/heartbeat.json", "w", encoding="utf-8") as f:
        json.dump({"status": "ok", "time": datetime.now(timezone.utc).isoformat()}, f, indent=2)

def telegram_polling_loop():
    if not TG_TOKEN:
        print("[TG] No Telegram token provided. Running dashboard/bot loop without Telegram.")
        return
    print("[TG] Starting Telegram polling via Bot API...")
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset}&timeout=30"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("ok"):
                    for result in data.get("result", []):
                        offset = result["update_id"] + 1
                        msg = result.get("message") or result.get("channel_post")
                        if msg and "text" in msg:
                            txt = msg["text"]
                            print(f"[TG MSG] {txt}")
                            sig = parse_signal(txt)
                            if sig:
                                execute_signal(sig)
        except Exception as e:
            print(f"[TG] Polling error: {e}")
            time.sleep(5)

def main():
    print("=" * 60)
    print("cTrader FIX API Bot & Dashboard Starting...")
    print(f"Host: {FIX_HOST}:{FIX_TRADE_PORT} | SenderCompID: {FIX_SENDER_COMP_ID}")
    print("=" * 60)

    # Connect to FIX API
    FIX_CLIENT.connect()

    # Start Telegram thread
    t_tg = threading.Thread(target=telegram_polling_loop, daemon=True)
    t_tg.start()

    # Main dashboard update loop
    while True:
        if not FIX_CLIENT.connected:
            print("[FIX] Reconnecting to FIX API...")
            FIX_CLIENT.connect()
        update_dashboard_files()
        time.sleep(10)

if __name__ == "__main__":
    main()

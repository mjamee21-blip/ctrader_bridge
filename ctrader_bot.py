#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader FIX API Telegram Bot + Dashboard (FIX Protocol Edition)

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
        self.logs = []
        self.recent_messages = []
        self.account_info = {"balance": 10000.0, "equity": 10000.0, "margin": 0.0, "freeMargin": 10000.0, "leverage": 100}

    def log(self, level, msg):
        entry = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "level": level, "message": msg}
        self.logs.insert(0, entry)
        print(f"[{level}] {msg}")

    def connect(self):
        try:
            self.log("INFO", f"Connecting to FIX API {self.host}:{self.port} (Sender: {self.sender_comp_id}, Sub: {self.sender_sub_id})...")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(15)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self.ssl_sock = ctx.wrap_socket(self.sock, server_hostname=self.host)
            self.ssl_sock.connect((self.host, self.port))
            # Note: connected becomes True once FIX Logon response (35=A) is received
            self.log("SUCCESS", f"SSL Connected successfully to {self.host}:{self.port}. Sending Logon...")
            self.send_logon()
            
            t = threading.Thread(target=self._recv_loop, daemon=True)
            t.start()
            return True
        except Exception as e:
            self.log("ERROR", f"Connection failed: {e}")
            self.connected = False
            return False

    def send_msg(self, msg_type, fields):
        with self.lock:
            if not self.ssl_sock:
                self.log("WARNING", f"Cannot send {msg_type}: socket not initialized.")
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
                return True
            except Exception as e:
                self.log("ERROR", f"Send error ({msg_type}): {e}")
                self.connected = False
                return False

    def send_logon(self):
        fields = {
            "98": "0",
            "108": "30",
            "554": self.password
        }
        self.send_msg("A", fields)

    def _recv_loop(self):
        buffer = b""
        while True:
            try:
                chunk = self.ssl_sock.recv(4096)
                if not chunk:
                    self.connected = False
                    break
                buffer += chunk
                while b"\x01" in buffer:
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
            except Exception as e:
                self.connected = False
                break

    def _handle_msg(self, msg_str):
        tags = {}
        for part in msg_str.split("\x01"):
            if "=" in part:
                k, v = part.split("=", 1)
                tags[k] = v
        msg_type = tags.get("35")
        if msg_type == "A":
            self.connected = True
            self.log("SUCCESS", f"FIX Logon acknowledged successfully for {self.sender_comp_id}!")
        elif msg_type == "0":
            # Heartbeat acknowledgment
            pass

    def place_market_order(self, symbol, side, qty, sl=None, tp=None):
        cl_ord_id = f"BOT_{int(time.time()*1000)}"
        fields = {
            "11": cl_ord_id,
            "55": symbol,
            "54": "1" if side.upper() == "BUY" else "2",
            "38": str(qty),
            "40": "1",
            "59": "0"
        }
        if sl:
            fields["99"] = str(sl)
        if tp:
            fields["114"] = str(tp)
            
        success = self.send_msg("D", fields)
        if success:
            order_info = {
                "orderId": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "status": "SENT_FIX",
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            self.orders.insert(0, order_info)
            self.log("SUCCESS", f"FIX Market Order placed: {side} {qty} {symbol} (ClOrdID: {cl_ord_id}, SL={sl}, TP={tp})")
        return success

FIX_CLIENT = CTraderFixClient(FIX_HOST, FIX_TRADE_PORT, FIX_SENDER_COMP_ID, FIX_TARGET_COMP_ID, FIX_SENDER_SUB_ID, FIX_PASSWORD)

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
    FIX_CLIENT.log("EXECUTE", f"Placing {sig['side']} {sig['qty']} {sig['symbol']} (SL={sig['sl']}, TP={sig['tp']}) via FIX API")
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
        "recentMessages": FIX_CLIENT.recent_messages,
        "logs": FIX_CLIENT.logs[:100],
        "lastUpdate": datetime.now(timezone.utc).isoformat()
    }
    with open("docs/system_state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    with open("docs/heartbeat.json", "w", encoding="utf-8") as f:
        json.dump({"status": "ok", "time": datetime.now(timezone.utc).isoformat()}, f, indent=2)

def check_telegram_messages():
    if not TG_TOKEN:
        FIX_CLIENT.log("WARNING", "No Telegram token provided.")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?timeout=5"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("ok"):
                for result in data.get("result", []):
                    msg = result.get("message") or result.get("channel_post")
                    if msg and "text" in msg:
                        txt = msg["text"]
                        msg_entry = {
                            "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "chat": str(msg.get("chat", {}).get("title", "Telegram Signal")),
                            "text": txt
                        }
                        FIX_CLIENT.recent_messages.insert(0, msg_entry)
                        FIX_CLIENT.log("INFO", f"Telegram Message Received: {txt}")
                        sig = parse_signal(txt)
                        if sig:
                            execute_signal(sig)
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"Error checking Telegram messages: {e}")

def main():
    FIX_CLIENT.log("INFO", "cTrader FIX API Bot & Dashboard Cycle Starting...")
    FIX_CLIENT.connect()
    
    # Wait up to 8 seconds for FIX Logon acknowledgment (35=A)
    for _ in range(8):
        if FIX_CLIENT.connected:
            break
        time.sleep(1)

    check_telegram_messages()
    update_dashboard_files()

    FIX_CLIENT.log("SUCCESS", "Cycle completed successfully. Exiting cleanly.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cTrader FIX API Bot - Complete Signal & Trade Tracking
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
import logging
from datetime import datetime, timezone
from collections import deque
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
def _clean_sec(val):
    return str(val or "").strip().strip('"').strip("'").strip()
# Configuration
FIX_HOST = _clean_sec(os.environ.get("FIX_HOST", "demo-uk-eqx-01.p.c-trader.com"))
FIX_TRADE_PORT = int(os.environ.get("FIX_TRADE_PORT", "5212"))
FIX_SENDER_COMP_ID = _clean_sec(os.environ.get("FIX_SENDER_COMP_ID", ""))
FIX_TARGET_COMP_ID = _clean_sec(os.environ.get("FIX_TARGET_COMP_ID", "cServer"))
FIX_SENDER_SUB_ID = _clean_sec(os.environ.get("FIX_SENDER_SUB_ID", "TRADE"))
FIX_PASSWORD = _clean_sec(os.environ.get("FIX_PASSWORD", ""))
TG_TOKEN = _clean_sec(os.environ.get("TG_TOKEN", ""))
TG_CHAT = _clean_sec(os.environ.get("TG_CHAT", ""))
PAIR_ALIASES = {
    "BTC": "BTCUSD", "BITCOIN": "BTCUSD", "ETH": "ETHUSD", "ETHEREUM": "ETHUSD",
    "LTC": "LTCUSD", "XRP": "XRPUSD",
    "GOLD": "XAUUSD", "XAU": "XAUUSD", "SILVER": "XAGUSD", "XAG": "XAGUSD",
    "OIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL", "BRENT": "UKOIL",
    "NAS100": "NAS100", "NASDAQ": "NAS100", "US100": "NAS100",
    "US30": "US30", "DOW": "US30", "SPX500": "SPX500", "SP500": "SPX500",
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
class CTraderFIXBot:
    def __init__(self):
        self.sock = None
        self.ssl_sock = None
        self.connected = False
        self.logged_in = False
        self.msg_seq = 1
        self.lock = threading.Lock()
        
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
        print(f"[{level}] {msg}")
    def error(self, msg):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        entry = {"time": ts, "error": msg}
        self.errors.appendleft(entry)
        self.backend_events.appendleft({"time": ts, "event": f"ERROR: {msg}", "type": "error"})
        print(f"[ERROR] {msg}")
    def connect(self):
        try:
            self.log("INFO", f"🔌 Connecting to {FIX_HOST}:{FIX_TRADE_PORT}...")
            self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": "Initiating SSL connection", "type": "connection"})

            # Enhanced network diagnostics
            import socket
            try:
                # Test basic connectivity
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.settimeout(10)
                try:
                    test_sock.connect((FIX_HOST, FIX_TRADE_PORT))
                    test_sock.close()
                    self.log("DEBUG", f"✅ Basic TCP connection to {FIX_HOST}:{FIX_TRADE_PORT} successful")
                except socket.timeout:
                    self.log("WARNING", f"⚠️ TCP connection timeout to {FIX_HOST}:{FIX_TRADE_PORT}")
                except ConnectionRefusedError:
                    self.log("WARNING", f"⚠️ Connection refused by {FIX_HOST}:{FIX_TRADE_PORT}")
                except Exception as e:
                    self.log("WARNING", f"⚠️ Basic TCP connection test failed: {e}")
            except Exception as e:
                self.log("WARNING", f"⚠️ Network diagnostics failed: {e}")

            try:
                addr_info = socket.getaddrinfo(FIX_HOST, FIX_TRADE_PORT, type=socket.SOCK_STREAM)
            except socket.gaierror as e:
                raise RuntimeError(f"DNS lookup failed for {FIX_HOST}: {e}") from e

            if not addr_info:
                raise RuntimeError(f"No socket addresses found for {FIX_HOST}:{FIX_TRADE_PORT}")

            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            if hasattr(ssl, "TLSVersion"):
                ctx.minimum_version = ssl.TLSVersion.TLSv1_2

            last_error = None
            attempt_count = 0
            max_attempts = len(addr_info)
            
            for family, socktype, proto, _, sockaddr in addr_info:
                attempt_count += 1
                try:
                    self.log("DEBUG", f"🔄 Connection attempt {attempt_count}/{max_attempts} to {sockaddr[0]}:{sockaddr[1]}")
                    
                    self.sock = socket.socket(family, socktype, proto)
                    self.sock.settimeout(30)
                    
                    # Add connection timeout with better error handling
                    start_time = time.time()
                    self.sock.connect(sockaddr)
                    connect_time = time.time() - start_time
                    self.log("DEBUG", f"✅ Socket connected in {connect_time:.2f} seconds to {sockaddr[0]}:{sockaddr[1]}")
                    
                    self.ssl_sock = ctx.wrap_socket(self.sock, server_hostname=FIX_HOST)
                    self.connected = True

                    self.log("SUCCESS", "✅ SSL Connected successfully")
                    self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": "SSL handshake complete", "type": "connection"})

                    threading.Thread(target=self._recv_loop, daemon=True).start()
                    self.send_logon()

                    # Enhanced logon waiting with better timeout handling
                    max_wait_time = 60  # Increased timeout
                    wait_start = time.time()
                    while time.time() - wait_start < max_wait_time:
                        if self.logged_in:
                            self.log("DEBUG", f"✅ Logon successful after {time.time() - wait_start:.2f} seconds")
                            return True
                        time.sleep(0.1)

                    if not self.logged_in:
                        raise TimeoutError(f"Logon timeout / not acknowledged by server (waited {max_wait_time} seconds)")
                    return True
                except socket.timeout as e:
                    last_error = e
                    self.log("WARNING", f"⏰ Connection attempt {attempt_count} to {sockaddr[0]}:{sockaddr[1]} timed out: {e}")
                    self.disconnect()
                    self.sock = None
                    self.ssl_sock = None
                except ConnectionRefusedError as e:
                    last_error = e
                    self.log("WARNING", f"🚫 Connection refused by {sockaddr[0]}:{sockaddr[1]}: {e}")
                    self.disconnect()
                    self.sock = None
                    self.ssl_sock = None
                except ssl.SSLError as e:
                    last_error = e
                    self.log("WARNING", f"🔒 SSL error connecting to {sockaddr[0]}:{sockaddr[1]}: {e}")
                    self.disconnect()
                    self.sock = None
                    self.ssl_sock = None
                except Exception as e:
                    last_error = e
                    self.log("WARNING", f"❌ Connection attempt {attempt_count} to {sockaddr[0]}:{sockaddr[1]} failed: {e}")
                    self.disconnect()
                    self.sock = None
                    self.ssl_sock = None

            raise last_error or RuntimeError(f"Failed to connect to {FIX_HOST}:{FIX_TRADE_PORT} after {max_attempts} attempts")
        except Exception as e:
            self.error(f"Connection failed: {str(e)}")
            self.disconnect()
            return False
    def disconnect(self):
        self.connected = False
        if self.ssl_sock:
            try:
                self.ssl_sock.close()
            except:
                pass
    def _calculate_checksum(self, msg):
        return sum(ord(c) for c in msg) % 256
    def send_logon(self):
        try:
            self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": "Sending LOGON message", "type": "fix"})
            self.log("INFO", f"FIX session config: host={FIX_HOST} port={FIX_TRADE_PORT} senderCompID={FIX_SENDER_COMP_ID} targetCompID={FIX_TARGET_COMP_ID} senderSubID={FIX_SENDER_SUB_ID}")
            self.log("DEBUG", f"DIAGNOSTIC CHECK - Username (Tag 553): '{FIX_SENDER_COMP_ID}' (Length: {len(FIX_SENDER_COMP_ID)})")
            self.log("DEBUG", f"DIAGNOSTIC CHECK - TargetCompID (Tag 56): '{FIX_TARGET_COMP_ID}'")
            self.log("DEBUG", f"DIAGNOSTIC CHECK - SenderSubID (Tag 57): '{FIX_SENDER_SUB_ID}'")
            self.log("DEBUG", f"DIAGNOSTIC CHECK - Password length: {len(FIX_PASSWORD)}")
            fields = {
                "98": "0",
                "108": "30",
                "141": "Y",
                "553": FIX_SENDER_COMP_ID,
                "554": FIX_PASSWORD,
            }
            self.log("DEBUG", f"DIAGNOSTIC - Sending logon fields: {fields}")
            self.send_msg("A", fields)
            self.log("INFO", "Logon message sent with Username and ResetSeqNumFlag")
            if self.connected and self.ssl_sock:
                self.log("DEBUG", "FIX Logon payload is being sent using the trade session values above; if this is not the TRADE session on 5212, the broker will ignore it.")
                self.log("DEBUG", "DIAGNOSTIC NOTE: If GitHub Actions ephemeral IP is not whitelisted by cTrader broker or credentials/CompIDs are mismatched, the server will drop the connection or timeout.")
        except Exception as e:
            self.error(f"Logon error: {e}")
    def send_msg(self, msg_type, fields):
        if not self.connected or not self.ssl_sock:
            self.error("Cannot send: not connected")
            return False
        try:
            body_parts = [
                f"35={msg_type}",
                f"49={FIX_SENDER_COMP_ID}",
                f"56={FIX_TARGET_COMP_ID}",
                f"57={FIX_SENDER_SUB_ID}",
                f"34={self.msg_seq}",
                f"52={datetime.now(timezone.utc).strftime('%Y%m%d-%H:%M:%S')}",
            ]
            self.msg_seq += 1
            for k, v in fields.items():
                body_parts.append(f"{k}={v}")
            body = "\x01".join(body_parts) + "\x01"
            header = f"8=FIX.4.4\x019={len(body)}\x01"
            message = header + body
            checksum = self._calculate_checksum(message)
            full_msg = message + f"10={checksum:03d}\x01"
            self.log("DEBUG", f"Raw FIX {msg_type} packet: {full_msg.replace(chr(1), '|')}")
            self.ssl_sock.sendall(full_msg.encode('ascii'))
            self.log("DEBUG", f"Sent FIX {msg_type} ({len(full_msg)} bytes)")
            return True
        except Exception as e:
            self.error(f"Send error: {e}")
            self.connected = False
            return False
    def _recv_loop(self):
        buffer = b""
        while self.connected:
            try:
                chunk = self.ssl_sock.recv(4096)
                if not chunk:
                    self.connected = False
                    break
                buffer += chunk
                while b"\x0110=" in buffer:
                    idx = buffer.find(b"\x0110=")
                    if idx != -1:
                        end = buffer.find(b"\x01", idx + 4)
                        if end != -1:
                            raw = buffer[:end + 1]
                            buffer = buffer[end + 1:]
                            self._handle_msg(raw.decode('ascii', errors='ignore'))
                        else:
                            break
                    else:
                        break
            except:
                self.connected = False
                break
    def _handle_msg(self, msg_str):
        tags = {}
        for part in msg_str.split("\x01"):
            if "=" in part:
                k, v = part.split("=", 1)
                tags[k] = v
        msg_type = tags.get("35", "")
        
        if msg_type == "A":
            self.logged_in = True
            self.log("SUCCESS", f"🔐 FIX Logon ACKNOWLEDGED")
            self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": "Logon acknowledged by server", "type": "fix"})
        elif msg_type == "0":
            self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": "Heartbeat received", "type": "heartbeat"})
        elif msg_type == "8":
            # Execution Report
            order_id = tags.get("37", "")
            status = tags.get("39", "")
            symbol = tags.get("55", "")
            exec_type = tags.get("150", "")
            
            status_names = {"0": "NEW", "1": "PARTIAL", "2": "FILLED", "4": "CANCELLED", "8": "REJECTED"}
            status_text = status_names.get(status, f"UNKNOWN({status})")
            
            self.log("INFO", f"📊 Execution Report: {symbol} {status_text} (OrderID: {order_id})")
            self.backend_events.appendleft({"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "event": f"Order {order_id} {status_text}", "type": "execution"})
            
            # Update trade status
            for trade in self.trades:
                if trade["orderId"] == order_id:
                    trade["status"] = status_text
                    if status_text == "FILLED":
                        trade["filledTime"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    break
    def place_order(self, symbol, side, qty, sl=None, tp=None, raw_signal=None):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Check if pair is enabled
        p_cfg = self.pairs_config.get(symbol.upper(), {})
        if p_cfg.get("enabled", True) is False:
            self.log("WARNING", f"⚠️ Pair {symbol} is DISABLED in settings. Skipping order.")
            return False
        
        # Use configured lot size if qty wasn't explicitly customized
        if symbol.upper() in self.pairs_config:
            qty = self.pairs_config[symbol.upper()]["lot"]

        order_id = f"BOT_{int(time.time()*1000)}"
        
        fields = {
            "11": order_id,
            "55": symbol,
            "54": "1" if side.upper() == "BUY" else "2",
            "38": str(qty),
            "40": "1",
            "59": "1"
        }
        
        if sl:
            fields["99"] = str(sl)
        if tp:
            fields["101"] = str(tp)
        success = self.send_msg("D", fields)
        
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
            "rawSignal": raw_signal or ""
        }
        
        self.trades.appendleft(trade)
        self.backend_events.appendleft({"time": ts, "event": f"Order {order_id} placed: {side} {qty} {symbol}", "type": "order"})
        
        if success:
            self.log("SUCCESS", f"✅ Order sent: {side} {qty} {symbol} | SL={sl} | TP={tp}")
        else:
            self.error(f"Failed to send order: {side} {qty} {symbol}")
        
        return success
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
def check_telegram():
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
                            BOT.place_order(signal['symbol'], signal['side'], signal['qty'], signal['sl'], signal['tp'], text)
                        else:
                            BOT.log("WARNING", f"⚠️  Unparseable text: {text[:50]}")
    except Exception as e:
        BOT.error(f"Telegram check failed: {str(e)}")
def save_state():
    try:
        os.makedirs("docs", exist_ok=True)
        
        state = {
            "connected": BOT.connected,
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
    except Exception as e:
        print(f"Failed to save state: {e}")
BOT = CTraderFIXBot()
def main():
    BOT.log("INFO", "=" * 80)
    BOT.log("INFO", "🤖 cTrader FIX API Bot STARTING")
    BOT.log("INFO", "=" * 80)
    
    if not FIX_PASSWORD or not FIX_SENDER_COMP_ID:
        BOT.error("Missing FIX credentials")
        return False
    
    if not BOT.connect():
        BOT.error("Failed to connect")
        return False
    
    time.sleep(3)
    
    BOT.log("INFO", "📱 Checking Telegram...")
    check_telegram()
    
    BOT.log("INFO", "💾 Saving state...")
    save_state()
    
    BOT.disconnect()
    BOT.log("SUCCESS", "✅ Cycle complete")
    return True
if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
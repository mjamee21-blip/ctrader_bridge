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
import logging
import base64
from datetime import datetime, timezone
from collections import deque

# Persistent config storage
CONFIG_FILE = "pair_config.json"
HEARTBEAT_LOG = "heartbeat.log"

# Ensure imports are available
try:
    import ssl
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

def _clean_sec(val):
    """Clean and strip security credentials"""
    return str(val or "").strip().strip('"').strip("'").strip()

# =====================================================================
# FIX API CONFIGURATION
# =====================================================================
FIX_HOST = _clean_sec(os.environ.get("FIX_HOST", "demo-uk-eqx-01.p.c-trader.com"))
FIX_TRADE_PORT = int(os.environ.get("FIX_TRADE_PORT", "5212"))
FIX_QUOTE_PORT = int(os.environ.get("FIX_QUOTE_PORT", "5211"))
FIX_SENDER_COMP_ID = _clean_sec(os.environ.get("FIX_SENDER_COMP_ID", "demo.deriv.2454444"))
FIX_TARGET_COMP_ID = _clean_sec(os.environ.get("FIX_TARGET_COMP_ID", "cServer"))
FIX_SENDER_SUB_ID = _clean_sec(os.environ.get("FIX_SENDER_SUB_ID", "TRADE"))
FIX_PASSWORD = _clean_sec(os.environ.get("FIX_PASSWORD", ""))

TG_TOKEN = _clean_sec(os.environ.get("TG_TOKEN", ""))
TG_CHAT = _clean_sec(os.environ.get("TG_CHAT", ""))

DEFAULT_QTY = float(os.environ.get("CTRADER_DEFAULT_QTY", "1.0") or "1.0")
MODE = os.environ.get("MODE", "bot")
LAST_UPDATE_ID = 0

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
    """Get lot size for pair"""
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
        self.heartbeat_stop = False
        self.positions = []
        self.orders = []
        self.logs = deque(maxlen=100)
        self.recent_messages = deque(maxlen=30)
        self.account_info = {
            "balance": 10000.0,
            "equity": 10000.0,
            "margin": 0.0,
            "freeMargin": 10000.0,
            "leverage": 100
        }

    def log(self, level, msg):
        """Log message with timestamp"""
        entry = {
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "level": level,
            "message": msg
        }
        self.logs.appendleft(entry)
        logger.info(f"[{level}] {msg}")

    def connect(self):
        """Connect to FIX API server"""
        try:
            self.log("INFO", f"Connecting to FIX API {self.host}:{self.port}...")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(10)  # Reduced timeout
            
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            self.ssl_sock = ctx.wrap_socket(self.sock, server_hostname=self.host)
            self.ssl_sock.connect((self.host, self.port))
            self.connected = True
            
            self.log("SUCCESS", f"SSL Connected to {self.host}:{self.port}")
            
            # Start receive loop BEFORE sending logon
            try:
                recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
                recv_thread.start()
                self.log("DEBUG", "Receive loop started")
            except Exception as e:
                self.log("WARNING", f"Could not start receive loop: {e}")
            
            # Start heartbeat BEFORE sending logon
            try:
                heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
                heartbeat_thread.start()
                self.log("DEBUG", "Heartbeat loop started")
            except Exception as e:
                self.log("WARNING", f"Could not start heartbeat: {e}")
            
            # Send logon AFTER threads are running
            self.send_logon()
            
            return True
        except socket.timeout:
            self.log("ERROR", f"Connection timeout to {self.host}:{self.port}")
            self.disconnect()
            return False
        except ConnectionRefusedError:
            self.log("ERROR", f"Connection refused by {self.host}:{self.port}")
            self.disconnect()
            return False
        except Exception as e:
            self.log("ERROR", f"Connection failed: {e}")
            self.disconnect()
            return False

    def disconnect(self):
        """Safely disconnect from FIX API"""
        self.connected = False
        self.heartbeat_stop = True
        
        if self.ssl_sock:
            try:
                self.ssl_sock.close()
            except:
                pass
        
        if self.sock:
            try:
                self.sock.close()
            except:
                pass

    def _calculate_checksum(self, msg):
        """Calculate FIX protocol checksum"""
        checksum = 0
        for char in msg:
            checksum += ord(char)
        return checksum % 256

    def send_msg(self, msg_type, fields):
        """Send FIX message"""
        with self.lock:
            if not self.connected or not self.ssl_sock:
                self.log("ERROR", f"⚠️  Cannot send {msg_type}: connection not active")
                return False
            
            try:
                # Build message body - FIX 4.4 format
                body_parts = [
                    f"35={msg_type}",           # Message Type
                    f"49={self.sender_comp_id}", # Sender CompID
                    f"56={self.target_comp_id}", # Target CompID
                ]
                
                if self.sender_sub_id:
                    body_parts.append(f"57={self.sender_sub_id}")  # Sender SubID
                
                body_parts.append(f"34={self.msg_seq_num}")        # Message Sequence Number
                self.msg_seq_num += 1
                
                now_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S")
                body_parts.append(f"52={now_str}")                 # Timestamp
                
                # Add all custom fields
                for k, v in fields.items():
                    body_parts.append(f"{k}={v}")
                
                body = "\x01".join(body_parts) + "\x01"
                
                # Build header with correct body length
                header = f"8=FIX.4.4\x019={len(body)}\x01"
                
                # Calculate checksum on header + body
                message_to_check = header + body
                checksum = self._calculate_checksum(message_to_check)
                
                # Build final message
                full_msg = message_to_check + f"10={checksum:03d}\x01"
                
                # Send the message
                bytes_sent = self.ssl_sock.sendall(full_msg.encode('ascii'))
                
                self.log("DEBUG", f"✓ Message {msg_type} sent (seq#{self.msg_seq_num-1}, {len(full_msg)} bytes)")
                return True
                
            except socket.error as e:
                self.log("ERROR", f"✗ Socket error sending {msg_type}: {e}")
                self.connected = False
                return False
            except Exception as e:
                self.log("ERROR", f"✗ Send error ({msg_type}): {e}")
                self.connected = False
                return False

    def send_logon(self):
        """Send FIX Logon message"""
        fields = {
            "98": "0",
            "108": "30",
            "554": self.password
        }
        self.send_msg("A", fields)
        self.log("INFO", "Logon message sent")

    def _send_heartbeat(self):
        """Send FIX heartbeat"""
        self.send_msg("0", {})

    def _heartbeat_loop(self):
        """Periodic heartbeat sender"""
        time.sleep(10)  # Wait before first heartbeat
        while self.connected and not self.heartbeat_stop:
            try:
                time.sleep(30)  # Send heartbeat every 30 seconds
                if self.connected:
                    self._send_heartbeat()
            except Exception as e:
                self.log("DEBUG", f"Heartbeat error: {e}")
                break

    def _recv_loop(self):
        """Receive and parse FIX messages"""
        buffer = b""
        
        while self.connected:
            try:
                chunk = self.ssl_sock.recv(4096)
                if not chunk:
                    # No data received but connection still valid
                    time.sleep(0.1)
                    continue
                
                buffer += chunk
                
                # Process complete FIX messages
                while b"\x0110=" in buffer:
                    # Find checksum field
                    checksum_pos = buffer.find(b"\x0110=")
                    if checksum_pos == -1:
                        break
                    
                    # Find end of checksum
                    checksum_end = buffer.find(b"\x01", checksum_pos + 4)
                    if checksum_end == -1:
                        break
                    
                    # Extract message
                    raw_msg = buffer[:checksum_end + 1]
                    buffer = buffer[checksum_end + 1:]
                    
                    try:
                        self._handle_msg(raw_msg.decode('ascii', errors='ignore'))
                    except Exception as e:
                        self.log("DEBUG", f"Message parsing: {e}")
                        
            except socket.timeout:
                # Timeout is normal, just continue
                continue
            except Exception as e:
                self.log("DEBUG", f"Receive loop: {e}")
                break

    def _handle_msg(self, msg_str):
        """Handle received FIX message"""
        if not msg_str or "\x01" not in msg_str:
            return
        
        # Parse FIX message
        tags = {}
        try:
            for part in msg_str.split("\x01"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    tags[k] = v
        except:
            return
        
        msg_type = tags.get("35")
        
        if msg_type == "A":
            # Logon response
            self.log("SUCCESS", f"🔐 FIX Logon ACKNOWLEDGED for {self.sender_comp_id}")
            
        elif msg_type == "0":
            # Heartbeat from server
            heartbeat_msg = f"[HEARTBEAT] {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            self.log("DEBUG", heartbeat_msg)
            with self.lock:
                with open(HEARTBEAT_LOG, 'a') as f:
                    f.write(heartbeat_msg + "\n")
            
        elif msg_type == "1":
            # Test Request
            self.log("DEBUG", "TestRequest received")
            
        elif msg_type == "8":
            # Execution Report - ORDER CONFIRMATION
            order_id = tags.get("37", "")
            exec_id = tags.get("17", "")
            status_code = tags.get("39", "")
            symbol = tags.get("55", "")
            qty = tags.get("38", "0")
            last_px = tags.get("31", "0")
            cum_qty = tags.get("14", "0")
            side = tags.get("54", "")
            
            # Status codes: 0=New, 1=PartialFill, 2=Filled, 3=DoneForDay, 4=Cancelled, etc.
            status_names = {
                "0": "NEW",
                "1": "PARTIAL_FILLED",
                "2": "FILLED ✓",
                "3": "DONE_FOR_DAY",
                "4": "CANCELLED",
                "5": "REPLACED",
                "6": "PENDING_CANCEL",
                "7": "STOPPED",
                "8": "REJECTED",
                "9": "SUSPENDED",
                "10": "PENDING_NEW",
                "11": "CALCULATED",
                "12": "EXPIRED",
                "13": "ACCEPTED_FOR_BIDDING",
                "14": "PENDING_REPLACE"
            }
            
            status_text = status_names.get(status_code, f"UNKNOWN({status_code})")
            
            self.log("SUCCESS", f"📊 EXECUTION REPORT | Symbol={symbol} | Qty={qty} | Price={last_px} | Status={status_text} | ExecID={exec_id}")
            
            # Update order status
            with self.lock:
                for order in self.orders:
                    if order["orderId"] == order_id or order["orderId"] in exec_id:
                        order["status"] = status_text
                        if float(last_px) > 0:
                            order["entryPrice"] = float(last_px)
                        break
            
        elif msg_type == "9":
            # Order Cancel Reject
            order_id = tags.get("37", "")
            reason = tags.get("58", "Unknown reason")
            self.log("ERROR", f"⚠️  ORDER CANCEL REJECTED | OrderID={order_id} | Reason={reason}")
            
        else:
            # Other message types
            self.log("DEBUG", f"Message Type: {msg_type}")

    def place_market_order(self, symbol, side, qty, sl=None, tp=None):
        """Place market order via FIX API"""
        cl_ord_id = f"BOT_{int(time.time()*1000)}"
        
        # FIX tag reference: 
        # 11=ClOrdID, 55=Symbol, 54=Side(1=Buy,2=Sell), 38=OrderQty, 40=OrdType(1=Market)
        # 99=StopPx, 101=TargetPx, 59=TimeInForce(0=Day,1=IOC,3=FillOrKill), 108=HeartBtInt
        
        fields = {
            "11": cl_ord_id,                    # Unique order ID
            "55": symbol,                       # Symbol
            "54": "1" if side.upper() == "BUY" else "2",  # Side: 1=Buy, 2=Sell
            "38": str(qty),                     # Quantity (actual lot size, not multiplied)
            "40": "1",                          # Order Type: 1=Market
            "59": "1",                          # Time in Force: 1=IOC (Immediate or Cancel)
            "10": "0"                           # Price (not needed for market order)
        }
        
        # Add stop loss and take profit if provided
        if sl is not None:
            fields["99"] = str(sl)              # StopPx
        if tp is not None:
            fields["101"] = str(tp)             # TargetPx
        
        success = self.send_msg("D", fields)
        
        if success:
            order_info = {
                "orderId": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "sl": sl,
                "tp": tp,
                "status": "PENDING",
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "entryPrice": None,
                "profitLoss": 0.0
            }
            with self.lock:
                self.orders.insert(0, order_info)
            
            self.log("SUCCESS", f"✓ ORDER SENT: {side} {qty} {symbol} | SL={sl} | TP={tp} | ID={cl_ord_id}")
        else:
            self.log("ERROR", f"✗ Failed to send order: {side} {qty} {symbol}")
        
        return success

# Initialize FIX client
FIX_CLIENT = CTraderFixClient(
    FIX_HOST,
    FIX_TRADE_PORT,
    FIX_SENDER_COMP_ID,
    FIX_TARGET_COMP_ID,
    FIX_SENDER_SUB_ID,
    FIX_PASSWORD
)

def parse_signal(text):
    """Parse trading signal from text"""
    if not text:
        return None
    
    t_upper = text.upper()
    side = None
    
    # Determine side
    if "BUY" in t_upper:
        side = "BUY"
    elif "SELL" in t_upper:
        side = "SELL"
    
    if not side:
        return None
    
    # Find symbol
    words = re.findall(r'[A-Z0-9]+', t_upper)
    symbol = None
    
    for w in words:
        if w in PAIR_ALIASES:
            symbol = PAIR_ALIASES[w]
            break
        elif len(w) >= 6 and w not in ["BUY", "SELL", "SL", "TP", "PRICE", "ENTRY", "MARKET"]:
            symbol = w
            break
    
    if not symbol:
        return None  # Can't determine pair
    
    # Extract SL and TP
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
    
    return {
        "side": side,
        "symbol": symbol,
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "raw": text
    }

def execute_signal(sig):
    """Execute parsed signal"""
    if not sig:
        return
    
    FIX_CLIENT.log(
        "EXECUTE",
        f"Placing {sig['side']} {sig['qty']} {sig['symbol']} (SL={sig['sl']}, TP={sig['tp']})"
    )
    FIX_CLIENT.place_market_order(
        sig['symbol'],
        sig['side'],
        sig['qty'],
        sig['sl'],
        sig['tp']
    )

def update_dashboard_files():
    """Update dashboard JSON files"""
    os.makedirs("docs", exist_ok=True)
    
    state = {
        "connected": FIX_CLIENT.connected,
        "host": FIX_HOST,
        "senderCompId": FIX_SENDER_COMP_ID,
        "account": FIX_CLIENT.account_info,
        "positions": list(FIX_CLIENT.positions),
        "orders": list(FIX_CLIENT.orders),
        "recentMessages": list(FIX_CLIENT.recent_messages),
        "logs": list(FIX_CLIENT.logs)[:100],
        "lastUpdate": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        with open("docs/system_state.json", "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        
        with open("docs/heartbeat.json", "w", encoding="utf-8") as f:
            json.dump({
                "status": "ok",
                "time": datetime.now(timezone.utc).isoformat(),
                "connected": FIX_CLIENT.connected
            }, f, indent=2)
        
        FIX_CLIENT.log("SUCCESS", "Dashboard files updated")
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"Dashboard update failed: {e}")

def check_telegram_messages():
    """Check for new Telegram messages"""
    global LAST_UPDATE_ID
    
    if not TG_TOKEN:
        FIX_CLIENT.log("WARNING", "No Telegram token configured")
        return
    
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        params = {
            "offset": LAST_UPDATE_ID + 1,
            "timeout": 5,
            "allowed_updates": ["message", "channel_post"]
        }
        
        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}"
        
        req = urllib.request.Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            if data.get("ok") and data.get("result"):
                for result in data.get("result", []):
                    msg = result.get("message") or result.get("channel_post")
                    
                    if msg and "text" in msg:
                        LAST_UPDATE_ID = result.get("update_id", LAST_UPDATE_ID)
                        txt = msg["text"]
                        
                        msg_entry = {
                            "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "chat": str(msg.get("chat", {}).get("title", "Telegram")),
                            "text": txt
                        }
                        
                        with FIX_CLIENT.lock:
                            FIX_CLIENT.recent_messages.appendleft(msg_entry)
                        
                        FIX_CLIENT.log("INFO", f"Telegram: {txt[:50]}")
                        
                        sig = parse_signal(txt)
                        if sig:
                            execute_signal(sig)
    
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"Telegram check failed: {e}")

def load_pair_config():
    """Load pair configuration from file"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return {}

def save_pair_config(config):
    """Save pair configuration to file"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"Failed to save pair config: {e}")
        return False

def main():
    """Main execution loop"""
    FIX_CLIENT.log("INFO", "=" * 80)
    FIX_CLIENT.log("INFO", "🤖 cTrader FIX API Bot CYCLE STARTED")
    FIX_CLIENT.log("INFO", "=" * 80)
    
    # Load persistent pair configuration
    pair_config = load_pair_config()
    FIX_CLIENT.log("INFO", f"Loaded pair configuration: {len(pair_config)} pairs configured")
    
    # Validate credentials
    if not FIX_PASSWORD:
        FIX_CLIENT.log("ERROR", "❌ FIX_PASSWORD not set!")
        update_dashboard_files()
        return False
    
    if not FIX_SENDER_COMP_ID:
        FIX_CLIENT.log("ERROR", "❌ FIX_SENDER_COMP_ID not set!")
        update_dashboard_files()
        return False
    
    # Attempt to connect to FIX API
    connection_success = False
    try:
        FIX_CLIENT.log("INFO", "🔌 Attempting FIX API connection...")
        if FIX_CLIENT.connect():
            connection_success = True
            FIX_CLIENT.log("SUCCESS", "✓ FIX API connection established!")
        else:
            FIX_CLIENT.log("ERROR", "✗ Failed to establish FIX connection")
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"✗ FIX connection exception: {e}")
    
    # Wait for logon processing
    time.sleep(5)
    
    if not connection_success:
        FIX_CLIENT.log("WARNING", "⚠️  Proceeding without FIX connection - Telegram processing will still work")
    
    # Attempt to check Telegram for signals
    try:
        FIX_CLIENT.log("INFO", "📱 Checking Telegram for trading signals...")
        check_telegram_messages()
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"✗ Telegram check error: {e}")
    
    # Always update dashboard with current state
    try:
        update_dashboard_files()
        FIX_CLIENT.log("SUCCESS", "✓ Dashboard files updated")
    except Exception as e:
        FIX_CLIENT.log("ERROR", f"✗ Dashboard update error: {e}")
    
    # Cleanup
    FIX_CLIENT.disconnect()
    
    FIX_CLIENT.log("SUCCESS", "=" * 80)
    FIX_CLIENT.log("SUCCESS", "✓ Bot cycle completed successfully")
    FIX_CLIENT.log("SUCCESS", "=" * 80)
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        FIX_CLIENT.disconnect()
        sys.exit(1)

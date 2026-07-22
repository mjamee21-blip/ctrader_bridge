#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TradeLocker Telegram Bot + Dashboard
# Comprehensive: Full logging, analytics, heartbeat, process details

import os, json, re, urllib.request, urllib.parse, sys
from urllib.error import HTTPError
from datetime import datetime, timezone
import ssl

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# =====================================================================
# CONFIG FROM GITHUB SECRETS
# =====================================================================
TL_EMAIL = os.environ.get("TL_EMAIL", "")
TL_PASSWORD = os.environ.get("TL_PASSWORD", "")
TL_SERVER = os.environ.get("TL_SERVER", "TradeLocker-Demo")
TL_ACCOUNT_ID = int(os.environ.get("TL_ACCOUNT_ID", "0")) if os.environ.get("TL_ACCOUNT_ID", "0").strip() else 0
TL_ACC_NUM = int(os.environ.get("TL_ACC_NUM", "1")) if os.environ.get("TL_ACC_NUM", "1").strip() else 1
TL_ENV = os.environ.get("TL_ENV", "demo")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")
TL_PAIR_MAP_JSON = os.environ.get("TL_PAIR_MAP", "{}").strip()
if TL_PAIR_MAP_JSON.startswith("'") and TL_PAIR_MAP_JSON.endswith("'"):
    TL_PAIR_MAP_JSON = TL_PAIR_MAP_JSON[1:-1].strip()

try:
    PAIR_MAP = json.loads(TL_PAIR_MAP_JSON)
    if not isinstance(PAIR_MAP, dict):
        PAIR_MAP = {}
except:
    PAIR_MAP = {}

DEFAULT_QTY = float(os.environ.get("TL_DEFAULT_QTY", "1.0").strip()) if os.environ.get("TL_DEFAULT_QTY", "").strip() else 1.0
MODE = os.environ.get("MODE", "bot")

TL_BASE = "https://live.tradelocker.com" if TL_ENV.lower() == "live" else "https://demo.tradelocker.com"
_last_update_id = 0
_instruments = {}

# =====================================================================
# COMPREHENSIVE LOGGING SYSTEM
# =====================================================================
_execution_log = []
_system_status = {
    "tradelocker": {"connected": False, "last_check": None, "error": None, "latency_ms": None},
    "telegram": {"connected": False, "last_check": None, "error": None, "latency_ms": None},
    "dashboard": {"last_update": None, "status": "unknown"},
    "cron": {"last_run": None, "last_status": "unknown", "next_run": None, "total_runs": 0, "successful_runs": 0, "failed_runs": 0},
    "instruments": {"loaded": False, "count": 0, "last_loaded": None, "error": None},
    "pair_mapping": {"configured": len(PAIR_MAP) > 0, "mappings": PAIR_MAP, "count": len(PAIR_MAP)},
    "trades": {"total": 0, "successful": 0, "failed": 0, "last_trade": None},
    "script_start": datetime.now(timezone.utc).isoformat(),
}

def log_event(category, event, details=None, status="info"):
    """Add event to execution log"""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "category": category,
        "event": event,
        "details": details or {},
        "status": status
    }
    _execution_log.append(entry)
    # Keep only last 1000 entries
    if len(_execution_log) > 1000:
        _execution_log.pop(0)

# =====================================================================
# TRADELOCKER API CLIENT
# =====================================================================

class TradeLockerClient:
    def __init__(self):
        self.base_url = f"{TL_BASE}/backend-api"
        self.token = None
        self.authenticated = False
    
    def _req(self, method, path, body=None, headers_extra=None, timeout=20):
        import time
        start_time = time.time()
        url = f"{self.base_url}{path}"
        headers = {"User-Agent": "TLBot/1.0", "Accept": "application/json"}
        if body:
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if headers_extra:
            headers.update(headers_extra)
        
        try:
            data = json.dumps(body).encode() if body else None
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content = resp.read().decode().strip()
                latency = int((time.time() - start_time) * 1000)
                try:
                    result = json.loads(content) if content else {}
                except:
                    result = {"raw": content}
                if not isinstance(result, dict):
                    result = {"data": result}
                result["_latency_ms"] = latency
                return result
        except HTTPError as e:
            latency = int((time.time() - start_time) * 1000)
            return {"error": f"http_error_{e.code}", "_latency_ms": latency}
        except Exception as e:
            latency = int((time.time() - start_time) * 1000)
            return {"error": str(e)[:100], "_latency_ms": latency}
    
    def auth(self):
        global _system_status
        log_event("tradelocker", "auth_start", {"server": TL_SERVER, "env": TL_ENV})
        try:
            payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}
            result = self._req("POST", "/auth/jwt/token", body=payload)
            latency = result.get("_latency_ms", 0)
            
            if result.get("error"):
                _system_status["tradelocker"] = {
                    "connected": False,
                    "last_check": datetime.now(timezone.utc).isoformat(),
                    "error": result.get("error"),
                    "latency_ms": latency
                }
                log_event("tradelocker", "auth_failed", {"error": result.get("error"), "latency_ms": latency}, "error")
                return False
            
            self.token = result.get("accessToken") or result.get("access_token") or result.get("token")
            self.authenticated = bool(self.token)
            _system_status["tradelocker"] = {
                "connected": self.authenticated,
                "last_check": datetime.now(timezone.utc).isoformat(),
                "error": None,
                "latency_ms": latency
            }
            log_event("tradelocker", "auth_success", {"latency_ms": latency}, "success")
            return self.authenticated
        except Exception as e:
            _system_status["tradelocker"] = {
                "connected": False,
                "last_check": datetime.now(timezone.utc).isoformat(),
                "error": str(e)[:100],
                "latency_ms": None
            }
            log_event("tradelocker", "auth_exception", {"error": str(e)}, "error")
            return False
    
    def load_instruments(self):
        global _instruments, _system_status
        log_event("instruments", "load_start", {"account_id": TL_ACCOUNT_ID})
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/instruments", headers_extra={"accNum": str(TL_ACC_NUM)})
        latency = result.get("_latency_ms", 0)
        
        if not isinstance(result, dict) or result.get("error"):
            _system_status["instruments"] = {
                "loaded": False, "count": 0,
                "last_loaded": datetime.now(timezone.utc).isoformat(),
                "error": result.get("error") if isinstance(result, dict) else "invalid_response"
            }
            log_event("instruments", "load_failed", {"error": _system_status["instruments"]["error"], "latency_ms": latency}, "error")
            return False
        
        data = result.get("d", result)
        if not isinstance(data, dict):
            data = result
        instruments = data.get("instruments") or data.get("data") or data.get("items") or []
        if not isinstance(instruments, list):
            instruments = []
        _instruments = {}
        
        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = (inst.get("name") or inst.get("symbol") or "").upper()
            inst_id = inst.get("tradableInstrumentId") or inst.get("id") or ""
            route_id = None
            for route in (inst.get("routes") or []):
                if isinstance(route, dict) and route.get("type") == "TRADE":
                    route_id = route.get("id")
                    break
            if name:
                _instruments[name] = {"id": inst_id, "route_id": route_id, "name": name}
        
        _system_status["instruments"] = {
            "loaded": len(_instruments) > 0,
            "count": len(_instruments),
            "last_loaded": datetime.now(timezone.utc).isoformat(),
            "error": None
        }
        log_event("instruments", "load_success", {"count": len(_instruments), "latency_ms": latency}, "success")
        return len(_instruments) > 0
    
    def find_instrument(self, pair_name):
        """Find instrument by pair name with detailed logging"""
        pair_upper = pair_name.upper().strip()
        log_event("pair_mapping", "lookup_start", {"pair": pair_upper, "pair_map": PAIR_MAP})
        
        # Direct match
        if pair_upper in _instruments:
            log_event("pair_mapping", "lookup_direct_match", {"pair": pair_upper, "instrument_id": _instruments[pair_upper]["id"]}, "success")
            return _instruments[pair_upper]
        
        # Check PAIR_MAP
        mapped = PAIR_MAP.get(pair_upper, "").upper()
        if mapped:
            if mapped in _instruments:
                log_event("pair_mapping", "lookup_mapped_match", {"pair": pair_upper, "mapped_to": mapped, "instrument_id": _instruments[mapped]["id"]}, "success")
                return _instruments[mapped]
            else:
                log_event("pair_mapping", "lookup_mapped_not_found", {"pair": pair_upper, "mapped_to": mapped}, "warning")
        
        # Try without slash
        normalized = pair_upper.replace("/", "")
        if normalized in _instruments:
            log_event("pair_mapping", "lookup_normalized_match", {"pair": pair_upper, "normalized": normalized}, "success")
            return _instruments[normalized]
        
        # Partial match
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                log_event("pair_mapping", "lookup_partial_match", {"pair": pair_upper, "matched": name}, "success")
                return info
        
        log_event("pair_mapping", "lookup_failed", {"pair": pair_upper, "available_pairs": list(_instruments.keys())[:10]}, "error")
        return None
    
    def get_quote(self, inst_id):
        qs = f"?tradableInstrumentId={inst_id}"
        result = self._req("GET", f"/trade/quotes{qs}", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return None
        d = result.get("d", result)
        quotes = d.get("quotes") if isinstance(d, dict) else None
        q = (quotes[0] if isinstance(quotes, list) and quotes else d)
        bp, ap = q.get("bp") or q.get("bid"), q.get("ap") or q.get("ask")
        return {"bp": float(bp), "ap": float(ap)} if bp and ap else None
    
    def place_order(self, pair, direction, sl, tp, qty=None):
        """Place market order with comprehensive logging"""
        global _system_status
        if not qty:
            qty = DEFAULT_QTY
        
        log_event("trade", "order_start", {"pair": pair, "direction": direction, "sl": sl, "tp": tp, "qty": qty})
        
        instrument = self.find_instrument(pair)
        if not instrument:
            self.load_instruments()
            instrument = self.find_instrument(pair)
        
        if not instrument:
            _system_status["trades"]["failed"] += 1
            _system_status["trades"]["total"] += 1
            log_event("trade", "order_failed", {"pair": pair, "reason": "instrument_not_found"}, "error")
            return False
        
        route_id = instrument.get("route_id")
        inst_id = instrument["id"]
        side = "buy" if direction.upper() == "BUY" else "sell"
        
        payload = {
            "tradableInstrumentId": inst_id,
            "type": "market",
            "validity": "IOC",
            "side": side,
            "qty": qty
        }
        if sl:
            payload["stopLoss"] = float(sl)
        if tp:
            payload["takeProfit"] = float(tp)
        if route_id:
            payload["routeId"] = str(route_id)
        
        log_event("trade", "order_payload", {"instrument": instrument["name"], "inst_id": inst_id, "route_id": route_id, "payload": payload})
        
        result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)
        latency = result.get("_latency_ms", 0)
        
        if result.get("error") or result.get("s") == "error":
            errmsg = result.get("errmsg") or result.get("error") or ""
            log_event("trade", "order_api_error", {"error": errmsg, "latency_ms": latency}, "warning")
            
            if "forbidden" in str(errmsg).lower() and "route" in str(errmsg).lower():
                quote = self.get_quote(inst_id)
                if quote:
                    mid = (quote["bp"] + quote["ap"]) / 2.0
                    offset = max(mid * 0.0015, 0.5)
                    limit_price = round(quote["ap"] + offset, 5) if side == "buy" else round(quote["bp"] - offset, 5)
                    lim_payload = {"tradableInstrumentId": inst_id, "type": "limit", "side": side, "qty": qty, "price": limit_price}
                    if sl:
                        lim_payload["stopLoss"] = float(sl)
                    if tp:
                        lim_payload["takeProfit"] = float(tp)
                    if route_id:
                        lim_payload["routeId"] = str(route_id)
                    result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=lim_payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)
                    log_event("trade", "order_retry_limit", {"limit_price": limit_price})
        
        success = not (result.get("error") or result.get("s") == "error")
        _system_status["trades"]["total"] += 1
        _system_status["trades"]["last_trade"] = datetime.now(timezone.utc).isoformat()
        
        if success:
            _system_status["trades"]["successful"] += 1
            log_event("trade", "order_success", {"pair": pair, "direction": direction, "instrument": instrument["name"], "sl": sl, "tp": tp, "latency_ms": latency}, "success")
        else:
            _system_status["trades"]["failed"] += 1
            log_event("trade", "order_failed", {"pair": pair, "error": result.get("error", "unknown"), "latency_ms": latency}, "error")
        
        return success
    
    def get_open_positions(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/positions", headers_extra={"accNum": str(TL_ACC_NUM)})
        if not isinstance(result, dict) or result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        positions = data.get("positions") or data.get("data") or data.get("items") or []
        return positions if isinstance(positions, list) else []
    
    def close_position(self, pos_id):
        result = self._req("DELETE", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", headers_extra={"accNum": str(TL_ACC_NUM)})
        success = isinstance(result, dict) and not result.get("error")
        log_event("trade", "close_position", {"pos_id": pos_id, "success": success}, "success" if success else "error")
        return success
    
    def modify_position(self, pos_id, pos, new_sl):
        payload = {"stopLoss": float(new_sl)}
        if isinstance(pos, dict):
            qty = pos.get("qty") or pos.get("quantity") or pos.get("size") or 1.0
            tp = pos.get("takeProfit") or pos.get("tp")
            try:
                payload["qty"] = float(qty)
            except:
                payload["qty"] = qty
            if tp:
                try:
                    payload["takeProfit"] = float(tp)
                except:
                    payload["takeProfit"] = tp
        
        result = self._req("PUT", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        if not isinstance(result, dict) or result.get("error"):
            result = self._req("PATCH", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        success = isinstance(result, dict) and not result.get("error")
        log_event("trade", "modify_sl", {"pos_id": pos_id, "new_sl": new_sl, "success": success}, "success" if success else "error")
        return success
    
    def get_account_state(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/state", headers_extra={"accNum": str(TL_ACC_NUM)})
        if not isinstance(result, dict) or result.get("error"):
            return {}
        data = result.get("d", result)
        if isinstance(data, dict):
            return data
        return {}
    
    def get_orders(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/orders?limit=50", headers_extra={"accNum": str(TL_ACC_NUM)})
        if not isinstance(result, dict) or result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data[:20]
        if not isinstance(data, dict):
            return []
        raw_orders = data.get("orders") or data.get("data") or data.get("items") or []
        if isinstance(raw_orders, list):
            return raw_orders[:20]
        return []

# =====================================================================
# SIGNAL PARSER
# =====================================================================

def looks_like_signal(text):
    if not text:
        return False
    t = text.upper()
    return "BUY" in t or "SELL" in t or "TP HIT" in t or "SL HIT" in t or "SL_UPDATE" in t

def parse_signal(text):
    if not text:
        return None
    lines = text.strip().split("\n")
    first = lines[0].strip().upper()
    
    if "TP HIT" in first or "SL HIT" in first:
        pair = re.search(r"(TP|SL)\s*HIT\s*[-–:]\s*(\S+)", first, re.IGNORECASE)
        pair_str = pair.group(2) if pair else ""
        if not pair_str:
            for line in lines[1:]:
                m = re.search(r"([A-Z]{3,6}[/]?[A-Z]{0,6})", line.strip())
                if m:
                    pair_str = m.group(1)
                    break
        result = "TP" if "TP HIT" in first else "SL"
        return {"type": "TPSL_HIT", "result": result, "pair": pair_str}
    
    if "#SL_UPDATE" in text.upper() or "SL UPDATE" in text.upper():
        pair, new_sl = None, None
        for line in lines:
            if "PAIR" in line.upper():
                pair = line.split(":", 1)[1].strip() if ":" in line else None
            m = re.search(r"(?:New\s*)?SL\s*[:=]\s*([\d.]+)", line, re.IGNORECASE)
            if m:
                try:
                    new_sl = float(m.group(1))
                except:
                    pass
        if pair and new_sl:
            return {"type": "SL_UPDATE", "pair": pair, "new_sl": new_sl}
        return None
    
    sig = re.search(r"\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)", first, re.IGNORECASE)
    if not sig:
        return None
    
    direction = sig.group(1).upper()
    pair = sig.group(2).upper()
    sl = tp = None
    
    for line in lines:
        cl = re.sub(r"<[^>]+>", "", line).strip()
        m = re.search(r"(?<![A-Za-z])SL\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and not sl:
            try:
                sl = float(m.group(1))
            except:
                pass
        m = re.search(r"(?<![A-Za-z])TP\s*[:\s]\s*([\d.]+)", cl, re.IGNORECASE)
        if m and not tp:
            try:
                tp = float(m.group(1))
            except:
                pass
    
    return {"type": "SIGNAL", "direction": direction, "pair": pair, "sl": sl, "tp": tp}

# =====================================================================
# TELEGRAM
# =====================================================================

def tg_get_messages(offset=0):
    global _last_update_id, _system_status
    import time
    start_time = time.time()
    
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset+1}&timeout=3&limit=100"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            latency = int((time.time() - start_time) * 1000)
            
            if not result.get("ok"):
                _system_status["telegram"] = {
                    "connected": False,
                    "last_check": datetime.now(timezone.utc).isoformat(),
                    "error": "Telegram API returned not OK",
                    "latency_ms": latency
                }
                log_event("telegram", "api_error", {"latency_ms": latency}, "error")
                return []
            
            messages = []
            for upd in result.get("result", []):
                uid = upd.get("update_id", 0)
                if uid > _last_update_id:
                    _last_update_id = uid
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat = msg.get("chat", {})
                chat_id = str(chat.get("id", ""))
                chat_uname = chat.get("username", "")
                target = str(TG_CHAT).lstrip("@")
                if TG_CHAT != "ANY" and target != chat_uname and target != chat_id:
                    continue
                text = msg.get("text") or msg.get("caption") or ""
                if text:
                    messages.append({"text": text})
            
            _system_status["telegram"] = {
                "connected": True,
                "last_check": datetime.now(timezone.utc).isoformat(),
                "error": None,
                "latency_ms": latency
            }
            log_event("telegram", "fetch_success", {"messages": len(messages), "latency_ms": latency}, "success")
            return messages
    except Exception as e:
        latency = int((time.time() - start_time) * 1000)
        _system_status["telegram"] = {
            "connected": False,
            "last_check": datetime.now(timezone.utc).isoformat(),
            "error": str(e)[:100],
            "latency_ms": latency
        }
        log_event("telegram", "fetch_error", {"error": str(e), "latency_ms": latency}, "error")
        return []

# =====================================================================
# DASHBOARD GENERATOR
# =====================================================================

def generate_dashboard_html(client, connected, error):
    state = client.get_account_state() if connected else {}
    positions = client.get_open_positions() if connected else []
    orders = client.get_orders() if connected else []
    
    # System status
    tl = _system_status["tradelocker"]
    tg = _system_status["telegram"]
    inst = _system_status["instruments"]
    trades = _system_status["trades"]
    cron = _system_status["cron"]
    
    # Account state
    account_state = {
        "balance": f"${state.get('accountBalance', 'N/A')}",
        "equity": f"${state.get('equity', 'N/A')}",
        "margin": f"${state.get('usedMargin', 'N/A')}",
        "free_margin": f"${state.get('freeMargin', 'N/A')}",
        "margin_level": f"{state.get('marginLevel', 'N/A')}%"
    }
    
    # Positions
    positions_data = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        try:
            pos_time = pos.get("openTime", "")
            if isinstance(pos_time, (int, float)):
                pos_time = datetime.fromtimestamp(pos_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            pnl_val = float(pos.get("pnl") or 0)
            positions_data.append({
                "pair": pos.get("instrumentName") or pos.get("symbol") or "N/A",
                "side": str(pos.get("side") or "").upper(),
                "qty": pos.get("qty"),
                "price": pos.get("price"),
                "sl": pos.get("stopLoss"),
                "tp": pos.get("takeProfit"),
                "pnl": f"{pnl_val:+.2f}",
                "pnl_value": pnl_val,
                "time": pos_time
            })
        except:
            continue
    
    # Orders
    orders_data = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        try:
            order_time = order.get("modifiedAt") or order.get("placedAt") or ""
            if isinstance(order_time, (int, float)):
                order_time = datetime.fromtimestamp(order_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            orders_data.append({
                "id": str(order.get("orderId") or order.get("id") or "")[:12],
                "pair": order.get("instrumentName") or order.get("symbol") or "N/A",
                "side": str(order.get("side") or "").upper(),
                "type": order.get("type") or "N/A",
                "qty": order.get("qty"),
                "price": order.get("price"),
                "status": order.get("status") or "N/A",
                "time": order_time
            })
        except:
            continue
    
    # Recent logs (last 50)
    recent_logs = _execution_log[-50:] if _execution_log else []
    
    # Logs HTML
    logs_html = ""
    for log in reversed(recent_logs):
        status_color = {"success": "#3fb950", "error": "#f85149", "warning": "#d29922", "info": "#58a6ff"}.get(log["status"], "#8b949e")
        logs_html += f'<div class="log-entry"><span class="log-time">{log["timestamp"][11:19]}</span> <span class="log-cat" style="color: {status_color}">[{log["category"]}]</span> <span class="log-event">{log["event"]}</span></div>'
    if not recent_logs:
        logs_html = '<div style="text-align: center; color: #8b949e; padding: 20px;">No logs yet</div>'
    
    # Positions HTML
    pos_rows = ""
    for p in positions_data:
        side_class = "buy" if "BUY" in p["side"] else "sell"
        pnl_color = "#3fb950" if p["pnl_value"] >= 0 else "#f85149"
        pos_rows += f'<tr><td class="pair">{p["pair"]}</td><td class="{side_class}">{p["side"]}</td><td>{p["qty"]}</td><td>{p["price"]}</td><td style="color: #f85149;">{p["sl"]}</td><td style="color: #3fb950;">{p["tp"]}</td><td style="color: {pnl_color}; font-weight: 600;">{p["pnl"]}</td></tr>'
    if not positions_data:
        pos_rows = '<tr><td colspan="7" style="text-align: center; color: #8b949e; padding: 20px;">No open positions</td></tr>'
    
    # Orders HTML
    order_rows = ""
    for o in orders_data:
        side_class = "buy" if "BUY" in o["side"] else "sell"
        order_rows += f'<tr><td style="font-size: 10px; color: #8b949e;">{o["id"]}</td><td class="pair">{o["pair"]}</td><td class="{side_class}">{o["side"]}</td><td>{o["type"]}</td><td>{o["qty"]}</td><td>{o["price"]}</td><td>{o["status"]}</td></tr>'
    if not orders_data:
        order_rows = '<tr><td colspan="7" style="text-align: center; color: #8b949e; padding: 20px;">No recent orders</td></tr>'
    
    last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="300">
    <title>TradeLocker Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; font-size: 12px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 15px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; padding: 12px 15px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 15px; }}
        .header h1 {{ font-size: 14px; font-weight: 700; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; margin-bottom: 12px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 12px; }}
        .stat {{ text-align: center; padding: 10px; }}
        .stat-value {{ font-size: 16px; font-weight: 800; color: #58a6ff; }}
        .stat-label {{ font-size: 9px; color: #8b949e; margin-top: 3px; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ padding: 8px; background: #0d1117; color: #8b949e; font-size: 9px; text-transform: uppercase; text-align: left; border-bottom: 1px solid #30363d; }}
        td {{ padding: 8px; border-bottom: 1px solid #30363d; font-size: 11px; }}
        .pair {{ font-weight: 600; }}
        .buy {{ color: #3fb950; }}
        .sell {{ color: #f85149; }}
        .section-title {{ font-size: 10px; color: #8b949e; font-weight: 600; text-transform: uppercase; margin: 15px 0 8px 0; }}
        .conn-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 12px; }}
        .conn-card {{ padding: 12px; border-radius: 6px; border: 1px solid #30363d; }}
        .conn-card.ok {{ background: rgba(63, 185, 80, 0.1); border-color: #3fb950; }}
        .conn-card.error {{ background: rgba(248, 81, 73, 0.1); border-color: #f85149; }}
        .conn-title {{ font-weight: 600; font-size: 11px; margin-bottom: 4px; }}
        .conn-detail {{ font-size: 10px; color: #8b949e; }}
        .log-entry {{ padding: 4px 8px; border-bottom: 1px solid #212628; font-family: monospace; font-size: 10px; }}
        .log-time {{ color: #8b949e; }}
        .log-cat {{ font-weight: 600; }}
        .log-event {{ color: #c9d1d9; }}
        .heartbeat {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 5px; }}
        .heartbeat.online {{ background: #3fb950; box-shadow: 0 0 8px #3fb950; }}
        .heartbeat.offline {{ background: #f85149; }}
        .stats-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 12px; }}
        .stat-box {{ background: #0d1117; padding: 10px; border-radius: 6px; text-align: center; }}
        .stat-box-value {{ font-size: 18px; font-weight: 700; }}
        .stat-box-label {{ font-size: 9px; color: #8b949e; margin-top: 3px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 TradeLocker Dashboard</h1>
            <div style="text-align: right;">
                <div style="font-size: 10px; color: #8b949e;">{last_update}</div>
                <div style="font-size: 9px; color: #8b949e;">Auto-refresh: 5min</div>
            </div>
        </div>
        
        <!-- System Health -->
        <div class="section-title">❤️ System Health</div>
        <div class="conn-grid">
            <div class="conn-card {"ok" if tl.get("connected") else "error"}">
                <div class="conn-title"><span class="heartbeat {"online" if tl.get("connected") else "offline"}"></span>TradeLocker</div>
                <div class="conn-detail">Status: {"Online" if tl.get("connected") else "Offline"}</div>
                <div class="conn-detail">Latency: {tl.get("latency_ms", "N/A")}ms</div>
                <div class="conn-detail">Last: {tl.get("last_check", "Never")[:19]}</div>
                {f'<div class="conn-detail" style="color: #f85149;">{tl.get("error")}</div>' if tl.get("error") else ""}
            </div>
            <div class="conn-card {"ok" if tg.get("connected") else "error"}">
                <div class="conn-title"><span class="heartbeat {"online" if tg.get("connected") else "offline"}"></span>Telegram</div>
                <div class="conn-detail">Status: {"Online" if tg.get("connected") else "Offline"}</div>
                <div class="conn-detail">Latency: {tg.get("latency_ms", "N/A")}ms</div>
                <div class="conn-detail">Last: {tg.get("last_check", "Never")[:19]}</div>
                {f'<div class="conn-detail" style="color: #f85149;">{tg.get("error")}</div>' if tg.get("error") else ""}
            </div>
            <div class="conn-card {"ok" if inst.get("loaded") else "error"}">
                <div class="conn-title"><span class="heartbeat {"online" if inst.get("loaded") else "offline"}"></span>Instruments</div>
                <div class="conn-detail">Loaded: {inst.get("count", 0)} pairs</div>
                <div class="conn-detail">Last: {inst.get("last_loaded", "Never")[:19]}</div>
                {f'<div class="conn-detail" style="color: #f85149;">{inst.get("error")}</div>' if inst.get("error") else ""}
            </div>
            <div class="conn-card ok">
                <div class="conn-title"><span class="heartbeat online"></span>Dashboard</div>
                <div class="conn-detail">Status: Active</div>
                <div class="conn-detail">Last Update: {last_update[:19]}</div>
                <div class="conn-detail">Script Started: {_system_status["script_start"][:19]}</div>
            </div>
        </div>
        
        <!-- Trade Stats -->
        <div class="section-title">📊 Trade Statistics</div>
        <div class="stats-row">
            <div class="stat-box">
                <div class="stat-box-value" style="color: #58a6ff;">{trades["total"]}</div>
                <div class="stat-box-label">Total Trades</div>
            </div>
            <div class="stat-box">
                <div class="stat-box-value" style="color: #3fb950;">{trades["successful"]}</div>
                <div class="stat-box-label">Successful</div>
            </div>
            <div class="stat-box">
                <div class="stat-box-value" style="color: #f85149;">{trades["failed"]}</div>
                <div class="stat-box-label">Failed</div>
            </div>
            <div class="stat-box">
                <div class="stat-box-value" style="color: #d29922;">{round(trades["successful"] / max(trades["total"], 1) * 100, 1)}%</div>
                <div class="stat-box-label">Success Rate</div>
            </div>
        </div>
        
        <!-- Account Overview -->
        <div class="section-title">💰 Account Overview</div>
        <div class="grid">
            <div class="card stat"><div class="stat-value">{account_state["balance"]}</div><div class="stat-label">Balance</div></div>
            <div class="card stat"><div class="stat-value">{account_state["equity"]}</div><div class="stat-label">Equity</div></div>
            <div class="card stat"><div class="stat-value">{account_state["margin"]}</div><div class="stat-label">Used Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state["free_margin"]}</div><div class="stat-label">Free Margin</div></div>
            <div class="card stat"><div class="stat-value">{account_state["margin_level"]}</div><div class="stat-label">Margin Level</div></div>
            <div class="card stat"><div class="stat-value">{len(positions_data)}</div><div class="stat-label">Open Positions</div></div>
        </div>
        
        <!-- Open Positions -->
        <div class="section-title">📈 Open Positions ({len(positions_data)})</div>
        <div class="card">
            <table>
                <thead><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry</th><th>SL</th><th>TP</th><th>P&L</th></tr></thead>
                <tbody>{pos_rows}</tbody>
            </table>
        </div>
        
        <!-- Recent Orders -->
        <div class="section-title">📋 Recent Orders ({len(orders_data)})</div>
        <div class="card">
            <table>
                <thead><tr><th>ID</th><th>Pair</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th></tr></thead>
                <tbody>{order_rows}</tbody>
            </table>
        </div>
        
        <!-- Execution Log -->
        <div class="section-title">📝 Execution Log (Last 50 Events)</div>
        <div class="card" style="max-height: 400px; overflow-y: auto;">
            {logs_html}
        </div>
    </div>
</body>
</html>"""
    return html

# =====================================================================
# BOT MODE
# =====================================================================

def run_bot():
    global _system_status
    _system_status["cron"]["last_run"] = datetime.now(timezone.utc).isoformat()
    _system_status["cron"]["total_runs"] += 1
    
    log_event("cron", "job_start", {"mode": "bot", "env": TL_ENV})
    
    if not (TL_EMAIL and TL_PASSWORD and TG_TOKEN and TG_CHAT):
        log_event("cron", "missing_secrets", status="error")
        _system_status["cron"]["failed_runs"] += 1
        _system_status["cron"]["last_status"] = "failed"
        return False
    
    client = TradeLockerClient()
    if not client.auth():
        log_event("cron", "auth_failed", status="error")
        _system_status["cron"]["failed_runs"] += 1
        _system_status["cron"]["last_status"] = "failed"
        return False
    
    client.load_instruments()
    messages = tg_get_messages(offset=_last_update_id)
    
    signals_found = 0
    for msg in messages:
        text = (msg.get("text") or "").strip()
        if not looks_like_signal(text):
            continue
        
        parsed = parse_signal(text)
        if not parsed:
            continue
        
        signals_found += 1
        log_event("signal", "received", parsed)
        
        if parsed["type"] == "SIGNAL":
            success = client.place_order(parsed["pair"], parsed["direction"], parsed["sl"], parsed["tp"])
            log_event("signal", "processed", {"pair": parsed["pair"], "success": success}, "success" if success else "error")
        
        elif parsed["type"] == "TPSL_HIT":
            pair = parsed["pair"]
            positions = client.get_open_positions()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_sym = pos.get("instrumentName") or pos.get("symbol") or pos.get("name") or ""
                if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                    pos_id = pos.get("positionId") or pos.get("id")
                    if pos_id:
                        client.close_position(pos_id)
        
        elif parsed["type"] == "SL_UPDATE":
            pair = parsed["pair"]
            new_sl = parsed["new_sl"]
            positions = client.get_open_positions()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_sym = pos.get("instrumentName") or pos.get("symbol") or pos.get("name") or ""
                if pos_sym.replace("/", "").upper() == pair.replace("/", "").upper():
                    pos_id = pos.get("positionId") or pos.get("id")
                    if pos_id:
                        client.modify_position(pos_id, pos, new_sl)
    
    _system_status["cron"]["successful_runs"] += 1
    _system_status["cron"]["last_status"] = "success"
    log_event("cron", "job_complete", {"signals_processed": signals_found}, "success")
    return True

# =====================================================================
# DASHBOARD MODE
# =====================================================================

def generate_dashboard():
    global _system_status
    _system_status["dashboard"]["last_update"] = datetime.now(timezone.utc).isoformat()
    _system_status["dashboard"]["status"] = "updating"
    
    client = TradeLockerClient()
    connected = client.auth()
    error = None
    
    if not connected:
        error = "Failed to authenticate. Check TL_EMAIL, TL_PASSWORD, TL_SERVER"
        _system_status["dashboard"]["status"] = "error"
    else:
        client.load_instruments()
        _system_status["dashboard"]["status"] = "online"
    
    html = generate_dashboard_html(client, connected, error)
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w") as f:
        f.write(html)
    log_event("dashboard", "generated", {"connected": connected})
    return True

# =====================================================================
# MAIN
# =====================================================================

if __name__ == "__main__":
    try:
        if MODE == "dashboard":
            generate_dashboard()
        else:
            run_bot()
        sys.exit(0)
    except Exception as e:
        log_event("system", "fatal_error", {"error": str(e)}, "error")
        print(f"ERROR: {e}")
        sys.exit(1)
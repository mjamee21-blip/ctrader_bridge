#!/usr/bin/python3
# -*- coding: utf-8 -*-
# TradeLocker Telegram Bridge - Minimal Cron Edition
# No file I/O, no logs, pure API hub

import os, json, re, urllib.request, urllib.parse, sys
from urllib.error import HTTPError
from datetime import datetime, timezone
import ssl

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass

# Config from GitHub Secrets only
TL_EMAIL = os.environ.get("TL_EMAIL", "")
TL_PASSWORD = os.environ.get("TL_PASSWORD", "")
TL_SERVER = os.environ.get("TL_SERVER", "TradeLocker-Demo")
TL_ACCOUNT_ID_STR = os.environ.get("TL_ACCOUNT_ID", "0").strip()
TL_ACCOUNT_ID = int(TL_ACCOUNT_ID_STR) if TL_ACCOUNT_ID_STR else 0
TL_ACC_NUM_STR = os.environ.get("TL_ACC_NUM", "1").strip()
TL_ACC_NUM = int(TL_ACC_NUM_STR) if TL_ACC_NUM_STR else 1
TL_ENV = os.environ.get("TL_ENV", "demo")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")
TL_PAIR_MAP_JSON = os.environ.get("TL_PAIR_MAP", "{}")
TL_DEFAULT_QTY_STR = os.environ.get("TL_DEFAULT_QTY", "0.10").strip()
DEFAULT_QTY = float(TL_DEFAULT_QTY_STR) if TL_DEFAULT_QTY_STR else 0.10

try:
    PAIR_MAP = json.loads(TL_PAIR_MAP_JSON)
except:
    PAIR_MAP = {}

TL_BASE = "https://live.tradelocker.com" if TL_ENV.lower() == "live" else "https://demo.tradelocker.com"
_last_update_id = 0
_instruments = {}

def tg_get_messages(offset=0):
    global _last_update_id
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset+1}&timeout=3&limit=100"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            if not result.get("ok"):
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
                    messages.append({"chat_id": chat_id, "message_id": msg.get("message_id"), "text": text})
            return messages
    except:
        return []

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

class TradeLockerClient:
    def __init__(self):
        self.base_url = f"{TL_BASE}/backend-api"
        self.token = None
        self.authenticated = False
    
    def _req(self, method, path, body=None, headers_extra=None, timeout=20):
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
                return json.loads(content) if content else {}
        except HTTPError as e:
            return {"error": f"HTTP_{e.code}"}
        except:
            return {"error": "request_failed"}
    
    def auth(self):
        payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}
        result = self._req("POST", "/auth/jwt/token", body=payload)
        if result.get("error"):
            return False
        self.token = result.get("accessToken") or result.get("access_token") or result.get("token")
        if not self.token:
            return False
        self.authenticated = True
        return True
    
    def load_instruments(self):
        global _instruments
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/instruments", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return False
        data = result.get("d", result)
        instruments = data.get("instruments") or data.get("data") or data.get("items") or []
        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = (inst.get("name") or inst.get("symbol") or "").upper()
            inst_id = inst.get("tradableInstrumentId") or inst.get("id") or ""
            route_id = None
            for route in (inst.get("routes") or []):
                if route.get("type") == "TRADE":
                    route_id = route.get("id")
                    break
            if name:
                _instruments[name] = {"id": inst_id, "route_id": route_id}
        return len(_instruments) > 0
    
    def find_instrument(self, pair_name):
        mapped = PAIR_MAP.get(pair_name, "").upper()
        if mapped and mapped in _instruments:
            return _instruments[mapped]
        normalized = pair_name.replace("/", "").upper()
        if normalized in _instruments:
            return _instruments[normalized]
        for name, info in _instruments.items():
            if normalized in name or name in normalized:
                return info
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
        if not qty:
            qty = DEFAULT_QTY
        
        instrument = self.find_instrument(pair)
        if not instrument:
            self.load_instruments()
            instrument = self.find_instrument(pair)
        if not instrument:
            return False
        
        route_id = instrument.get("route_id")
        inst_id = instrument["id"]
        side = "buy" if direction.upper() == "BUY" else "sell"
        
        payload = {"tradableInstrumentId": inst_id, "type": "market", "validity": "IOC", "side": side, "qty": qty}
        if sl:
            payload["stopLoss"] = sl
        if tp:
            payload["takeProfit"] = tp
        if route_id:
            payload["routeId"] = str(route_id)
        
        result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)
        
        if result.get("error") or result.get("s") == "error":
            errmsg = result.get("errmsg") or result.get("error") or ""
            if "forbidden" in str(errmsg).lower() and "route" in str(errmsg).lower():
                quote = self.get_quote(inst_id)
                if quote:
                    mid = (quote["bp"] + quote["ap"]) / 2.0
                    offset = max(mid * 0.0015, 0.5)
                    limit_price = round(quote["ap"] + offset, 5) if side == "buy" else round(quote["bp"] - offset, 5)
                    lim_payload = {"tradableInstrumentId": inst_id, "type": "limit", "side": side, "qty": qty, "price": limit_price}
                    if sl:
                        lim_payload["stopLoss"] = sl
                    if tp:
                        lim_payload["takeProfit"] = tp
                    if route_id:
                        lim_payload["routeId"] = str(route_id)
                    result = self._req("POST", f"/trade/accounts/{TL_ACCOUNT_ID}/orders", body=lim_payload, headers_extra={"accNum": str(TL_ACC_NUM)}, timeout=25)
            if result.get("error") or result.get("s") == "error":
                return False
        
        return True
    
    def get_open_positions(self):
        result = self._req("GET", f"/trade/accounts/{TL_ACCOUNT_ID}/positions", headers_extra={"accNum": str(TL_ACC_NUM)})
        if result.get("error"):
            return []
        data = result.get("d", result)
        if isinstance(data, list):
            return data
        return data.get("positions") or data.get("data") or data.get("items") or []
    
    def close_position(self, pos_id):
        result = self._req("DELETE", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", headers_extra={"accNum": str(TL_ACC_NUM)})
        return not result.get("error")
    
    def modify_position(self, pos_id, pos, new_sl):
        payload = {"stopLoss": new_sl}
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
        if result.get("error"):
            result = self._req("PATCH", f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{pos_id}", body=payload, headers_extra={"accNum": str(TL_ACC_NUM)})
        return not result.get("error")

def pair_matches(pos_symbol, pair):
    a = re.sub(r"[^A-Z0-9]", "", str(pos_symbol).upper())
    b = re.sub(r"[^A-Z0-9]", "", str(pair).upper())
    return bool(a and b and (a == b or b in a or a in b))

def main():
    global _last_update_id
    
    # Validate required secrets
    if not TL_EMAIL:
        print("ERROR: TL_EMAIL not set")
        return False
    if not TL_PASSWORD:
        print("ERROR: TL_PASSWORD not set")
        return False
    if not TG_TOKEN:
        print("ERROR: TG_TOKEN not set")
        return False
    if not TG_CHAT:
        print("ERROR: TG_CHAT not set")
        return False
    if TL_ACCOUNT_ID == 0:
        print("ERROR: TL_ACCOUNT_ID not set or invalid")
        return False
    
    client = TradeLockerClient()
    if not client.auth():
        return False
    
    client.load_instruments()
    
    messages = tg_get_messages(offset=_last_update_id)
    
    for msg in messages:
        text = (msg.get("text") or "").strip()
        if not looks_like_signal(text):
            continue
        
        parsed = parse_signal(text)
        if not parsed:
            continue
        
        if parsed["type"] == "SIGNAL":
            direction = parsed["direction"]
            pair = parsed["pair"]
            sl = parsed["sl"]
            tp = parsed["tp"]
            client.place_order(pair, direction, sl, tp)
        
        elif parsed["type"] == "TPSL_HIT":
            pair = parsed["pair"]
            positions = client.get_open_positions()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                pos_sym = pos.get("instrumentName") or pos.get("symbol") or pos.get("name") or ""
                if not pair_matches(pos_sym, pair):
                    continue
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
                if not pair_matches(pos_sym, pair):
                    continue
                pos_id = pos.get("positionId") or pos.get("id")
                if pos_id:
                    client.modify_position(pos_id, pos, new_sl)
    
    return True

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except:
        sys.exit(1)

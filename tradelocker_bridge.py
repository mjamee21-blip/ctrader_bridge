#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
TradeLocker Telegram Bridge - Lightweight Stateless Cron
========================================================
Pull Telegram signals and execute them on TradeLocker.
No logs. No state files. No signal saving. Fully stateless.
"""

import os
import sys
import json
import re
import time
import html
from datetime import datetime, timezone
from curl_cffi import requests as curl_requests

# =====================================================
# CONFIG
# =====================================================

TL_EMAIL        = os.environ.get("TL_EMAIL", "")
TL_PASSWORD     = os.environ.get("TL_PASSWORD", "")
TL_SERVER       = os.environ.get("TL_SERVER", "")
TL_ACCOUNT_ID   = os.environ.get("TL_ACCOUNT_ID", "")
TL_ACC_NUM      = os.environ.get("TL_ACC_NUM", "1")
TL_ENV          = os.environ.get("TL_ENV", "demo")
TL_BASE_URL     = os.environ.get("TL_BASE_URL", "")
TG_TOKEN        = os.environ.get("TG_TOKEN", "")
TG_CHAT         = os.environ.get("TG_CHAT", "")
DEFAULT_QTY     = float(os.environ.get("TL_DEFAULT_QTY", "0.10"))

if not TL_BASE_URL:
    TL_BASE_URL = "https://demo.tradelocker.com" if TL_ENV == "demo" else "https://live.tradelocker.com"

AUTH_URL    = f"{TL_BASE_URL}/backend-api/auth/jwt/token"
API_BASE    = f"{TL_BASE_URL}/backend-api"
TG_API      = f"https://api.telegram.org/bot{TG_TOKEN}"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://demo.tradelocker.com/",
    "Origin": "https://demo.tradelocker.com/",
    "content-type": "application/json",
}

# =====================================================
# TRADELOCKER API
# =====================================================

_auth_token = None
_auth_expires_at = 0.0

def _impersonate_kwargs():
    return {"impersonate": "chrome120"}

def authenticate():
    global _auth_token, _auth_expires_at

    if _auth_token and time.time() < _auth_expires_at - 60:
        return _auth_token

    headers = dict(BROWSER_HEADERS)
    payload = {"email": TL_EMAIL, "password": TL_PASSWORD, "server": TL_SERVER}

    try:
        resp = curl_requests.post(AUTH_URL, json=payload, headers=headers, timeout=20, **_impersonate_kwargs())
        data = resp.json() if resp.text.strip() else {}
    except Exception as exc:
        print(f"AUTH FAILED: {str(exc)[:120]}")
        return None

    if resp.status_code != 200 or data.get("error"):
        detail = data.get("detail", data.get("error", f"HTTP {resp.status_code}"))
        print(f"AUTH FAILED: HTTP {resp.status_code} {str(detail)[:120]}")
        return None

    _auth_token = data.get("accessToken") or data.get("access_token") or data.get("token")
    if not _auth_token:
        print("AUTH FAILED: no access token in response")
        return None

    expires_in = data.get("expiresIn") or data.get("expires_in") or 3600
    _auth_expires_at = time.time() + int(expires_in)
    print("AUTH OK")
    return _auth_token


def tl_get(path, extra_headers=None):
    token = authenticate()
    if not token:
        return {}
    h = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = curl_requests.get(f"{API_BASE}{path}", headers=h, timeout=15, **_impersonate_kwargs())
        return resp.json() if resp.text.strip() else {}
    except Exception as exc:
        print(f"TL GET {path} failed: {str(exc)[:80]}")
        return {"error": str(exc)[:100]}


def tl_post(path, body, extra_headers=None):
    token = authenticate()
    if not token:
        return {"error": "not authenticated"}
    h = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json",
    }
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = curl_requests.post(f"{API_BASE}{path}", json=body, headers=h, timeout=25, **_impersonate_kwargs())
        return resp.json() if resp.text.strip() else {}
    except Exception as exc:
        print(f"TL POST {path} failed: {str(exc)[:80]}")
        return {"error": str(exc)[:100]}


def tl_put(path, body, extra_headers=None):
    token = authenticate()
    if not token:
        return {"error": "not authenticated"}
    h = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json",
    }
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = curl_requests.put(f"{API_BASE}{path}", json=body, headers=h, timeout=25, **_impersonate_kwargs())
        return resp.json() if resp.text.strip() else {}
    except Exception as exc:
        print(f"TL PUT {path} failed: {str(exc)[:80]}")
        return {"error": str(exc)[:100]}


def tl_delete(path, extra_headers=None):
    token = authenticate()
    if not token:
        return {"error": "not authenticated"}
    h = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = curl_requests.delete(f"{API_BASE}{path}", headers=h, timeout=20, **_impersonate_kwargs())
        return resp.json() if resp.text.strip() else {}
    except Exception as exc:
        print(f"TL DELETE {path} failed: {str(exc)[:80]}")
        return {"error": str(exc)[:100]}


# =====================================================
# TELEGRAM
# =====================================================

def tg_get_updates(limit=20, offset=None):
    if not TG_TOKEN:
        return []
    params = {"timeout": 1, "limit": limit}
    if offset:
        params["offset"] = offset
    try:
        resp = curl_requests.get(f"{TG_API}/getUpdates", params=params, timeout=15, **_impersonate_kwargs())
        data = resp.json() if resp.text.strip() else {}
        return data.get("result", [])
    except Exception as exc:
        print(f"TG getUpdates failed: {str(exc)[:80]}")
        return []


def tg_delete_webhook():
    if not TG_TOKEN:
        return
    try:
        curl_requests.get(f"{TG_API}/deleteWebhook", params={"drop_pending_updates": False}, timeout=10, **_impersonate_kwargs())
    except Exception:
        pass


# =====================================================
# SIGNAL PARSER
# =====================================================

def _looks_like_signal(text):
    if not text:
        return False
    t = text.upper()
    return ("BUY" in t or "SELL" in t or "CLOSE" in t or
            "TP HIT" in t or "SL HIT" in t or
            "#SL_UPDATE" in t or "SL UPDATE" in t or "NEW SL" in t)


def _pair_matches(pos_symbol, pair):
    a = re.sub(r"[^A-Z0-9]", "", str(pos_symbol).upper())
    b = re.sub(r"[^A-Z0-9]", "", str(pair).upper())
    if not a or not b:
        return False
    return a == b or b in a or a in b


def parse_signal(text):
    if not text or not _looks_like_signal(text):
        return None

    lines = text.strip().split("\n")
    first_line = lines[0].strip()

    # TP/SL HIT
    tpsl_m = re.search(r'(TP|SL)\s*HIT', first_line, re.IGNORECASE)
    if tpsl_m:
        pair = ""
        pm = re.search(r'(TP|SL)\s*HIT\s*[-–:]\s*(\S+)', first_line, re.IGNORECASE)
        if pm:
            pair = pm.group(2)
        if not pair:
            for line in lines[1:]:
                m = re.search(r'([A-Z]{3,6}[/]?[A-Z]{0,6})', line.strip())
                if m:
                    pair = m.group(1)
                    break
        return {"type": "TPSL_HIT", "result": tpsl_m.group(1).upper(), "pair": pair.upper()}

    # SL UPDATE
    upper_text = text.upper()
    if "#SL_UPDATE" in upper_text or "SL UPDATE" in upper_text or "NEW SL" in upper_text:
        pair = new_sl = None
        for line in lines:
            cl = line.strip()
            if not cl:
                continue
            if cl.upper().startswith("PAIR"):
                parts = cl.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    pair = parts[1].strip()
                continue
            m = re.search(r'New\s*SL\s*[:=]?\s*([\d.]+)', cl, re.IGNORECASE)
            if not m:
                m = re.search(r'(?<![A-Za-z])SL\s*[:=]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    new_sl = float(m.group(1))
                except ValueError:
                    pass
        if not pair:
            for line in lines:
                cl = line.strip()
                if "SL_UPDATE" in cl.upper() or "SL UPDATE" in cl.upper():
                    sym = re.search(r'\b([A-Z]{3,6}\s*/?\s*[A-Z]{0,6})\b',
                                    cl.upper().replace("SL_UPDATE", "").replace("SL UPDATE", ""))
                    if sym and sym.group(1).strip():
                        pair = sym.group(1).strip()
                        break
        if pair and new_sl is not None:
            return {"type": "SL_UPDATE", "pair": pair.upper(), "new_sl": new_sl}
        return None

    # TRADE SIGNAL
    sig_m = re.search(r'\b(BUY|SELL|CLOSE)\s+([A-Za-z0-9/_-]+)', first_line, re.IGNORECASE)
    if not sig_m:
        return None

    direction = sig_m.group(1).upper()
    pair = sig_m.group(2).upper()
    ref_price = sl = tp = rr = None

    for line in lines:
        cl = re.sub(r'<[^>]+>', '', line).strip()
        if not cl:
            continue
        if ref_price is None:
            m = re.search(r'(?:Entry|Ref)\s*[:=]\s*(?:MARKET|NOW|LIMIT)?\s*\(?\s*(?:Ref\s*[:=]\s*)?([\d.]+)\s*\)?', cl, re.IGNORECASE)
            if m:
                try:
                    ref_price = float(m.group(1))
                except ValueError:
                    pass
        if sl is None:
            m = re.search(r'(?<![A-Za-z])SL\s*[:\s]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    sl = float(m.group(1))
                except ValueError:
                    pass
        if tp is None:
            m = re.search(r'(?<![A-Za-z])TP\s*[:\s]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                try:
                    tp = float(m.group(1))
                except ValueError:
                    pass
        if rr is None:
            m = re.search(r'RR\s*[:\s]\s*([\d.]+)\s*[:/]\s*([\d.]+)', cl, re.IGNORECASE)
            if m:
                rr = f"{m.group(1)}:{m.group(2)}"

    return {
        "type": "SIGNAL",
        "direction": direction,
        "pair": pair,
        "ref_price": ref_price,
        "sl": sl,
        "tp": tp,
        "rr": rr,
    }


# =====================================================
# TRADELOCKER HELPERS
# =====================================================

_instruments_cache = None
_instruments_ts = 0
_INSTRUMENTS_TTL = 300

def load_instruments():
    global _instruments_cache, _instruments_ts
    if _instruments_cache and (time.time() - _instruments_ts) < _INSTRUMENTS_TTL:
        return _instruments_cache
    data = tl_get("/trade/instruments")
    instruments = {}
    if isinstance(data, list):
        for inst in data:
            name = inst.get("name", "").upper()
            if name:
                instruments[name] = inst
                instruments[name.replace("/", "")] = inst
    elif isinstance(data, dict):
        for inst in data.get("d", data).get("instruments", []):
            name = inst.get("name", "").upper()
            if name:
                instruments[name] = inst
                instruments[name.replace("/", "")] = inst
    _instruments_cache = instruments
    _instruments_ts = time.time()
    return instruments


def find_instrument(pair):
    pair = pair.upper()
    instruments = load_instruments()
    return instruments.get(pair)


def _tl_field(pos, *keys):
    for k in keys:
        v = pos.get(k)
        if v not in (None, ""):
            return v
    return None


def get_positions():
    data = tl_get(f"/trade/accounts/{TL_ACCOUNT_ID}/positions", {"accNum": TL_ACC_NUM})
    if isinstance(data, list):
        return [p for p in data if isinstance(p, dict)]
    if isinstance(data, dict):
        return data.get("d", data.get("positions", []))
    return []


def get_history():
    data = tl_get(f"/trade/accounts/{TL_ACCOUNT_ID}/ordersHistory", {"accNum": TL_ACC_NUM})
    if isinstance(data, list):
        return data[:50]
    if isinstance(data, dict):
        return data.get("d", data.get("orders", []))[:50]
    return []


def get_account_info():
    return tl_get("/auth/jwt/all-accounts")


# =====================================================
# ORDER EXECUTION
# =====================================================

def place_market(pair, direction, sl, tp, quantity=None):
    if quantity is None:
        quantity = DEFAULT_QTY

    instrument = find_instrument(pair)
    if not instrument:
        print(f"Instrument not found: {pair}")
        return False, {"error": f"Instrument not found: {pair}"}

    route_id = instrument.get("route_id_trade")
    inst_id  = instrument["id"]
    side     = "buy" if direction.upper() == "BUY" else "sell"
    headers  = {"accNum": str(TL_ACC_NUM)}

    payload = {
        "tradableInstrumentId": inst_id,
        "type": "market",
        "validity": "IOC",
        "side": side,
        "qty": quantity,
    }
    if sl is not None and sl != 0:
        payload["stopLoss"] = float(sl)
    if tp is not None and tp != 0:
        payload["takeProfit"] = float(tp)
    if route_id:
        payload["routeId"] = str(route_id)

    result = tl_post(f"/trade/accounts/{TL_ACCOUNT_ID}/orders", payload, headers)

    errmsg = str(result.get("errmsg") or result.get("error") or "")
    is_error = (result.get("s") == "error" or (result.get("error") and result.get("s") != "ok"))
    forbidden = "forbidden" in errmsg.lower() and "route" in errmsg.lower()

    if is_error and forbidden:
        quote = get_quote(inst_id)
        if quote:
            mid = (quote["bp"] + quote["ap"]) / 2.0
            offset = max(mid * 0.0015, 0.5)
            limit_price = (round(quote["ap"] + offset, 5) if side == "buy" else round(quote["bp"] - offset, 5))
            lim_payload = dict(payload)
            lim_payload["type"] = "limit"
            lim_payload["price"] = limit_price
            result = tl_post(f"/trade/accounts/{TL_ACCOUNT_ID}/orders", lim_payload, headers)

    errmsg = str(result.get("errmsg") or result.get("error") or "")
    is_error = (result.get("s") == "error" or (result.get("error") and result.get("s") != "ok"))
    if is_error:
        print(f"Order FAILED {pair}: {errmsg}")
        return False, result

    d = result.get("d", {}) if isinstance(result.get("d"), dict) else {}
    order_id = d.get("orderId") or d.get("id") or d.get("order_id") or result.get("orderId") or result.get("id")
    print(f"Order placed {direction} {pair} qty={quantity} order_id={order_id}")

    if sl or tp:
        time.sleep(0.5)
        verify_and_set_sltp(pair, order_id, sl, tp)

    return True, {"order_id": order_id}


def get_quote(inst_id):
    data = tl_get(f"/trade/quotes?tradableInstrumentId={inst_id}", {"accNum": str(TL_ACC_NUM)})
    d = data.get("d", data)
    quotes = d.get("quotes") if isinstance(d, dict) else None
    q = quotes[0] if isinstance(quotes, list) and quotes else d if isinstance(d, dict) else data
    bp = q.get("bp") or q.get("bid")
    ap = q.get("ap") or q.get("ask")
    if bp is None or ap is None:
        return None
    return {"bp": float(bp), "ap": float(ap)}


def verify_and_set_sltp(pair, order_id, intended_sl, intended_tp):
    positions = get_positions()
    for pos in positions:
        pos_pair = _tl_field(pos, "instrumentName", "symbol", "name", "pair", "instrument")
        if not _pair_matches(pos_pair, pair):
            continue
        pos_id = _tl_field(pos, "positionId", "id", "position_id", "tradeId")
        if not pos_id:
            continue
        cur_sl = _to_float(_tl_field(pos, "stopLoss", "sl", "stop_loss", "stopLossPrice"))
        cur_tp = _to_float(_tl_field(pos, "takeProfit", "tp", "take_profit", "takeProfitPrice"))

        if (intended_sl and cur_sl != float(intended_sl)) or (intended_tp and cur_tp != float(intended_tp)):
            ok, _ = modify_position(pos_id, pos, intended_sl, intended_tp)
            if ok:
                print(f"SL/TP corrected on {pos_id}")
    return


def modify_position(position_id, pos=None, new_sl=None, new_tp=None):
    payload = {}
    if new_sl is not None:
        payload["stopLoss"] = float(new_sl)
    if new_tp is not None:
        payload["takeProfit"] = float(new_tp)
    if not payload:
        return True, {}

    if isinstance(pos, dict):
        qty = _tl_field(pos, "qty", "quantity", "size", "volume", "lots")
        if qty not in ("", None):
            try:
                payload["qty"] = float(qty)
            except (TypeError, ValueError):
                payload["qty"] = qty
        if new_tp is None:
            cur_tp = _tl_field(pos, "takeProfit", "tp", "take_profit", "takeProfitPrice")
            if cur_tp not in ("", None):
                try:
                    payload["takeProfit"] = float(cur_tp)
                except (TypeError, ValueError):
                    payload["takeProfit"] = cur_tp
        if new_sl is None:
            cur_sl = _tl_field(pos, "stopLoss", "sl", "stop_loss", "stopLossPrice")
            if cur_sl not in ("", None):
                try:
                    payload["stopLoss"] = float(cur_sl)
                except (TypeError, ValueError):
                    payload["stopLoss"] = cur_sl

    result = tl_put(f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{position_id}", payload, {"accNum": str(TL_ACC_NUM)})
    if result.get("error") and str(result.get("error")) in ("HTTP_405", "HTTP_404", "HTTP_400"):
        result = tl_post(f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{position_id}", payload, {"accNum": str(TL_ACC_NUM)})

    if result.get("error") or result.get("s") == "error":
        print(f"Modify position {position_id} failed: {result}")
        return False, result
    print(f"Position {position_id} modified OK")
    return True, result


def close_position(position_id):
    result = tl_delete(f"/trade/accounts/{TL_ACCOUNT_ID}/positions/{position_id}", {"accNum": str(TL_ACC_NUM)})
    if result.get("error"):
        print(f"Close position {position_id} failed: {result}")
        return False, result
    print(f"Position {position_id} closed OK")
    return True, result


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# =====================================================
# CRON RUNNER
# =====================================================

def run_cron():
    if not all([TL_EMAIL, TL_PASSWORD, TL_SERVER, TL_ACCOUNT_ID, TG_TOKEN, TG_CHAT]):
        print("Missing required env vars")
        return False

    token = authenticate()
    if not token:
        return False

    load_instruments()

    updates = tg_get_updates(limit=20)
    if not updates:
        print("No new Telegram updates")
        return True

    print(f"Processing {len(updates)} Telegram updates")

    for update in updates:
        msg = update.get("message") or update.get("channel_post")
        if not msg:
            continue
        chat_id = str(msg.get("chat", {}).get("id", ""))
        if chat_id != str(TG_CHAT):
            continue

        text = (msg.get("text") or "").strip()
        if not _looks_like_signal(text):
            continue

        parsed = parse_signal(text)
        if not parsed:
            continue

        if parsed["type"] == "SIGNAL":
            direction = parsed["direction"]
            if direction == "CLOSE":
                positions = get_positions()
                for pos in positions:
                    pos_pair = _tl_field(pos, "instrumentName", "symbol", "name", "pair", "instrument")
                    if _pair_matches(pos_pair, parsed["pair"]):
                        pos_id = _tl_field(pos, "positionId", "id", "position_id", "tradeId")
                        if pos_id:
                            close_position(pos_id)
            else:
                place_market(parsed["pair"], parsed["direction"], parsed.get("sl"), parsed.get("tp"))

        elif parsed["type"] == "TPSL_HIT":
            positions = get_positions()
            for pos in positions:
                pos_pair = _tl_field(pos, "instrumentName", "symbol", "name", "pair", "instrument")
                if _pair_matches(pos_pair, parsed["pair"]):
                    pos_id = _tl_field(pos, "positionId", "id", "position_id", "tradeId")
                    if pos_id:
                        close_position(pos_id)

        elif parsed["type"] == "SL_UPDATE":
            positions = get_positions()
            for pos in positions:
                pos_pair = _tl_field(pos, "instrumentName", "symbol", "name", "pair", "instrument")
                if _pair_matches(pos_pair, parsed["pair"]):
                    pos_id = _tl_field(pos, "positionId", "id", "position_id", "tradeId")
                    if pos_id:
                        modify_position(pos_id, pos=pos, new_sl=parsed["new_sl"])

    return True


# =====================================================
# DASHBOARD
# =====================================================

def render_dashboard():
    print("Content-Type: text/html; charset=utf-8")
    print()

    auth_error = None
    account = None
    positions = []
    history = []

    try:
        token = authenticate()
        if token:
            account = get_account_info()
            positions = get_positions()
            history = get_history()
        else:
            auth_error = "Authentication failed"
    except Exception as exc:
        auth_error = str(exc)[:120]

    def esc(s):
        return html.escape(str(s)) if s is not None else ""

    rows_pos = ""
    for p in positions:
        rows_pos += f"<tr><td>{esc(p.get('instrumentName', _tl_field(p, 'symbol', 'pair')))}</td>"
        rows_pos += f"<td>{esc(p.get('side', ''))}</td>"
        rows_pos += f"<td>{esc(p.get('qty', p.get('quantity', '')))}</td>"
        rows_pos += f"<td>{esc(p.get('price', ''))}</td>"
        rows_pos += f"<td>{esc(p.get('stopLoss', ''))}</td>"
        rows_pos += f"<td>{esc(p.get('takeProfit', ''))}</td>"
        rows_pos += f"<td>{esc(p.get('pnl', p.get('unrealizedPnL', '')))}</td>"
        rows_pos += f"<td>{esc(p.get('openTime', ''))}</td></tr>"

    rows_hist = ""
    for h in history[:30]:
        rows_hist += f"<tr><td>{esc(h.get('instrumentName', _tl_field(h, 'symbol', 'pair')))}</td>"
        rows_hist += f"<td>{esc(h.get('side', ''))}</td>"
        rows_hist += f"<td>{esc(h.get('qty', h.get('quantity', '')))}</td>"
        rows_hist += f"<td>{esc(h.get('price', h.get('avgPrice', '')))}</td>"
        rows_hist += f"<td>{esc(h.get('status', ''))}</td>"
        rows_hist += f"<td>{esc(h.get('realizedPnL', h.get('pnl', '')))}</td>"
        rows_hist += f"<td>{esc(h.get('closedAt', h.get('placedAt', '')))}</td></tr>"

    acc_balance = ""
    acc_equity = ""
    if isinstance(account, list) and account:
        acc = account[0]
        acc_balance = esc(acc.get("accountBalance", acc.get("balance", "")))
        acc_equity = esc(acc.get("equity", acc.get("accountEquity", "")))
    elif isinstance(account, dict):
        acc_balance = esc(account.get("accountBalance", account.get("balance", "")))
        acc_equity = esc(account.get("equity", account.get("accountEquity", "")))

    status_color = "red" if auth_error else "green"
    status_text = esc(auth_error) if auth_error else "Authenticated"

    print(f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeLocker Bridge</title>
<style>
:root{{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--text2:#8b949e;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:13px;line-height:1.4}}
.wrap{{max-width:1200px;margin:0 auto;padding:10px}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:10px}}
.card h2{{font-size:14px;margin-bottom:8px;color:#fff}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th,td{{text-align:left;padding:6px;border-bottom:1px solid var(--border)}}
th{{color:var(--text2);font-weight:600}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.stat{{padding:10px;text-align:center}}
.stat-v{{font-size:20px;font-weight:800}}
.dot{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}}
</style></head><body><div class="wrap">
<div class="card"><h2><span class="dot" style="background:{status_color}"></span>Status: {status_text}</h2>
<div class="g2">
<div class="stat"><div class="stat-v" style="color:var(--blue)">Balance</div><div>{acc_balance}</div></div>
<div class="stat"><div class="stat-v" style="color:var(--green)">Equity</div><div>{acc_equity}</div></div>
</div></div>
<div class="g2">
<div class="card"><h2>Open Positions ({len(positions)})</h2>
<table><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Price</th><th>SL</th><th>TP</th><th>PnL</th><th>Open Time</th></tr>
{rows_pos or '<tr><td colspan="8">No open positions</td></tr>'}
</table></div>
<div class="card"><h2>Recent Orders ({min(len(history), 30)})</h2>
<table><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Price</th><th>Status</th><th>PnL</th><th>Time</th></tr>
{rows_hist or '<tr><td colspan="7">No orders</td></tr>'}
</table></div>
</div>
</div></body></html>""")


# =====================================================
# ENTRYPOINT
# =====================================================

if __name__ == "__main__":
    if "--dashboard" in sys.argv or (len(sys.argv) > 1 and sys.argv[1].lower() == "dashboard"):
        render_dashboard()
    else:
        success = run_cron()
        sys.exit(0 if success else 1)

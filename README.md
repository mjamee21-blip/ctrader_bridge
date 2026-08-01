# cTrader FIX API Bot & Dashboard

A Telegram trading bot that connects directly to cTrader via the **FIX API**, plus an auto-updating dashboard.

## ✨ Features
- 🔌 **cTrader FIX API Connection** — Secure SSL TCP connection (`demo-uk-eqx-01.p.c-trader.com:5212`) using FIX 4.4 protocol, Logon (`35=A`), and automated heartbeats.
- ⚡ **Market Execution** — Places market orders (`35=D` New Order Single) immediately from Telegram signals with SL and TP.
- 📊 **Dashboard & State Sync** — Generates [`docs/system_state.json`](docs/system_state.json) and [`docs/index.html`](docs/index.html) for live monitoring.

## 📦 Setup Instructions

### Step 1: Add Repository Secrets
Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Required | Description | Example |
|--------|----------|-------------|---------|
| `FIX_HOST` | ✅ | FIX Host name | `demo-uk-eqx-01.p.c-trader.com` |
| `FIX_TRADE_PORT` | ✅ | Trade Port (SSL) | `5212` |
| `FIX_SENDER_COMP_ID` | ✅ | SenderCompID | `demo.deriv.2454444` |
| `FIX_TARGET_COMP_ID` | ✅ | TargetCompID | `cServer` |
| `FIX_SENDER_SUB_ID` | ✅ | SenderSubID | `TRADE` |
| `FIX_PASSWORD` | ✅ | cTrader FIX API Password | `your_fix_password` |
| `TG_TOKEN` | ✅ | Telegram bot token | `123456:ABC-DEF...` |
| `TG_CHAT` | ✅ | Telegram chat/username or `ANY` | `@mychannel` |

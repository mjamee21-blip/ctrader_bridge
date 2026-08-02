# cTrader FIX API Bot & Dashboard

A Telegram trading bot that connects directly to cTrader via the industry-standard **FIX API (Financial Information Exchange v4.4)**, designed for advanced trading robots, algorithmic execution, and real-time state synchronization.

## ✨ Features & Alignment with cTrader FIX API Specifications
- 🔌 **FIX 4.4 Protocol Connection** — Secure SSL TCP connection (`demo-uk-eqx-01.p.c-trader.com:5212`) adhering to the official cTrader FIX API Getting Started specifications (Logon [`35=A`](ctrader_bot.py:362), Session management, and heartbeats).
- ⚡ **Advanced Trading Robot / Algorithmic Execution** — Instantly parses Telegram signals and executes market orders (`35=D` New Order Single) with Stop Loss and Take Profit.
- 📊 **Dashboard & State Sync** — Generates [`docs/system_state.json`](docs/system_state.json) and [`docs/index.html`](docs/index.html) for live institutional-grade monitoring.

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

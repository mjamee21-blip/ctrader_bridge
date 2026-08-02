# cTrader OpenAPI Trading Bot & Dashboard

A Telegram trading bot that connects directly to cTrader via **cTrader OpenAPI (WebSocket)**, designed for algorithmic execution without IP whitelisting restrictions.

## ✨ Features
- 🔌 **cTrader OpenAPI Connection** — Secure WebSocket connection to cTrader OpenAPI (`wss://demo.ctraderapi.com:5035` or `wss://live.ctraderapi.com:5035`) with OAuth token authentication and application authorization.
- ⚡ **Algorithmic Execution** — Parses Telegram signals and executes market orders with Stop Loss and Take Profit.
- 📊 **Dashboard & State Sync** — Generates [`docs/system_state.json`](docs/system_state.json) and [`docs/index.html`](docs/index.html) for live monitoring.

## 📦 Setup Instructions

### Step 1: Add Repository Secrets
Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Required | Description | Example |
|--------|----------|-------------|---------|
| `CT_CLIENT_ID` | ✅ | cTrader OpenAPI Client ID | `your_client_id` |
| `CT_CLIENT_SECRET` | ✅ | cTrader OpenAPI Client Secret | `your_client_secret` |
| `CT_ACCESS_TOKEN` | ✅ | cTrader OpenAPI Access Token | `your_access_token` |
| `CT_ACCOUNT_ID` | ✅ | cTrader Account ID (numeric) | `12345678` |
| `CT_ENV` | Optional | Environment (`demo` or `live`) | `demo` |
| `TG_TOKEN` | ✅ | Telegram bot token | `123456:ABC-DEF...` |
| `TG_CHAT` | ✅ | Telegram chat/username or `ANY` | `@mychannel` |

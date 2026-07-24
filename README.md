# cTrader Bot + Dashboard

A Telegram trading bot that connects to cTrader Open API, plus an auto-updating dashboard with live connection status and a "Fetch Now" button.

## ✨ Features

### Dashboard
- 📊 **cTrader connection status** (Connected/Disconnected with server info)
- ✈️ **Telegram connection status** (Connected/Disconnected with bot username)
- 🔄 **"Fetch Now" button** — triggers an immediate dashboard refresh via GitHub API
- 📈 **Open positions table** — shows Entry Price, SL, TP, and live P&L for each trade
- 🏁 **Closed trades table** — shows Entry, Exit, P&L, and result
- 📋 **Recent orders table** — last 20 orders with status
- 💰 **Account overview** — Balance, Equity, Margin, Free Margin, Leverage

### Bot
- ⚡ **Market execution** — places orders immediately, ignoring signal entry price
- 🎯 **Accurate SL/TP** — extracts and places Stop Loss and Take Profit
- 🔧 **SL Update handling** — adjusts existing position SL when `#SL_UPDATE` signal received
- 📝 **Detailed logging** — every step is logged for debugging in GitHub Actions

## 📦 Setup Instructions

### Step 1: Add Repository Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Required | Description | Example |
|--------|----------|-------------|---------|
| `CTRADER_CLIENT_ID` | ✅ | cTrader Client ID | `your_client_id` |
| `CTRADER_CLIENT_SECRET` | ✅ | cTrader Client Secret | `your_client_secret` |
| `CTRADER_ACCESS_TOKEN` | ✅ | cTrader Access Token | `your_access_token` |
| `CTRADER_ACCOUNT_ID` | ✅ | cTrader Account ID (numeric) | `12345678` |
| `CTRADER_ENV` | ✅ | Environment | `demo` or `live` |
| `TG_TOKEN` | ✅ | Telegram bot token | `123456:ABC-DEF...` |
| `TG_CHAT` | ✅ | Telegram chat/username or `ANY` | `@mychannel` |
| `GH_OWNER` | ❌ | GitHub username (for Fetch button) | `mjamee21-blip` |
| `GH_REPO` | ❌ | Repo name (for Fetch button) | `ctrader_bridge` |
| `GH_WORKFLOW` | ❌ | Workflow filename | `ctrader.yml` |

### Step 2: Enable GitHub Pages

1. Go to **Settings → Pages**
2. **Source**: `Deploy from a branch`
3. **Branch**: `gh-pages` → `/ (root)`
4. Click **Save**

### Step 3: Run the Workflow

1. Go to **Actions** tab
2. Click **"cTrader Bot & Dashboard"**
3. Click **"Run workflow"**

### Step 4: Access Your Dashboard

```
https://<your-username>.github.io/<your-repo-name>/
```

## 📨 Signal Format Examples

### Buy/Sell Signal
```
BUY XAUUSD
SL: 2000.00
TP: 2050.00
```
→ Bot places a **MARKET BUY** order with SL=2000, TP=2050

### Sell Signal
```
SELL EURUSD
SL: 1.0900
TP: 1.0800
```
→ Bot places a **MARKET SELL** order

# TradeLocker Bot + Dashboard (Enhanced)

A Telegram trading bot that connects to TradeLocker, plus an auto-updating dashboard with live connection status and a "Fetch Now" button.

## ✨ Features

### Dashboard
- 📊 **TradeLocker connection status** (Connected/Disconnected with server info)
- ✈️ **Telegram connection status** (Connected/Disconnected with bot username)
- 🔄 **"Fetch Now" button** — triggers an immediate dashboard refresh via GitHub API
- 📈 **Open positions table** — shows Entry Price, SL, TP, and live P&L for each trade
- 🏁 **Closed trades table** — shows Entry, Exit, P&L, and result (TP Hit / SL Hit)
- 📋 **Recent orders table** — last 20 orders with status
- 💰 **Account overview** — Balance, Equity, Margin, Free Margin, Margin Level

### Bot
- ⚡ **Market execution** — places orders immediately, IGNORES any entry/REF price in signal
- 🎯 **Accurate SL/TP** — extracts and places Stop Loss and Take Profit from signal
- 🔧 **SL Update handling** — adjusts existing position SL when `#SL_UPDATE` signal received
- 🔢 **Instrument ID resolution** — maps pair names (e.g. "XAUUSD") to TradeLocker's numeric `tradableInstrumentId`
- 🏷️ **Pair aliases** — common names like "GOLD" → "XAUUSD", "OIL" → "USOIL", etc.
- 📝 **Detailed logging** — every step is logged for debugging in GitHub Actions

## 🔧 How It Works

```
Telegram Signal → Bot Parses → Resolves Pair→Numeric ID → Places MARKET Order
                                                    ↓
                                     SL/TP Set Accurately from Signal

Dashboard:
GitHub Actions (every 30 min) → Python Connects to TL API → Generates HTML → Deploys to gh-pages
```

## 📦 Setup Instructions

### Step 1: Add Repository Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Required | Description | Example |
|--------|----------|-------------|---------|
| `TL_EMAIL` | ✅ | TradeLocker email | `you@example.com` |
| `TL_PASSWORD` | ✅ | TradeLocker password | `********` |
| `TL_SERVER` | ✅ | TradeLocker server | `TradeLocker-Demo` |
| `TL_ACCOUNT_ID` | ✅ | Account ID (numeric) | `12345` |
| `TL_ACC_NUM` | ✅ | Account number | `1` |
| `TL_ENV` | ✅ | Environment | `demo` or `live` |
| `TG_TOKEN` | ✅ | Telegram bot token | `123456:ABC-DEF...` |
| `TG_CHAT` | ✅ | Telegram chat/username or `ANY` | `@mychannel` |
| `TL_PAIR_MAP` | ❌ | Custom pair mapping (JSON) | `{"XAUUSD":"XAUUSD"}` |
| `TL_DEFAULT_QTY` | ❌ | Default trade quantity | `1.0` |
| `GH_OWNER` | ❌ | GitHub username (for Fetch button) | `mjamee21-blip` |
| `GH_REPO` | ❌ | Repo name (for Fetch button) | `tradelocker_bridge` |
| `GH_WORKFLOW` | ❌ | Workflow filename | `tradelocker.yml` |

### Step 2: Enable GitHub Pages

1. Go to **Settings → Pages**
2. **Source**: `Deploy from a branch`
3. **Branch**: `gh-pages` → `/ (root)`
4. Click **Save**

> ⚠️ **IMPORTANT**: GitHub Pages requires a **public** repository on the free plan. If your repo is private, either make it public or use Netlify/Vercel.

### Step 3: Run the Workflow

1. Go to **Actions** tab
2. Click **"TradeLocker Bot & Dashboard"**
3. Click **"Run workflow"**

### Step 4: Access Your Dashboard

```
https://<your-username>.github.io/<your-repo-name>/
```

## 🔄 "Fetch Now" Button Setup

The "Fetch Now" button triggers an immediate workflow run via the GitHub API. To use it:

1. Open the dashboard
2. Click the **⚙️** button
3. Enter your **GitHub Personal Access Token** (create one at [GitHub Settings → Tokens](https://github.com/settings/tokens) with `workflow` scope)
4. Enter your **username** and **repo name**
5. Click **Save**
6. Click **🔄 Fetch Now** — the dashboard will refresh in ~30 seconds

Your token is stored in `localStorage` (browser only) and never sent to any server except GitHub.

## 📨 Signal Format Examples

### Buy/Sell Signal
```
BUY XAUUSD
SL: 2000.00
TP: 2050.00
```
→ Bot places a **MARKET BUY** order with SL=2000, TP=2050 (ignores any entry price)

### Sell Signal
```
SELL EURUSD
SL: 1.0900
TP: 1.0800
```
→ Bot places a **MARKET SELL** order

### TP/SL Hit (Close Position)
```
TP HIT: XAUUSD
```
or
```
SL HIT: XAUUSD
```
→ Bot closes the matching position

### SL Update
```
#SL_UPDATE
PAIR: XAUUSD
New SL: 2020.00
```
→ Bot updates the SL on the existing XAUUSD position

## 🔢 Pair Name → Numeric ID Resolution

TradeLocker uses numeric instrument IDs. The bot automatically:
1. Loads ALL instruments on startup
2. Maps pair names → numeric IDs (e.g., `XAUUSD` → `12345`)
3. Uses the numeric ID when placing orders

If a pair isn't found directly, it checks:
1. Your custom `TL_PAIR_MAP`
2. Common aliases (GOLD→XAUUSD, OIL→USOIL, etc.)
3. Exact name match
4. Fuzzy (contains) match

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| 404 on dashboard | Make repo public OR use Netlify/Vercel. Enable Pages in Settings. |
| "Instrument not found" | Check the Actions log for available instrument names. Add to `TL_PAIR_MAP`. |
| Bot not placing orders | Check that `TG_CHAT` matches your channel/group username. |
| SL/TP not placed | Ensure signal format has `SL:` and `TP:` on separate lines. |
| "Route forbidden" error | Bot automatically falls back to a limit order near market price. |

## 📊 Logs

All bot actions are logged in GitHub Actions. Check the **Actions** tab → click a run → expand "Run Trading Bot" to see:
- Authentication status
- Instrument count and samples
- Signal parsing details
- Order placement results
- SL/TP values used
- Position modifications
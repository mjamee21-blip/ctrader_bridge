#!/usr/bin/env bash
# ============================================
# Render.com startup script for TradeLocker Bridge
# ============================================
# This runs the bridge in continuous mode.
# Render Background Workers run 24/7.
# The continuous_run() function polls Telegram
# every 5 seconds and processes signals.
#
# Environment variables are set in Render dashboard:
#   TL_EMAIL, TL_PASSWORD, TL_SERVER, TL_ACCOUNT_ID
#   TL_ACC_NUM, TL_ENV, TG_TOKEN, TG_CHAT, TL_PAIR_MAP
# ============================================

set -e

echo "🚀 Starting TradeLocker Bridge (continuous mode)..."
echo "   Environment: ${TL_ENV:-demo}"
echo "   Server: ${TL_SERVER:-TradeLocker-Demo}"
echo "   Account: ${TL_ACCOUNT_ID:-0}"

# Run the continuous bridge loop
python3 -c "
import traceback, sys
try:
    from tradelocker_bridge import continuous_run
    continuous_run()
except Exception as e:
    print('CRASH:', traceback.format_exc())
    sys.exit(1)
"
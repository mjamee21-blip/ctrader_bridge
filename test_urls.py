#!/usr/bin/env python3
"""
Local validation test for tradelocker_bridge.py URL handling.
Does not require real TradeLocker credentials.
"""

import os
import sys
import importlib.util

# Test 1: Verify module can be loaded without crashing
print("Test 1: Load module...")
spec = importlib.util.spec_from_file_location("tradelocker_bridge", "tradelocker_bridge.py")
mod = importlib.util.module_from_spec(spec)

# Set dummy env vars before loading
os.environ["TL_EMAIL"] = "test@example.com"
os.environ["TL_PASSWORD"] = "testpass"
os.environ["TL_SERVER"] = "TestServer"
os.environ["TL_ACCOUNT_ID"] = "123456"
os.environ["TL_ACC_NUM"] = "1"
os.environ["TL_ENV"] = "demo"
os.environ["TL_BASE_URL"] = "https://demo.tradelocker.com"
os.environ["TG_TOKEN"] = "123456:ABC-DEF"
os.environ["TG_CHAT"] = "-100123456"
os.environ["TL_DEFAULT_QTY"] = "0.10"

try:
    spec.loader.exec_module(mod)
    sys.modules["tradelocker_bridge"] = mod
    print("  PASS: module loaded")
except Exception as e:
    print(f"  FAIL: {e}")
    sys.exit(1)

# Test 2: Verify URL normalization with clean domain
print("Test 2: URL with clean domain...")
expected_auth = "https://demo.tradelocker.com/backend-api/auth/jwt/token"
expected_api = "https://demo.tradelocker.com/backend-api"
if mod.AUTH_URL == expected_auth and mod.API_BASE == expected_api:
    print(f"  PASS: AUTH_URL={mod.AUTH_URL}")
    print(f"        API_BASE={mod.API_BASE}")
else:
    print(f"  FAIL: expected AUTH_URL={expected_auth}, got {mod.AUTH_URL}")
    print(f"        expected API_BASE={expected_api}, got {mod.API_BASE}")
    sys.exit(1)

# Test 3: Verify URL normalization when TL_BASE_URL already includes /backend-api
print("Test 3: URL normalization with /backend-api suffix...")
os.environ["TL_BASE_URL"] = "https://demo.tradelocker.com/backend-api"
# Reload module to pick up new env var
import importlib
importlib.reload(mod)
expected_auth = "https://demo.tradelocker.com/backend-api/auth/jwt/token"
expected_api = "https://demo.tradelocker.com/backend-api"
if mod.AUTH_URL == expected_auth and mod.API_BASE == expected_api:
    print(f"  PASS: AUTH_URL={mod.AUTH_URL}")
    print(f"        API_BASE={mod.API_BASE}")
else:
    print(f"  FAIL: expected AUTH_URL={expected_auth}, got {mod.AUTH_URL}")
    print(f"        expected API_BASE={expected_api}, got {mod.API_BASE}")
    sys.exit(1)

# Test 4: Verify URL normalization with trailing slash and /backend-api
print("Test 4: URL normalization with trailing slash...")
os.environ["TL_BASE_URL"] = "https://demo.tradelocker.com/backend-api/"
importlib.reload(mod)
expected_auth = "https://demo.tradelocker.com/backend-api/auth/jwt/token"
expected_api = "https://demo.tradelocker.com/backend-api"
if mod.AUTH_URL == expected_auth and mod.API_BASE == expected_api:
    print(f"  PASS: AUTH_URL={mod.AUTH_URL}")
    print(f"        API_BASE={mod.API_BASE}")
else:
    print(f"  FAIL: expected AUTH_URL={expected_auth}, got {mod.AUTH_URL}")
    print(f"        expected API_BASE={expected_api}, got {mod.API_BASE}")
    sys.exit(1)

# Test 5: Verify TL_BASE_URL defaults work
print("Test 5: Default TL_BASE_URL from TL_ENV...")
os.environ.pop("TL_BASE_URL", None)
os.environ["TL_ENV"] = "live"
importlib.reload(mod)
if mod.TL_BASE_URL == "https://live.tradelocker.com":
    print(f"  PASS: TL_BASE_URL={mod.TL_BASE_URL}")
else:
    print(f"  FAIL: expected https://live.tradelocker.com, got {mod.TL_BASE_URL}")
    sys.exit(1)

print("\nAll tests passed!")

#!/usr/bin/env python3
"""
Local auth test using .env1 secrets.
Does not print secret values.
"""

import os
import sys
import traceback

# Load .env1 into environment
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env1")
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ[key] = value

# Now import the bridge module
try:
    import tradelocker_bridge as bridge
except Exception as e:
    print(f"FAIL: could not import module: {e}")
    traceback.print_exc()
    sys.exit(1)

print("Module loaded successfully")
print(f"TL_BASE_URL={bridge.TL_BASE_URL}")
print(f"TL_SERVER={bridge.TL_SERVER}")
print(f"TL_EMAIL set={bool(bridge.TL_EMAIL)}")
print(f"TL_PASSWORD set={bool(bridge.TL_PASSWORD)}")
print(f"AUTH_URL={bridge.AUTH_URL}")
print()

print("Attempting authentication...")
try:
    token = bridge.authenticate()
    if token:
        print(f"AUTH SUCCESS: token_len={len(token)}")
        sys.exit(0)
    else:
        print("AUTH FAILED: no token returned")
        sys.exit(1)
except Exception as e:
    print(f"AUTH CRASH: {e}")
    traceback.print_exc()
    sys.exit(1)

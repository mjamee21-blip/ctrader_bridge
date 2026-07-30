from pyrogram import Client

print("--- Pyrogram Session String Generator ---")
api_id = input("Enter your Telegram api_id (numeric): ").strip()
api_hash = input("Enter your Telegram api_hash (alphanumeric): ").strip()

try:
    api_id = int(api_id)
except ValueError:
    print("Error: api_id must be a number.")
    exit(1)

print("\nStarting Pyrogram client to generate session string...")
app = Client("my_session", api_id=api_id, api_hash=api_hash, in_memory=True)

with app:
    session_string = app.export_session_string()
    print("\nSUCCESS! 🎉 Here is your permanent session string:")
    print("-" * 60)
    print(session_string)
    print("-" * 60)
    print("Copy the entire string above and save it securely as your Telegram session secret.")

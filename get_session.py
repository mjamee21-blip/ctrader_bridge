import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    print("--- Telegram Telethon Session Generator ---")
    try:
        api_id_str = input("Enter your Telegram API_ID: ").strip()
        api_id = int(api_id_str)
    except ValueError:
        print("Invalid API_ID. Must be an integer.")
        return

    api_hash = input("Enter your Telegram API_HASH: ").strip()

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()
    
    session_string = client.session.save()
    print("\n[SUCCESS] Authentication successful!")
    print("\nYour Telethon Session String:")
    print("-" * 50)
    print(session_string)
    print("-" * 50)
    print("\nPlease copy the string above and save it into your GitHub Repository Secrets as TG_SESSION_STRING, along with TG_API_ID and TG_API_HASH.")

if __name__ == '__main__':
    asyncio.run(main())

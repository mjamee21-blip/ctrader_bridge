import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 38011181
API_HASH = 'f6147aae4bc47b08a58fb840ddf14502'
PHONE = '+251777770757'

async def main():
    print("--- Telegram Telethon Session Generator ---")
    print(f"Using API_ID: {API_ID}, Phone: {PHONE}")

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.start(phone=PHONE)
    
    session_string = client.session.save()
    print("\n[SUCCESS] Authentication successful!")
    print("\nYour Telethon Session String:")
    print("-" * 50)
    print(session_string)
    print("-" * 50)
    print("\nPlease copy the string above and save it into your GitHub Repository Secrets as TG_SESSION_STRING, along with TG_API_ID and TG_API_HASH.")

if __name__ == '__main__':
    asyncio.run(main())

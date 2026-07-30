import asyncio
import sys
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 38011181
API_HASH = 'f6147aae4bc47b08a58fb840ddf14502'
PHONE = '+251777770757'

async def main():
    code = sys.argv[1] if len(sys.argv) > 1 else None
    password = sys.argv[2] if len(sys.argv) > 2 else None

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        if not code:
            print("SENDING_CODE_REQUEST")
            await client.send_code_request(PHONE)
            print(f"SESSION_STRING:{client.session.save()}")
        else:
            print(f"Signing in with code...")
            try:
                await client.sign_in(PHONE, code)
            except Exception as e:
                if password:
                    print("Signing in with 2FA password...")
                    await client.sign_in(password=password)
                else:
                    raise e
            
            session_str = client.session.save()
            print("\n[SUCCESS] TELETHON_SESSION_STRING_START")
            print(session_str)
            print("TELETHON_SESSION_STRING_END")

if __name__ == '__main__':
    asyncio.run(main())

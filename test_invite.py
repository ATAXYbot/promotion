import asyncio
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import CheckChatInviteRequest

api_id = 20543583
api_hash = '505e57baf9b48347e18446d352cacce3'

async def main():
    client = TelegramClient('sessions/user_6541008362', api_id, api_hash)
    await client.connect()
    try:
        res = await client(CheckChatInviteRequest('1tP8_TIQLRMyMTdl'))
        print("Success:", res.stringify())
    except Exception as e:
        print("Error:", e)
    finally:
        await client.disconnect()

asyncio.run(main())

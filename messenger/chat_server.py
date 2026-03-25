import asyncio
import datetime
import logging
import sys
from asyncio import StreamReader, StreamWriter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))


class ChatServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.host = host
        self.port = port
        self.connected_users: dict = {}

    async def _send_data(self, writer: StreamWriter, data: bytes):
        """Send data to a client, including the length of the message."""
        length = (str(len(data)) + '\n').encode()
        writer.write(length + data)
        await writer.drain()

    async def _handle_send(self, username: bytes, message: bytes):
        """Handle the send message command."""
        message_to_send = (
            bytes(str(datetime.now()), 'utf-8') + b' '
            + username + b': ' + message
        )
        logger.info("User %s sent a message: %s",
                    username.decode(), message.decode())
        for name, user_data in self.connected_users.items():
            if name != username:
                await self._send_data(user_data['writer'], message_to_send)

    async def _handle_disconnect(self, username: bytes):
        """Handle the disconnect command."""
        if username in self.connected_users:
            logger.info('User %s disconnected', username.decode())
            del self.connected_users[username]

    async def _handle_connect(self, username: bytes, writer: StreamWriter):
        """Handle a new user connection."""
        logger.info('User %s connected', username.decode())
        self.connected_users[username] = {'writer': writer}

    async def _handle_client(self, reader: StreamReader, writer: StreamWriter):
        """Handle client requests."""
        username = (await reader.readline()).strip()
        logger.info("Client connected: %s", username.decode())
        await self._handle_connect(username, writer)

        while True:
            data = await reader.readline()
            if not data:
                break
            payload = data.strip().split(b'&')
            command = payload[0].split(b'=')[1]
            if command == b'send':
                message = payload[1].split(b'=')[1]
                await self._handle_send(username, message)
            elif command == b'exit':
                await self._handle_disconnect(username)
                break

        writer.close()
        await writer.wait_closed()

    async def start(self):
        """Start the server and listen for incoming connections."""
        await asyncio.start_server(
            self._handle_client
        )

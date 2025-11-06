import logging
import sys
import asyncio
import socket
from yaml import load, SafeLoader
from asyncio.streams import StreamReader, StreamWriter


class Base:
    config: dict = None
    _logger: logging.Logger

    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = load(f, Loader=SafeLoader)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(getattr(logging, self.config['logging']['level']))
        self._logger.addHandler(logging.StreamHandler(stream=sys.stdout))

    @property
    def logger(self) -> logging.Logger:
        return self._logger


# ProxyServer: создание TCP‑сервера, принятие клиентских соединений.
class ProxyServer(Base):
    def __init__(self):
        super().__init__()
        self._handler = ClientConnectionHandler()

    async def client_connected(self, reader: StreamReader, writer: StreamWriter):
        address = writer.get_extra_info('peername')
        self.logger.info('Start serving %s', address)

        try:
            await self._handler.handle(reader, writer)

        except Exception:
            writer.close()

    async def run(self, host: str, port: int):
        srv = await asyncio.start_server(
            self.client_connected,
            host, 
            port
        )

        async with srv:
            await srv.serve_forever()

# ClientConnectionHandler: чтение стартовой строки/заголовков запроса, выбор апстрима, проксирование данных (двунаправленный стриминг).
class ClientConnectionHandler(Base):
    def __init__(self):
        super().__init__()
        self._poller = UpstreamPool()

    async def handle(self, reader: StreamReader, writer: StreamWriter):
        line = await reader.readline()
        self.logger.info('Line %s', line)
        if not line:
            raise Exception("error connection")

        headers = []

        while True:
            header = await reader.readline()
            if header == b"\r\n" or header == b"":
                await asyncio.sleep(1)
                break
            
            headers.append(header)


        # v1
        data = await reader.read(1024)
        self.logger.info('1) Line %s, headers %s, body %s', line, headers, data)

        writer.write(b'HTTP/1.1 204 No Content')
        self.logger.info('2) Line %s, headers %s, body %s', line, headers, data)
        # v1


        # v2
        # self.logger.info('Line %s, headers %s', line, headers)
        # writer.write(b'HTTP/1.1 200 OK\r\n')
        # writer.write(b''.join(headers))

        # while (data := await reader.read(1024)) != b"":
        #     self.logger.info('Data %s', data)
        #     writer.write(data)
        # v2


        # v3
        # # do proxy logic

        # data = await reader.read(1024)

        # client_socket = self._poller.server_socket()

        # client_socket.sendall(line + b''.join(headers) + data)

        # while (resp := client_socket.recv(1024)) != "":
        #     self.logger.info(resp)
        #     writer.write(resp)

        # client_socket.close()
        # v3

        await writer.drain()
        writer.close()

class UpstreamPool(Base):
    def server_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("https://ya.ru", "443"))
        return s

if __name__ == '__main__':
    asyncio.run(ProxyServer().run('127.0.0.1', 8100))



# UpstreamPool: хранение списка апстримов, round‑robin выбор, лимиты на соединения, опционально — re‑use соединений.
# TimeoutPolicy: значения таймаутов и функции обёртки asyncio.wait_for.
# Logger/Metrics (минимальный): время запроса, байты, коды статусов, количество активных соединений.



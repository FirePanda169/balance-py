from __future__ import annotations
import typing as t
import logging
import sys
import asyncio
import traceback
import uuid
import time
import socket
import multiprocessing
# from concurrent.futures import ProcessPoolExecutor
import os
from dataclasses import dataclass, field
from json import dumps
from prometheus_client import Counter, Histogram, start_http_server
from prometheus_client.utils import INF
from parse import parse
from types import TracebackType
# from datetime import datetime, timezone
from yaml import load, SafeLoader
from asyncio.streams import StreamReader, StreamWriter



START_LINE_REQUEST_FORMAT = "{method} {path} {version}\r\n"
START_LINE_RESPONSE_FORMAT = "{version} {code} {message}\r\n"
HEADER_FORMAT = "{header}: {value}\r\n"

http_requests_total = Counter(
    'http_requests_total', 
    'Total number of requests by method, status and handler.',
    ['handler', 'method', 'status']
)

http_request_duration_seconds_bucket = Histogram(
    'http_request_duration_seconds', 
    'Latency with only few buckets by handler. Made to be only used if aggregation by handler is important.',
    ['handler', 'method'],
    buckets=(.001, .005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 7.5, 10.0, INF)
)

class Stream:
    stream_reader: StreamReader
    stream_writer: StreamWriter

    request_method: str
    request_path: str
    request_version: str

    request_headers: dict

    upstream_reader: StreamReader
    upstream_writer: StreamWriter

    response_version: str
    response_code: str
    response_message: str

    response_headers: dict

    def __init__(self, stream_reader: StreamReader, stream_writer: StreamWriter):
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.request_headers = {}
        self.response_headers = {}

        self.handled = False

    def parse_request_start_line(self, line: bytes) -> None:
        data = parse(START_LINE_REQUEST_FORMAT, line.decode())
        self.request_method = data['method']
        self.request_path = data['path']
        self.request_version = data['version']

    def parse_request_headers(self, request_headers: t.List[bytes]) -> None:
        for header in request_headers:
            request_header_data = parse(HEADER_FORMAT, header.decode())
            self.request_headers[request_header_data['header'].lower()] = request_header_data['value']

    def generate_request_id(self) -> None:
        if self.request_id is None:
            self.request_headers['x-request-id'] = str(uuid.uuid4())

    @property
    def request_content_len(self) -> int:
        return int(self.request_headers.get("content-length", "0"))

    @property
    def request_id(self) -> int:
        return self.request_headers.get("x-request-id", None)

    @property
    def is_connection_close(self) -> bool:
        # return True
        return self.request_headers.get("connection", "") == "close"

    @property
    def is_request_body(self) -> bool:
        return self.request_content_len > 0 or self.request_headers.get("transfer-encoding", "") == "chunked"

    @property
    def host(self) -> str:
        return self.request_headers["host"] if "host" in self.request_headers else ""

    def proxy_headers(self, headers) -> list:
        return [
            k.encode() + b": " + v.encode() + b"\r\n"
            for k, v in self.request_headers.items()
            if k in headers
        ]

    def parse_response_start_line(self, line: bytes) -> None:
        data = parse(START_LINE_RESPONSE_FORMAT, line.decode())
        self.response_version = data['version']
        self.response_code = data['code']
        self.response_message = data['message']

    def parse_response_headers(self, response_headers: t.List[bytes]) -> None:
        for header in response_headers:
            request_header_data = parse(HEADER_FORMAT, header.decode())
            self.response_headers[request_header_data['header'].lower()] = request_header_data['value']

    @property
    def response_content_len(self) -> int:
        return int(self.response_headers.get("content-length", "0"))

    @property
    def is_response_body(self) -> bool:
        return self.response_content_len > 0 or self.response_headers.get("transfer-encoding", "") == "chunked"

    @property
    def response_body_stream_type(self) -> str:
        if self.response_content_len > 0:
            return "content_len"
        if self.response_headers.get("transfer-encoding", "") == "chunked":
            return "chunked"
        return ""




class Config:
    data: dict = None

    def __init__(self, path: str):
        with open(path, 'r') as f:
            self.data = load(f, Loader=SafeLoader)

    def __getattr__(self, name):
        return self.data[name]

    @property
    def timeout_connect(self) -> float:
        return self.data['timeouts']['connect_ms'] / 1000

    @property
    def keep_alive(self) -> float:
        return self.data['timeouts']['keep-alive'] / 1000

    @property
    def timeout_read(self) -> float:
        return self.data['timeouts']['read_ms'] / 1000

    @property
    def timeout_write(self) -> float:
        return self.data['timeouts']['write_ms'] / 1000

    @property
    def timeout_total(self) -> float:
        return self.data['timeouts']['total_ms'] / 1000


class Base:
    config: Config = None
    _logger: logging.Logger = None

    def __init__(self):
        self.config = Config("./config.yaml")

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger(self.__class__.__name__)
            self._logger.setLevel(getattr(logging, self.config.logging[self.__class__.__name__] if self.__class__.__name__ in self.config.logging else self.config.logging['level']))

            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

            self._logger.addHandler(handler)

        return self._logger


@dataclass
class Upstream:
    host: str
    port: int
    ssl: bool
    connections: t.List[Connection]
    semaphore: asyncio.Semaphore

@dataclass
class Connection:
    upstream: Upstream
    reader: StreamReader
    writer: StreamWriter
    in_use: bool

    async def __aenter__(self) -> "Connection":
        if not self.in_use:
            raise ValueError("Can't use a closed or unacquired connection")
        return self

    async def __aexit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc: t.Optional[BaseException],
        tb: t.Optional[TracebackType],
    ) -> None:
        self.in_use = False
        self.upstream.semaphore.release()

class UpstreamPool(Base):
    upstreams: t.List[Upstream] = []
    pooling_task: asyncio.Task = None
    _con = None

    def __init__(self, loop = None):
        super().__init__()
        for upstream in self.config.upstreams:
            self.upstreams.append(Upstream(
                host=upstream["host"],
                port=upstream["port"],
                ssl=upstream["ssl"],
                connections=[],
                semaphore=asyncio.Semaphore(self.config.limits['max_conns_per_upstream']),
            ))

        self.upstreams_len = len(self.upstreams)
        self.upstreams_next = 0

    async def create_connection(self, upstream: Upstream) -> Connection:
        reader, writer = await asyncio.open_connection(
            upstream.host,
            upstream.port,
            ssl=upstream.ssl,
        )

        connection = Connection(
            upstream,
            reader,
            writer,
            True,
        )

        upstream.connections.append(connection)

        return connection

    def get_upstream(self) -> Upstream:
        self.upstreams_next = (self.upstreams_next + 1) % self.upstreams_len
        return self.upstreams[self.upstreams_next]

    async def get_connection(self) -> Connection:
        t00 = time.time()
        upstream = self.get_upstream()
        t01 = time.time()

        closed_connections = list((conn for conn in upstream.connections if conn.reader.at_eof() or conn.writer.is_closing()))

        t02 = time.time()
        for conn in closed_connections:
            upstream.connections.remove(conn)

        self.logger.debug(f"Upstream to {upstream.host} - connections {len(upstream.connections)} - processes {upstream.semaphore._value}")

        t03 = time.time()
        await upstream.semaphore.acquire()

        t04 = time.time()
        connection = next((conn for conn in upstream.connections if not conn.in_use and not conn.reader.at_eof() and not conn.writer.is_closing()), None)

        t05 = time.time()
        if connection is None:
            connection = await self.create_connection(upstream)
        else:
            upstream.connections.remove(connection)
            upstream.connections.append(connection)

        connection.in_use = True

        t07 = time.time()
        if self.config.contimings and t07-t00 > 0.05:
            self.logger.info(f'\n\
connect upstream:                   {t01-t00:.9f}\n\
closed_connections:                 {t02-t01:.9f}\n\
upstream.connections.remove(conn):  {t03-t02:.9f}\n\
upstream.semaphore.acquire():       {t04-t03:.9f}\n\
select connection:                  {t05-t04:.9f}\n\
create connection:                  {t07-t05:.9f}\n\
get connect total:                  {t07-t00:.9f}\n'
            )

        return connection

# ProxyServer: создание TCP‑сервера, принятие клиентских соединений.
class ProxyServer(Base):
    def __init__(self):
        super().__init__()

        self._queue = multiprocessing.Queue()
        self._poller = UpstreamPool()
        self._handler = ClientConnectionHandler(self._poller, self._queue)

    async def client_connection(self, reader: StreamReader, writer: StreamWriter):
        name = multiprocessing.current_process().name
        try:
            address = writer.get_extra_info('peername')
            self.logger.info('Start serving %s %s', name, address)
            # await self._handler.handle_connection(reader, writer)
            await self._handler.handle_connection_acceptor(reader, writer)
        except Exception as ex:
            if self.config.debug:
                self.logger.debug('Exception %s %s - %s trace %s', name, address, ex, traceback.format_exc())
            else:
                self.logger.debug('Exception %s %s - %s', name, address, ex)
        finally:
            writer.close()
            self.logger.info('End serving %s %s', name, address)

    def run_server(self, sock, client_connection):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        server = loop.run_until_complete(
            asyncio.start_server(
                client_connection,
                sock=sock,
                backlog=8192,
                reuse_port=True,
            )
        )
        self.logger.info(f"Процесс {multiprocessing.current_process().name} слушает на {sock.getsockname()}")
        
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

    def run_worker(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        self.logger.info(f"Процесс {multiprocessing.current_process().name} слушает очередь запросов")

        asyncio.run(self._handler.handle_request_worker())

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        # sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.bind((self.config.host, self.config.port))

        num_workers = self.config.workers - 1 if self.config.workers > 1 else 1

        processes = []

        # for i in range(num_workers):
        #     p = multiprocessing.Process(
        #         target=self.run_server,
        #         args=(sock, self.client_connection),
        #         name=f"Worker-{i+1}"
        #     )
        #     p.daemon = True
        #     p.start()
        #     processes.append(p)

        for i in range(num_workers):
            p = multiprocessing.Process(
                target=self.run_worker,
                name=f"Worker-{i+1}"
            )
            p.daemon = True
            p.start()
            processes.append(p)

        self.run_server(sock, self.client_connection)

        self.logger.info('Server started')

        start_http_server(9000)

        self.logger.info('Server metrics')

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            print("\nОстановка...")
            for p in processes:
                p.terminate()
            sock.close()

# ClientConnectionHandler: чтение стартовой строки/заголовков запроса, выбор апстрима, проксирование данных (двунаправленный стриминг).
class ClientConnectionHandler(Base):
    _poller: UpstreamPool = None

    def __init__(self, poller: UpstreamPool, queue: multiprocessing.Queue):
        super().__init__()
        self._poller = poller
        self._queue = queue

    async def handle_connection_acceptor(self, reader: StreamReader, writer: StreamWriter):
        name = multiprocessing.current_process().name
        while not reader.at_eof() and not writer.is_closing():
            client_fd = writer.get_extra_info('socket').fileno()
            self.logger.info(f"Worker {name} send client_fd {client_fd}")

            # self._queue.put(client_fd)
            self._queue.put({
                "client_fd": client_fd,
                "line": await reader.readline(),
            })

            await asyncio.sleep(0.005)
            writer.close()
            await writer.wait_closed()

    async def handle_request_worker(self):
        name = multiprocessing.current_process().name
        while True:
            try:
                # client_fd = self._queue.get()
                task = self._queue.get()
                self.logger.info(f"Worker {name} handle client_fd {task['client_fd']}")
                client_sock = socket.fromfd(task['client_fd'], socket.AF_INET, socket.SOCK_STREAM)
                reader, writer = await asyncio.open_connection(sock=client_sock)

                stream = Stream(reader, writer)
                await self.handle_request(stream, task['line'])

            except Exception as e:
                self.logger.error(f"Worker {name} error: {e}\n{traceback.format_exc()}")

    async def handle_connection(self, reader: StreamReader, writer: StreamWriter):
        while not reader.at_eof() and not writer.is_closing():
            stream = Stream(reader, writer)
            stream = await asyncio.wait_for(
                self.handle_request(stream),
                self.config.timeout_total,
            )

            if stream.is_connection_close:
                return

    async def handle_request(self, stream: Stream, line: str) -> Stream:
        self.logger.debug('Start handle_request')
        t00 = time.time()

        # await stream.stream_reader._wait_for_data("handle_request")

        t01 = time.time()
        self.logger.debug('Read request line')

        if line:
            request_line = line
        else:
            request_line = await asyncio.wait_for(
                stream.stream_reader.readline(),
                self.config.timeout_read + self.config.keep_alive,
            )

        if stream.stream_reader.at_eof() or stream.stream_writer.is_closing():
            return stream

        if not request_line:
            raise Exception(f"error connection {request_line}")

        t01_1 = time.time()
        stream.parse_request_start_line(request_line)

        t02 = time.time()
        self.logger.debug('Read request headers')
        request_headers = []
        while True:
            header = await asyncio.wait_for(
                stream.stream_reader.readline(),
                self.config.timeout_read,
            )
            if header == b"\r\n" or header == b"":
                # await asyncio.sleep(1)
                break

            request_headers.append(header)

        t03_1 = time.time()
        stream.parse_request_headers(request_headers)

        stream.generate_request_id()

        # self.logger.debug(f'\n1) Line {stream.request_version} {stream.request_method} {stream.request_path}\n2) headers {request_headers}\n3) headers proxy {stream.proxy_headers(self.config.proxy_headers + ["host", "content-length", "transfer-encoding"])}')

        t03_2 = time.time()
        connection = await asyncio.wait_for(
            self._poller.get_connection(),
            self.config.timeout_connect,
        )
        t04 = time.time()

        async with connection:
            stream.upstream_reader = connection.reader
            stream.upstream_writer = connection.writer

            self.logger.debug('Stream request line')
            await asyncio.wait_for(
                self.sync_to_async(
                    stream.upstream_writer.write,
                    request_line,
                ),
                self.config.timeout_write,
            )
            t05 = time.time()
            self.logger.debug('Stream request headers')
            await asyncio.wait_for(
                self.sync_to_async(
                    stream.upstream_writer.write,
                    b"".join(stream.proxy_headers(self.config.proxy_headers + ["host", "content-length", "transfer-encoding"])) + b"\n",
                ),
                self.config.timeout_write,
            )

            t06 = time.time()
            self.logger.debug('Stream request data')
            if stream.is_request_body:
                sum_read = 0
                while sum_read < stream.request_content_len:
                    request_data = await asyncio.wait_for(
                        stream.stream_reader.read(1024),
                        self.config.timeout_read,
                    )
                    await asyncio.wait_for(
                        self.sync_to_async(
                            stream.upstream_writer.write,
                            request_data,
                        ),
                        self.config.timeout_read,
                    )
                    sum_read += len(request_data)
                    self.logger.debug('Data %s %s', request_data, stream.stream_reader.at_eof())

            t06_1 = time.time()

            # await stream.upstream_reader._wait_for_data("handle_request")

            t07 = time.time()
            self.logger.debug('Read response line')
            response_line = await asyncio.wait_for(
                stream.upstream_reader.readline(),
                self.config.timeout_read,
            )
            t07_1 = time.time()
            stream.parse_response_start_line(response_line)

            t08 = time.time()
            self.logger.debug('Read response headers')
            response_headers = []
            while True:
                header = await asyncio.wait_for(
                    stream.upstream_reader.readline(),
                    self.config.timeout_read,
                )
                if header == b"\r\n" or header == b"":
                    break
                
                response_headers.append(header)

            t09_1 = time.time()
            stream.parse_response_headers(response_headers)
            
            # self.logger.debug(f'\n1) Line {stream.response_version} {stream.response_code} {stream.response_message}\n2) headers {dumps(stream.response_headers)}')

            t09_2 = time.time()
            self.logger.debug('Stream response line')
            await asyncio.wait_for(
                self.sync_to_async(
                    stream.stream_writer.write,
                    response_line,
                ),
                self.config.timeout_write,
            )
            t10 = time.time()
            self.logger.debug('Stream response headers')
            await asyncio.wait_for(
                self.sync_to_async(
                    stream.stream_writer.write,
                    b"".join(response_headers) + (b"\n" if stream.is_response_body else b"\n\n"),
                ),
            self.config.timeout_write,
            )

            t11 = time.time()
            self.logger.debug('Stream response data')
            if stream.is_response_body > 0:
                response_body_stream_type = stream.response_body_stream_type
                if response_body_stream_type == "content_len":
                    sum_read = 0
                    while sum_read < stream.response_content_len:
                        read_len = stream.response_content_len - sum_read if stream.response_content_len - sum_read < 1024 else 1024
                        response_data = await asyncio.wait_for(
                            stream.upstream_reader.read(read_len),
                            self.config.timeout_read,
                        )
                        await asyncio.wait_for(
                            self.sync_to_async(
                                stream.stream_writer.write,
                                response_data,
                            ),
                            self.config.timeout_write,
                        )
                        sum_read += len(response_data)
                        self.logger.debug('Data %s', response_data)
                elif response_body_stream_type == "chunked":
                    while True:
                        chank_len_x16 = await asyncio.wait_for(
                            stream.upstream_reader.readline(),
                            self.config.timeout_read,
                        )
                        self.logger.debug('Data chank len %s', chank_len_x16)
                        chank_len = int(chank_len_x16.decode(), 16)
                        self.logger.debug('Data len %s', chank_len)

                        if chank_len == 0:
                            data = await asyncio.wait_for(
                                stream.upstream_reader.readline(),
                                self.config.timeout_read,
                            )
                            self.logger.debug('Data %s', chank_len_x16 + data)
                            await asyncio.wait_for(
                                self.sync_to_async(
                                    stream.stream_writer.write,
                                    chank_len_x16 + data,
                                ),
                                self.config.timeout_write,
                            )
                            break

                        data = await asyncio.wait_for(
                            stream.upstream_reader.read(chank_len + 2),
                            self.config.timeout_read,
                        )
                        self.logger.debug('Data %s', chank_len_x16 + data)
                        await asyncio.wait_for(
                            self.sync_to_async(
                                stream.stream_writer.write,
                                chank_len_x16 + data,
                            ),
                            self.config.timeout_write,
                        )

        t12 = time.time()

        await stream.stream_writer.drain()

        t13 = time.time()

        if self.config.mainlog:
            self.logger.info(
                'request %s | %s | %s | %s:%d | %s | %.9f',
                stream.request_id,
                stream.request_method,
                stream.request_path,
                connection.upstream.host,
                connection.upstream.port,
                stream.response_code,
                t13-t01,
            )

        http_requests_total.labels(
            stream.request_path,
            stream.request_method,
            stream.response_code,
        ).inc()
        http_request_duration_seconds_bucket.labels(
            stream.request_path,
            stream.request_method,
        ).observe(t13-t01)
        

        if self.config.timings and t13-t00 > 0.01:
            self.logger.info(f'\n\
x-request-id:               {stream.request_id}\n\
wait request data:          {t01-t00:.9f}\n\
read request start_line:    {t01_1-t01:.9f}\n\
parse start_line:           {t02-t01_1:.9f}\n\
read request headers:       {t03_1-t02:.9f}\n\
parse request headers:      {t03_2-t03_1:.9f}\n\
connect upstream:           {t04-t03_2:.9f}\n\
stream request start_line:  {t05-t04:.9f}\n\
stream request headers:     {t06-t05:.9f}\n\
stream request data:        {t06_1-t06:.9f}\n\
wait response data:         {t07-t06_1:.9f}\n\
read response start_line:   {t07_1-t07:.9f}\n\
parse response start_line:  {t08-t07_1:.9f}\n\
read response headers:      {t09_1-t08:.9f}\n\
parse response headers:     {t09_2-t09_1:.9f}\n\
stream response start_line: {t10-t09_2:.9f}\n\
stream response headers:    {t11-t10:.9f}\n\
stream response data:       {t12-t11:.9f}\n\
end:                        {t13-t12:.9f}\n\
total:                      {t13-t00:.9f}\n'
            )

        return stream

    @staticmethod
    async def sync_to_async(func, *args, **kwargs):
        return func(*args, **kwargs)


if __name__ == '__main__':
    ProxyServer().run()

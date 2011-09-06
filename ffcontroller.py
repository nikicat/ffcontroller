#!/usr/bin/env python

import threading
import urllib.parse
import argparse
import logging
import logging.config
import socketserver
import socket
import queue
import http.server
import http.client
import json
import datetime
from collections import defaultdict, deque
import itertools
import errno
import pygeoip
import IPy

global options
global control_connections
global proxy_connections
global host_connections
global proxy_port_range
global_queue = queue.Queue()
control_connections = []
proxy_connections = []
geoip = pygeoip.GeoIP('/usr/share/GeoIP/GeoIPCity.dat')

# Monkey patching conditions in slave queue
global_queue.not_empty._notify = global_queue.not_empty.notify
global_queue.not_empty.notify = lambda: global_queue.not_empty._notify(len(global_queue.not_empty._waiters))
global_queue.not_full._notify = global_queue.not_full.notify
global_queue.not_full.notify = lambda: global_queue.not_full._notify(len(global_queue.not_full._waiters))

class MixedQueue(queue.Queue):
    def __init__(self, other):
        self.other = other
        self.maxsize = 0
        self._init(self.maxsize)
        self.mutex = other.mutex
        self.not_empty = other.not_empty
        self.not_full = other.not_full
        self.all_tasks_done = other.all_tasks_done
        self.unfinished_tasks = 0

    def _qsize(self):
        return len(self.queue) + self.other._qsize()

    def _get(self):
        if queue.Queue._qsize(self) > 0:
            return queue.Queue._get(self)
        elif self.other._qsize() > 0:
            return self.other._get()
        else:
            raise RuntimeError('MixedQueue: _get() from empty queues')

class ThreadingMixin(socketserver.ThreadingMixIn):
    def start(self):
        self.thread = threading.Thread(target=self.serve_forever)
        self.thread.start()

class ThreadedTCPServer(ThreadingMixin, socketserver.TCPServer):
    allow_reuse_address = True
    def __init__(self, family, sockaddr, handler):
        self.address_family = family
        socketserver.TCPServer.__init__(self, sockaddr, handler)

class ThreadedHTTPServer(ThreadingMixin, http.server.HTTPServer):
    allow_reuse_address = True
    def __init__(self, family, sockaddr, handler, pool=None):
        self.address_family = family
        self.pool = pool
        http.server.HTTPServer.__init__(self, sockaddr, handler)

def start_server(server_class, handler, address, port, *args):
    infos = socket.getaddrinfo(address, port, 0, socket.SOCK_STREAM)
    for family, socktype, proto, canonname, sockaddr in infos:
        logging.debug('creating {5} with {0} {1} {2} {3} {4}'.format(family, socktype, proto, canonname, sockaddr, server_class.__name__))
        server = server_class(family, sockaddr, handler, *args)
        server.start()
        # TODO: remove this temporary hack
        return server

class ControlConnectionHandler(socketserver.StreamRequestHandler):
    def setup(self):
        # some hack for mapped IPv4-to-IPv6 addresses
        ip = IPy.IP(self.client_address[0])
        if ip.iptype() == 'IPV4MAP':
            self.client_address = (ip._getIPv4Map().strNormal(), self.client_address[1])
        client_name = socket.getnameinfo(self.client_address, socket.NI_NUMERICSERV)
        self.logger = logging.getLogger('ffcontroller.control-connection')
        threading.current_thread().setName(client_name[0])
        self.start_time = datetime.datetime.now()
        self.last_command_time = self.start_time
        self.commands_sent = 0
        self.queue = MixedQueue(global_queue)
        # start dedicated server for us
        self.proxy_server = start_server(ThreadedHTTPServer, HTTPProxyRequestHandler, options.proxy_address, proxy_port_range.popleft(), ServerConnectionPool(self.queue))
        # register self for statistics
        control_connections.append(self)
        self.logger.debug('started')
        socketserver.StreamRequestHandler.setup(self)
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def handle(self):
        while True:
            try:
                netlocs = self.queue.get(timeout=10)
                command = 'TCPCONNECT {0} {1}'.format(*netlocs)
                self.logger.debug('command: {0}'.format(command))
                command += '\n'
                self.connection.sendall(command.encode())
                self.last_command_time = datetime.datetime.now()
                self.commands_sent += 1
            except queue.Empty as e:
                err = self.connection.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err != 0:
                    self.logger.warning('error: {0}'.format(errno.errorcode[err]))
                    break
            except:
                self.queue.put(netlocs)
                raise

    def finish(self):
        socketserver.StreamRequestHandler.finish(self)
        self.proxy_server.shutdown()
        proxy_port_range.append(self.proxy_server.server_port)
        self.logger.info('finished')
        control_connections.remove(self)

class ServerConnection(http.client.HTTPConnection):
    def __init__(self, netloc, queue):
        http.client.HTTPConnection.__init__(self, '', 0)
        self.netloc = netloc
        self.logger = logging.getLogger('ffcontroller.server-connection')
        threading.current_thread().setName(str(self))
        self.start_time = datetime.datetime.now()
        self.last_request_time = self.start_time
        self.requests_sent = 0
        self.last_request = ''
        self.queue = queue

    def __str__(self):
        return '{1}#{0:X}'.format(id(self), self.netloc)

    def connect(self):
        # create listening socket for back connection
        backserv = socket.socket()
        backserv.settimeout(options.backconn_timeout)
        # bind to some unused port
        backserv.bind((options.backconn_address, 0))
        selfaddr = backserv.getsockname()
        selfnetloc = '{0}:{1}'.format(*selfaddr)
        self.logger.debug('awaiting back connection on {0}'.format(selfnetloc))
        # start listening
        backserv.listen(1)
        for i in range(options.retry_count):
            # put both locations to queue
            self.queue.put((selfnetloc, self.netloc))
            # wait for connection
            try:
                self.sock,peeraddr = backserv.accept()
                self.sock.settimeout(options.transport_timeout)
                break
            except socket.timeout:
                self.logger.info('timeout while accepting back connection. resending request.')
        else:
            raise socket.timeout('timeout while trying to establish back connection')
        self.logger.debug('accepted back connection from {0}:{1}'.format(*socket.getnameinfo(peeraddr, socket.NI_NUMERICSERV)))
        # close server socket
        backserv.close()

    def check(self):
        if self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) != 0:
            self.close()
        return self.sock is not None

    def request(self, method, url, body=None, headers={}):
        self.logger.debug('request: {0} {1}'.format(method, url))
        http.client.HTTPConnection.request(self, method, url, body, headers)

class ServerConnectionPool(object):
    def __init__(self, queue):
        self.queue = queue
        self.host_connections = defaultdict(deque)
        self.logger = logging.getLogger('ffcontroller.server-connection-pool[{0:X}]'.format(id(self)))

    def create(self, peernetloc):
        backconn = ServerConnection(peernetloc, self.queue)
        self.logger.debug('created new host connection: {0}'.format(backconn))
        return backconn

    def acquire(self, peernetloc):
        while len(self.host_connections[peernetloc]) > 0:
            backconn = self.host_connections[peernetloc].popleft()
            if backconn.check():
                self.logger.debug('reusing host connection: {0}'.format(backconn))
                break
        else:
            backconn = self.create(peernetloc)
        return backconn

    def release(self, peernetloc, backconn):
        if backconn.sock is not None and backconn.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) == 0:
            # Do not forget to close response to reuse connection later
            self.logger.debug('storing connection for reusing later: {0}'.format(backconn))
            self.host_connections[peernetloc].append(backconn)

global_connection_pool = ServerConnectionPool(global_queue)

class HTTPProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    rbufsize = 0
    protocol_version = 'HTTP/1.1'

    def setup(self):
        self.logger = logging.getLogger('ffcontroller.http-proxy')
        threading.current_thread().setName('{0}&{1}'.format(self.address_string(), self.client_address[1]))
        self.start_time = datetime.datetime.now()
        self.last_send_time = self.start_time
        self.peernetloc = ''
        self.last_request = ''
        proxy_connections.append(self)
        http.server.BaseHTTPRequestHandler.setup(self)

    def finish(self):
        http.server.BaseHTTPRequestHandler.finish(self)
        proxy_connections.remove(self)
        self.logger.info('finished')

    def do_GET(self):
        self.logger.info('{0} {1} {2}'.format(self.command, self.path, self.request_version))
        self.last_request = '{0} {1} {2}'.format(self.command, self.path, self.request_version)
        for key, value in self.headers.items():
            self.logger.debug('request header: "{0}: {1}"'.format(key, value))
        r = urllib.parse.urlparse(self.path)
        peernetloc = r.netloc
        if r.port is None:
            port = {'http': 80}[r.scheme]
            peernetloc += ':' + str(port)
        for i in range(options.retry_count):
            try:
                backconn = self.server.pool.acquire(peernetloc)
                response = self.send_request(r, backconn)
                self.send_full_response(response)
                response.close()
                self.server.pool.release(peernetloc, backconn)
                break
            except Exception as e:
                self.logger.error('{0}: {1}'.format(type(e).__name__, e))
                self.logger.info('retrying request: {0} {1}'.format(self.command, self.path))
        else:
            self.send_error(504)

    def send_request(self, r, backconn):
        unparsedurl = urllib.parse.urlunparse(('', None, r.path, r.params, r.query, r.fragment))
        if unparsedurl == '':
            unparsedurl = '/'
        # Remove Proxy-Connection hack header and add Connection header
        headers = dict(self.headers)
        headers.pop('Proxy-Connection', None)
        headers['Connection'] = 'Keep-Alive'
        if self.command in ('POST', 'PUT'):
            body = self.rfile
        else:
            body = None
        self.logger.debug('request: {0} {1}'.format(self.command, unparsedurl))
        backconn.request(self.command, unparsedurl, body, headers)
        return backconn.getresponse()

    def send_full_response(self, response):
        self.logger.debug('response: {0} {1}'.format(response.status, response.reason))
        self.send_response_only(response.status, response.reason)
        for key, value in response.getheaders():
            self.logger.debug('response header: "{0}: {1}"'.format(key, value))
            self.send_header(key, value)
        self.end_headers()
        self.last_send_time = datetime.datetime.now()
        if response.chunked:
            # We will read response manually in intention to get chunk headers and send them to client
            # Read chunks
            while True:
                chunk_header = response.fp.readline()
                self.logger.debug('sending chunk header {0}'.format(chunk_header[:-2].decode()))
                self.wfile.write(chunk_header)
                length = int(chunk_header.split(b';')[0], 16)
                self.logger.debug('received chunk header. chunk length is {0} byes'.format(length))
                if length == 0:
                    self.logger.debug('received zero length chunk. stopping transmission')
                    break
                while length > 0:
                    data = response.fp.read(min(8192, length))
                    if len(data) == 0:
                        raise socket.error('connection closed')
                    self.last_send_time = datetime.datetime.now()
                    length -= len(data)
                    self.logger.debug('sending part of chunk with size {0} bytes'.format(len(data)))
                    self.wfile.write(data)
                crlf = response.fp.read(2)
                if crlf != b'\r\n':
                    self.logger.warning('not CRLF at the end of chunk, but {0}'.format(crlf))
                self.last_send_time = datetime.datetime.now()
                self.wfile.write(crlf)
            # Read the trailer
            while True:
                line = response.fp.readline()
                if not line:
                    break
                self.last_send_time = datetime.datetime.now()
                self.wfile.write(line)
                if line == b'\r\n':
                    break
        else:
            while response.length:
                data = response.read(8192)
                if len(data) == 0:
                    self.logger.info('server connection closed while sending body: {0} bytes left'.format(response.length))
                    break
                self.logger.debug('sending body: {0} bytes'.format(len(data)))
                self.wfile.write(data)
                self.last_send_time = datetime.datetime.now()

    do_POST = do_PUT = do_HEAD = do_GET

    def do_CONNECT(self):
        self.last_request = '{0} {1} {2}'.format(self.command, self.path, self.request_version)
        try:
            backconn = self.server.pool.create(self.path)
            backconn.connect()
            self.send_response(200)
        except:
            self.send_response(503)
            return
        finally:
            self.end_headers()
            self.last_send_time = datetime.datetime.now()

        def pump(source, dest):
            logger = logging.getLogger('{0}.{1}'.format(self.logger.name, threading.current_thread().name))
            try:
                while True:
                    data = source.read(8192)
                    logger.debug('pumping {0} bytes'.format(len(data)))
                    if len(data) == 0:
                        raise socket.error('connection closed')
                    dest.write(data)
                    self.last_send_time = datetime.datetime.now()
            except Exception as e:
                logger.debug('{0}. closing both sides.'.format(e))
                dest._sock.close()
                source._sock.close()

        client_addr = '{0}:{1}'.format(*socket.getnameinfo(self.request.getpeername(), socket.NI_NUMERICSERV))
        server_addr = '{0}:{1}'.format(*socket.getnameinfo(backconn.sock.getpeername(), socket.NI_NUMERICSERV))
        self.logger.debug('pumping between {0} and {1}'.format(client_addr, server_addr))
        pump_threads = [threading.Thread(target=pump, args=(backconn.sock.makefile('rb', 0), self.wfile), name='s2c'),
                        threading.Thread(target=pump, args=(self.rfile, backconn.sock.makefile('wb', 0)), name='c2s')]
        # pump data until the end
        list(map(threading.Thread.start, pump_threads))
        list(map(threading.Thread.join, pump_threads))

def get_location(address):
    record = geoip.record_by_addr(address)
    country_name = record.get('country_name', '')
    city = record.get('city', b'')
    return '{0}, {1}'.format(country_name, city)

class AdminRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        now = datetime.datetime.now()
        if self.path == '/':
            body = '''<html>
            <title>ffcontroller admin page</title>
            <style type="text/css" title="currentStyle">
                @import "http://www.datatables.net/release-datatables/media/css/demo_page.css";
                @import "http://www.datatables.net/release-datatables/media/css/demo_table.css";
                @import "http://www.datatables.net/release-datatables/media/css/demo_table_jui.css";
                @import "http://www.datatables.net/release-datatables/examples/examples_support/themes/smoothness/jquery-ui-1.8.4.custom.css";
            </style>
            <script type="text/javascript" language="javascript" src="http://yandex.st/jquery/1.6.2/jquery.js"></script>

            <script type="text/javascript" language="javascript" src="http://www.datatables.net/release-datatables/media/js/jquery.dataTables.js"></script>
            <script type="text/javascript" charset="utf-8">
                $(document).ready(function() {
                    $('#control-connections').dataTable( {
                        "bProcessing": true,
                        "sAjaxSource": '/control-connections',
                        "bJQueryUI": true,
                        "iDisplayLength": 30
                    } );
                    $('#proxy-connections').dataTable( {
                        "bProcessing": true,
                        "sAjaxSource": '/proxy-connections',
                        "bJQueryUI": true
                    } );
                    $('#server-connections').dataTable( {
                        "bProcessing": true,
                        "sAjaxSource": '/server-connections',
                        "bJQueryUI": true
                    } );

                } );
            </script>
            <body>
                <div class="demo_jui">
                <h1>Control connections</h1>
                <table class="display" id="control-connections">
                    <thead>
                        <th>Client address</th>
                        <th>Geolocation</th>
                        <th>Port</th>
                        <th>Established time</th>
                        <th>Commands sent</th>
                        <th>Idle time</th>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
                <h1>Proxy connection</h1>
                <table class="display" id="proxy-connections">
                    <thead>
                        <th>Client address</th>
                        <th>Last request</th>
                        <th>Established time</th>
                        <th>Idle time</th>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
                <h1>Server connections</h1>
                <table class="display" id="server-connections">
                    <thead>
                        <th>Server address</th>
                        <th>Requests sent</th>
                        <th>Last request</th>
                        <th>Established time</th>
                        <th>Idle time</th>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
                </div>
            </body>
            </html>
            '''
            content_type = 'text/html; charset=UTF-8'
        elif self.path.startswith('/control-connections'):
            data = {'aaData': [[
                '{0}:{1}'.format(*socket.getnameinfo(cc.client_address, socket.NI_NUMERICSERV)),
                get_location(cc.client_address[0]),
                cc.proxy_server.server_port,
                str(now - cc.start_time),
                cc.commands_sent,
                str(now - cc.last_command_time)
            ] for cc in control_connections]}
            body = json.dumps(data)
            content_type = 'application/json'
        elif self.path.startswith('/proxy-connections'):
            data = {'aaData': [['{0}:{1}'.format(*socket.getnameinfo(pc.client_address, socket.NI_NUMERICSERV)), pc.last_request, str(now - pc.start_time), str(now - pc.last_send_time)] for pc in proxy_connections]}
            body = json.dumps(data)
            content_type = 'application/json'
        elif self.path.startswith('/server-connections'):
            data = {'aaData': [[sc.netloc, sc.requests_sent, sc.last_request, str(now - sc.start_time), str(now - sc.last_request_time)] for sc in itertools.chain(*global_connection_pool.host_connections.values())]}
            body = json.dumps(data)
            content_type = 'application/json'
        else:
            self.send_error(404)
            self.end_headers()
            return
        body = body.encode()
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--control-address', dest='control_address', default='::')
    parser.add_argument('--control-port', dest='control_port', type=int, default=4000)
    parser.add_argument('--proxy-address', dest='proxy_address', default='::')
    parser.add_argument('--proxy-port', dest='proxy_port', type=int, default=4001)
    parser.add_argument('--proxy-port-range-start', dest='proxy_port_range_start', type=int, default=5000)
    parser.add_argument('--proxy-port-range-end', dest='proxy_port_range_end', type=int, default=9999)
    parser.add_argument('--backconn-address', dest='backconn_address', default=socket.getfqdn())
    parser.add_argument('--backconn-timeout', dest='backconn_timeout', type=float, default=3.0)
    parser.add_argument('--admin-address', dest='admin_address', default='::')
    parser.add_argument('--admin-port', dest='admin_port', type=int, default=4002)
    parser.add_argument('--retry-count', dest='retry_count', type=int, default=3)
    parser.add_argument('--transport-timeout', dest='transport_timeout', type=float, default=3.0)
    parser.add_argument('--logging-config', dest='logging_config', default='logging.conf')

    global options
    options = parser.parse_args()

    try:
        logging.config.dictConfig(json.load(open(options.logging_config)))
    except Exception as e:
        logging.error('failed to configure logging: {0}'.format(e))
        logging.exception(e)

    global proxy_port_range
    proxy_port_range = deque(range(options.proxy_port_range_start, options.proxy_port_range_end))

    start_server(ThreadedTCPServer, ControlConnectionHandler, options.control_address, options.control_port)
    start_server(ThreadedHTTPServer, HTTPProxyRequestHandler, options.proxy_address, options.proxy_port, global_connection_pool)
    start_server(ThreadedHTTPServer, AdminRequestHandler, options.admin_address, options.admin_port)

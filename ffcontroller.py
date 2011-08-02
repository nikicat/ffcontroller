#!/usr/bin/env python

import threading
import urllib.parse
import argparse
import logging
import socketserver
import socket
import queue
import http.server
import http.client

global logger
global options
logger = logging.basicConfig(level=logging.DEBUG)
netloc_queue = queue.Queue()

class ControlConnectionHandler(socketserver.StreamRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('control-connection-handler')
        socketserver.StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        client_name = socket.getnameinfo(self.client_address, 0)
        self.logger.debug('new connection from {0}:{1}'.format(*client_name))
        logger = logging.getLogger('{0}.{1}'.format(self.logger.name, client_name[0]))
        global netloc_queue
        while True:
            netlocs = netloc_queue.get()
            try:
                command = 'TCPCONNECT {0} {1}\n'.format(*netlocs)
                logger.debug('sending command {0}'.format(command))
                self.request.sendall(command.encode())
            except:
                netloc_queue.put(netlocs)

class SocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock):
        http.client.HTTPConnection.__init__(self, '', 0)
        self.sock = sock

class HTTPProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    rbufsize = 0

    def setup(self):
        self.logger = logging.getLogger('ffcontroller.http-proxy.{0}'.format(self.address_string()))
        self.logger.debug('new connection')
        http.server.BaseHTTPRequestHandler.setup(self)

    def do_GET(self):
        r = urllib.parse.urlparse(self.path)
        peernetloc = r.netloc
        if r.port is None:
            port = {'http': 80}[r.scheme]
            peernetloc += ':' + str(port)
        backsock = self.establish_backconn(peernetloc)
        backconn = SocketHTTPConnection(backsock)
        unparsedurl = urllib.parse.urlunparse(('', None, r.path, r.params, r.query, r.fragment))
        if unparsedurl == '':
            unparsedurl = '/'
        headers = dict(self.headers)
        headers.pop('Proxy-Connection', None)
        if self.command in ('POST', 'PUT'):
            body = self.rfile
        else:
            body = None
        backconn.request(self.command, unparsedurl, body, headers)
        response = backconn.getresponse()
        self.send_response_only(response.status, response.reason)
        for key, value in response.getheaders():
            self.send_header(key, value)
        self.end_headers()
        while response.length:
            data = response.read(8192)
            self.wfile.write(data)

    do_POST = do_PUT = do_HEAD = do_GET

    def do_CONNECT(self):
        try:
            backconn = self.establish_backconn(self.path)
            self.send_response(200)
        except:
            self.send_response(503)
        finally:
            self.end_headers()

        def pump(source, dest):
            logger = logging.getLogger('{0}.{1}'.format(self.logger.name, threading.current_thread().name))
            while True:
                data = source.read(8192)
                logger.debug('pumping {0} bytes'.format(len(data)))
                if len(data) == 0:
                    logger.debug('connection closed. closing opposite side.')
                    dest._sock.close()
                    break
                dest.write(data)

        client_addr = '{0}:{1}'.format(*socket.getnameinfo(self.request.getpeername(), 0))
        server_addr = '{0}:{1}'.format(*socket.getnameinfo(backconn.getpeername(), 0))
        self.logger.debug('pumping between {0} and {1}'.format(client_addr, server_addr))
        pump_threads = [threading.Thread(target=pump, args=(backconn.makefile('rb', 0), self.wfile), name='server-to-client'),
                        threading.Thread(target=pump, args=(self.rfile, backconn.makefile('wb', 0)), name='client-to-server')]
        # pump data until the end
        list(map(threading.Thread.start, pump_threads))
        list(map(threading.Thread.join, pump_threads))
        self.logger.debug('request handling completed')

    def establish_backconn(self, peernetloc):
        # create listening socket for back connection
        backserv = socket.socket()
        global options
        backserv.settimeout(options.backconn_timeout)
        # bind to some unused port
        backserv.bind((options.backconn_address, 0))
        selfaddr = backserv.getsockname()
        selfnetloc = '{0}:{1}'.format(*selfaddr)
        self.logger.debug('awaiting back connection on {0}'.format(selfnetloc))
        # start listening
        backserv.listen(1)
        while True:
            # put both locations to queue
            netloc_queue.put((selfnetloc, peernetloc))
            # wait for connection
            try:
                backconn,peeraddr = backserv.accept()
                break
            except socket.timeout:
                self.logger.debug('timeout while accepting back connection. resending request.')
        self.logger.debug('accepted back connection from {0}:{1}'.format(*socket.getnameinfo(peeraddr, 0)))
        # close server socket
        backserv.close()
        return backconn

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    def __init__(self, family, sockaddr, handler):
        self.address_family = family
        socketserver.TCPServer.__init__(self, sockaddr, handler)

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True
    def __init__(self, family, sockaddr, handler):
        self.address_family = family
        http.server.HTTPServer.__init__(self, sockaddr, handler)

def start_server(server_class, handler, address, port):
    infos = socket.getaddrinfo(address, port, 0, socket.SOCK_STREAM)
    for family, socktype, proto, canonname, sockaddr in infos:
        logging.debug('creating threaded tcp server with {0} {1} {2} {3} {4}'.format(family, socktype, proto, canonname, sockaddr))
        server = server_class(family, sockaddr, handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--control-address', dest='control_address', default='::')
    parser.add_argument('--control-port', dest='control_port', type=int, default=4000)
    parser.add_argument('--proxy-address', dest='proxy_address', default='::')
    parser.add_argument('--proxy-port', dest='proxy_port', type=int, default=4001)
    parser.add_argument('--backconn-address', dest='backconn_address', default=socket.getfqdn())
    parser.add_argument('--backconn-timeout', dest='backconn_timeout', type=float, default=3.0)

    global options
    options = parser.parse_args()

    start_server(ThreadedTCPServer, ControlConnectionHandler, options.control_address, options.control_port)
    start_server(ThreadedHTTPServer, HTTPProxyRequestHandler, options.proxy_address, options.proxy_port)

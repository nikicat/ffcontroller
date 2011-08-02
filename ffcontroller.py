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
import json
import datetime

global logger
global options
global control_connections
global proxy_connections
logger = logging.basicConfig(level=logging.DEBUG)
netloc_queue = queue.Queue()
control_connections = []
proxy_connections = []

class ControlConnectionHandler(socketserver.StreamRequestHandler):
    def setup(self):
        client_name = socket.getnameinfo(self.client_address, 0)
        self.logger = logging.getLogger('ffcontroller.control-connection.{0}'.format(*client_name))
        self.start_time = datetime.datetime.now()
        self.last_command_time = self.start_time
        self.commands_sent = 0
        global control_connections
        control_connections.append(self)
        self.logger.debug('started')
        socketserver.StreamRequestHandler.setup(self)

    def handle(self):
        global netloc_queue
        while True:
            netlocs = netloc_queue.get()
            try:
                command = 'TCPCONNECT {0} {1}'.format(*netlocs)
                self.logger.debug('command: {0}'.format(command))
                command += '\n'
                self.connection.sendall(command.encode())
                self.last_command_time = datetime.datetime.now()
                self.commands_sent += 1
            except:
                netloc_queue.put(netlocs)
                raise

    def finish(self):
        socketserver.StreamRequestHandler.finish(self)
        self.logger.info('finished')
        global control_connections
        control_connections.remove(self)

class SocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock):
        http.client.HTTPConnection.__init__(self, '', 0)
        self.sock = sock

class HTTPProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    rbufsize = 0

    def setup(self):
        self.logger = logging.getLogger('ffcontroller.http-proxy.{0}'.format(self.address_string()))
        self.logger.info('started')
        self.start_time = datetime.datetime.now()
        self.last_send_time = self.start_time
        global proxy_connections
        proxy_connections.append(self)
        http.server.BaseHTTPRequestHandler.setup(self)

    def finish(self):
        http.server.BaseHTTPRequestHandler.finish(self)
        global proxy_connections
        proxy_connections.remove(self)
        self.logger.info('finished')

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
        self.logger.debug('sending request: {0} {1}'.format(self.command, unparsedurl))
        backconn.request(self.command, unparsedurl, body, headers)
        response = backconn.getresponse()
        self.logger.debug('received response: {0} {1}'.format(response.status, response.reason))
        self.send_response_only(response.status, response.reason)
        for key, value in response.getheaders():
            self.logger.debug('sending header: "{0}: {1}"'.format(key, value))
            self.send_header(key, value)
        self.end_headers()
        self.last_send_time = datetime.datetime.now()
        while response.length or response.chunked:
            data = response.read(8192)
            if len(data) == 0:
                self.logger.info('server connection closed while sending body: {0} bytes left'.format(response.length))
                break
            self.logger.debug('sending body: {0} bytes'.format(len(data)))
            self.wfile.write(data)
            self.last_send_time = datetime.datetime.now()

    do_POST = do_PUT = do_HEAD = do_GET

    def do_CONNECT(self):
        try:
            backconn = self.establish_backconn(self.path)
            self.send_response(200)
        except:
            self.send_response(503)
        finally:
            self.end_headers()
            self.last_send_time = datetime.datetime.now()

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
                self.last_send_time = datetime.datetime.now()

        client_addr = '{0}:{1}'.format(*socket.getnameinfo(self.request.getpeername(), 0))
        server_addr = '{0}:{1}'.format(*socket.getnameinfo(backconn.getpeername(), 0))
        self.logger.debug('pumping between {0} and {1}'.format(client_addr, server_addr))
        pump_threads = [threading.Thread(target=pump, args=(backconn.makefile('rb', 0), self.wfile), name='server-to-client'),
                        threading.Thread(target=pump, args=(self.rfile, backconn.makefile('wb', 0)), name='client-to-server')]
        # pump data until the end
        list(map(threading.Thread.start, pump_threads))
        list(map(threading.Thread.join, pump_threads))

    def establish_backconn(self, peernetloc):
        self.peernetloc = peernetloc
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
            <script type="text/javascript" language="javascript" src="http://www.datatables.net/release-datatables/media/js/jquery.js"></script>

            <script type="text/javascript" language="javascript" src="http://www.datatables.net/release-datatables/media/js/jquery.dataTables.js"></script>
            <script type="text/javascript" charset="utf-8">
                $(document).ready(function() {
                    var oTable = $('#control-connections').dataTable( {
                        "bProcessing": true,
                        "sAjaxSource": '/control-connections',
                        "bJQueryUI": true
                    } );
                    var oTable = $('#proxy-connections').dataTable( {
                        "bProcessing": true,
                        "sAjaxSource": '/proxy-connections',
                        "bJQueryUI": true
                    } );

                } );
            </script>
            <body>
                <div class="demo_jui">
                <table class="display" id="control-connections">
                    <thead>
                        <th>Client address</th>
                        <th>Established time</th>
                        <th>Commands sent</th>
                        <th>Idle time</th>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
                <table class="display" id="proxy-connections">
                    <thead>
                        <th>Client address</th>
                        <th>Server address</th>
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
            data = {'aaData': [['{0}:{1}'.format(*socket.getnameinfo(cc.client_address, 0)), str(now - cc.start_time), cc.commands_sent, str(now - cc.last_command_time)] for cc in control_connections]}
            body = json.dumps(data)
            content_type = 'application/json'
        elif self.path.startswith('/proxy-connections'):
            data = {'aaData': [['{0}:{1}'.format(*socket.getnameinfo(pc.client_address, 0)), pc.peernetloc, str(now - pc.start_time), str(now - pc.last_send_time)] for pc in proxy_connections]}
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
    parser.add_argument('--admin-address', dest='admin_address', default='::')
    parser.add_argument('--admin-port', dest='admin_port', type=int, default=4002)

    global options
    options = parser.parse_args()

    start_server(ThreadedTCPServer, ControlConnectionHandler, options.control_address, options.control_port)
    start_server(ThreadedHTTPServer, HTTPProxyRequestHandler, options.proxy_address, options.proxy_port)
    start_server(ThreadedHTTPServer, AdminRequestHandler, options.admin_address, options.admin_port)

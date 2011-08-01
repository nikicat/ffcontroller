#!/usr/bin/env python

import threading
import urllib.parse
import argparse
import logging
import socketserver
import socket
import queue

global logger
global options
logger = logging.basicConfig(level=logging.DEBUG)
netloc_queue = queue.Queue()
#options = None

class ControlConnectionHandler(socketserver.StreamRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('control-connection-handler')
        socketserver.StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        self.logger.debug('new connection from {0}:{1}'.format(*self.client_address))
        global netloc_queue
        while True:
            netlocs = netloc_queue.get()
            command = 'TCPCONNECT {0} {1}\n'.format(*netlocs)
            self.logger.debug('sending command {0}'.format(command))
            self.wfile.write(command.encode())

class ProxyConnectionHandler(socketserver.StreamRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('proxy-connection')
        socketserver.StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        self.logger.debug('new connection from {0}:{1}'.format(*self.client_address))
        # not using self.rfile to avoid second line consuming by buffering
        request = self.request.makefile('rb', buffering=0).readline().decode()
        if len(request) == 0:
            self.logger.debug('connection closed')
            return
        self.logger.debug('received request: {0}'.format(request))
        method, url, protocol = request.split()
        r = urllib.parse.urlparse(url)
        # create listening socket for back connection
        backserv = socket.socket()
        # bind to some unused port
        backserv.bind((options.backconn_address, 0))
        selfaddr = backserv.getsockname()
        selfnetloc = '{0}:{1}'.format(*selfaddr)
        self.logger.debug('awaiting back connection on {0}'.format(selfnetloc))
        # start listening
        backserv.listen(1)
        # put both locations to queue
        peernetloc = r.netloc
        if r.port is None:
            port = {'http': 80, 'https': 443}[r.scheme]
            peernetloc += ':' + str(port)
        netloc_queue.put((selfnetloc, peernetloc))
        # wait for connection
        backconn,peeraddr = backserv.accept()
        self.logger.debug('accepted back connection from {0}:{1}'.format(*peeraddr))
        # close server socket
        backserv.close()
        # send first request string
        unparsedurl = urllib.parse.urlunparse((None, None, r.path, r.params, r.query, r.fragment)).decode()
        if unparsedurl == '':
            unparsedurl = '/'
        request = '{0} {1} {2}\n'.format(method, unparsedurl, protocol).encode()
        self.logger.debug('sending http request: {0}'.format(request))
        backconn.sendall(request)
        def pump(source, dest):
            source_addr = '{0}:{1}'.format(*source.getpeername())
            dest_addr = '{0}:{1}'.format(*dest.getpeername())
            self.logger.debug('starting to pump from {0} to {1}'.format(source_addr, dest_addr))
            while True:
                data = source.recv(8192)
                self.logger.debug('pumping {2} from {0} to {1}'.format(source_addr, dest_addr, data))
                if len(data) == 0:
                    self.logger.debug('connection closed')
                    break
                dest.sendall(data)
        pump_threads = [threading.Thread(target=pump, args=(self.request, backconn)),
                        threading.Thread(target=pump, args=(backconn, self.request))]
        # pump data until the end
        list(map(threading.Thread.start, pump_threads))
        list(map(threading.Thread.join, pump_threads))
        self.logger.debug('request handling completed')

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    def __init__(self, family, sockaddr, handler):
        self.address_family = family
        socketserver.TCPServer.__init__(self, sockaddr, handler)

def start_server(handler, address, port):
    infos = socket.getaddrinfo(address, port, 0, socket.SOCK_STREAM)
    for family, socktype, proto, canonname, sockaddr in infos:
        logging.debug('creating threaded tcp server with {0} {1} {2} {3} {4}'.format(family, socktype, proto, canonname, sockaddr))
        server = ThreadedTCPServer(family, sockaddr, handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--control-address', dest='control_address', default='::')
    parser.add_argument('--control-port', dest='control_port', type=int, default=4000)
    parser.add_argument('--proxy-address', dest='proxy_address', default='::')
    parser.add_argument('--proxy-port', dest='proxy_port', type=int, default=4001)
    parser.add_argument('--backconn-address', dest='backconn_address', default=socket.getfqdn())

    global options
    options = parser.parse_args()

    start_server(ControlConnectionHandler, options.control_address, options.control_port)
    start_server(ProxyConnectionHandler, options.proxy_address, options.proxy_port)

#!/usr/bin/env python2

import threading
import urlparse
import argparse
import logging
import SocketServer

logger = logging.basicConfig(level=logging.DEBUG)
netloc_queue = Queue()

class ControlConnection(object):
    def __init__(self, conn, selfnetloc):
        self.conn = conn
        self.selfnetloc = selfnetloc

    def establish_connection(self, netloc):
        self.conn.send('TCPCONNECT {0} {1}\n'.format(self.selfnetloc, netloc))

def establish_connection(netloc):
    control_connection = random.choice(control_connections)
    control_connection.establish_connection(netloc)

class ControlConnectionHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        global netloc_queue
        while True:
            netloc = netloc_queue.get()
            self.wfile.writeline('TCPCONNECT {0} {1}\n'.format(self.server.selfnetloc, netloc))

class ProxyConnectionHandler(SocketServer.StreamRequestHandler):
    def __init__(self):
        global logger
        self.logger = logger.getLogger('proxy-connection')

    def handle(self):
        self.logger.debug('new connection from {0}:{1}'.format(*self.client_address))
        request = self.rfile.readline()
        self.logger.debug('received request: {0}'.format(request))
        method, url, protocol = request.split()
        r = urlparse.urlparse(url)
        conn = establish_connection(r.netloc)
        unparsedurl = urlparse.urlunparse((None, None, r.path, r.params, r.query, r.fragment))
        conn.send('{0} {1} {2}'.format(method, unparsedurl))
        link_connections(self.request, conn)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

def start_server(handler, address, port):
    server = ThreadedTCPServer((address, port), handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.setDaemon(True)
    thread.start()

class ControlServer(Server):
    handler = ControlConnectionHandler

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--control-address', dest='control_address', default='0.0.0.0')
    parser.add_argument('--control-port', dest='control_port', type=int, default=4000)
    parser.add_argument('--proxy-address', dest='proxy_address', default='0.0.0.0')
    parser.add_argument('--proxy-port', dest='proxy_port', type=int, default=4001)
    parser.add_argument('--data-address', dest='data_address', default='0.0.0.0')
    parser.add_argument('--data-port', dest='data_port', type=int, default=4002)

    args = parser.parse_args()

    start_server(ControlConnectionHandler, args.control_address, args.control_port)
    start_server(BackConnectionHandler, args.data_address, args.data_port)
    start_server(ProxyConnectionHandler, args.proxy_address, args.proxy_port)

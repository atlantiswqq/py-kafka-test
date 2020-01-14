# _*_ coding:utf-8 _*_
# Author:Atlantis
# Date:2020-01-14

import json
import threading
from utils.resultlog import AsyncWrite
from urllib.parse import urlparse
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

aw = AsyncWrite()
aw.reconnect()


class KafkaTestServer(BaseHTTPRequestHandler):

    def do_GET(self):
        print(self.requestline)
        if not self.path.startswith('/test'):
            self.send_error(404, "Page not Found!")
            return
        data = {}
        query = urlparse(self.path).query
        for item in query.split("&"):
            k, v = item.split("=")
            data[k] = v
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_POST(self):
        req_datas = self.rfile.read(int(self.headers['content-length']))  # 重点在此步!
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        g = threading.Thread(target=aw.write, args=(req_datas.decode(),))
        g.start()
        self.wfile.write(req_datas)


if __name__ == '__main__':
    server_address = ('', 9002)
    server = HTTPServer(server_address, KafkaTestServer)
    server.serve_forever()

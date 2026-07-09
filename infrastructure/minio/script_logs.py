import http.server


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        with open('/var/log/audit/minio-audit.log', 'a') as f:
            f.write(body.decode() + '\n')
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass


if __name__ == '__main__':
    server = http.server.HTTPServer(('0.0.0.0', 8080), Handler)
    print("Audit webhook listening on :8080")
    server.serve_forever()

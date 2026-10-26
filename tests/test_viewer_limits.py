import contextlib
import http.client
import socket
import threading
import time

from test_client import TempCase
from mlboard.store import RunStore
from mlboard.viewer import ViewerServer
from mlboard.viewer_limits import ViewerLimits


class ViewerBoundaryTests(TempCase):
    def setUp(self):
        super().setUp()
        store = RunStore(self.root)
        store.close()

    @contextlib.contextmanager
    def serving(self, **changes):
        limits = ViewerLimits(**changes.pop("limits", {}))
        server = ViewerServer(self.root, ("127.0.0.1", 0), limits=limits, **changes)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            server.server_close()
            thread.join(1)

    def active(self, server):
        with server._connection_lock:
            return len(server._connections)

    def wait_for(self, predicate, seconds=1):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            if predicate():
                return
            time.sleep(.01)
        self.assertTrue(predicate())

    def test_first_incomplete_header_and_empty_socket_expire(self):
        with self.serving(limits={"header_seconds": .18, "idle_seconds": 2}) as server:
            for partial in (b"", b"GET /api/v1/projects HTTP/1.1\r\nHost: "):
                with socket.create_connection(server.server_address) as client:
                    if partial:
                        client.sendall(partial)
                    client.settimeout(1)
                    self.assertEqual(client.recv(1), b"")
                self.wait_for(lambda: self.active(server) == 0)

    def test_progress_cannot_extend_total_header_deadline(self):
        with self.serving(limits={"header_seconds": .25, "idle_seconds": .2}) as server:
            with socket.create_connection(server.server_address) as client:
                client.settimeout(.03)
                start = time.monotonic()
                closed = False
                while time.monotonic() - start < 1:
                    try:
                        client.sendall(b"X")
                        if client.recv(1) == b"":
                            closed = True
                            break
                    except TimeoutError:
                        pass
                    except (BrokenPipeError, ConnectionResetError):
                        closed = True
                        break
                self.assertTrue(closed)
                self.assertLess(time.monotonic() - start, .8)
            self.wait_for(lambda: self.active(server) == 0)

    def test_connection_gate_precedes_threads_and_recovers(self):
        with self.serving(limits={"connections": 2, "header_seconds": 2}) as server:
            clients = [socket.create_connection(server.server_address) for _ in range(2)]
            try:
                self.wait_for(lambda: self.active(server) == 2)
                with socket.create_connection(server.server_address) as extra:
                    extra.settimeout(.5)
                    self.assertEqual(extra.recv(1), b"")
                self.assertEqual(self.active(server), 2)
                clients[0].close()
                self.wait_for(lambda: self.active(server) == 1)
                connection = http.client.HTTPConnection(*server.server_address, timeout=1)
                try:
                    connection.request("GET", "/api/v1/projects", headers={"Authorization": "Bearer " + server.access_token})
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    response.read()
                finally:
                    connection.close()
            finally:
                for client in clients:
                    client.close()

    def test_keepalive_next_request_has_new_deadline(self):
        with self.serving(limits={"header_seconds": .2, "idle_seconds": 1}) as server:
            connection = http.client.HTTPConnection(*server.server_address, timeout=1)
            try:
                for _ in range(2):
                    connection.request("GET", "/api/v1/projects", headers={"Authorization": "Bearer " + server.access_token})
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    response.read()
                connection.sock.sendall(b"GET / HTTP/1.1\r\nHost: ")
                self.assertEqual(connection.sock.recv(1), b"")
            finally:
                connection.close()

    def test_header_byte_limit(self):
        with self.serving(limits={"header_bytes": 512}) as server:
            with socket.create_connection(server.server_address) as client:
                client.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: " + b"x" * 600 + b"\r\n\r\n")
                client.settimeout(1)
                self.assertIn(b"431", client.recv(4096).split(b"\r\n", 1)[0])

    def test_https_proxy_origin_and_secure_cookie(self):
        with self.serving(public_url="https://mlboard.example") as server:
            connection = http.client.HTTPConnection(*server.server_address, timeout=1)
            try:
                headers = {"Host": "mlboard.example", "Origin": "https://mlboard.example", "Authorization": "Bearer " + server.access_token, "Content-Type": "application/json"}
                connection.request("POST", "/api/local/session", body=b"{}", headers=headers)
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertIn("; Secure", response.getheader("Set-Cookie"))
                response.read()
                headers["Origin"] = "http://mlboard.example"
                connection.request("GET", "/api/v1/projects", headers=headers)
                response = connection.getresponse()
                self.assertEqual(response.status, 403)
                response.read()
            finally:
                connection.close()

    def test_slow_body_total_deadline_and_duplicate_length_close(self):
        with self.serving(limits={"body_seconds": .2, "idle_seconds": 1}) as server:
            host = f"127.0.0.1:{server.server_port}"
            head = (f"POST /api/local/session HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {server.access_token}\r\nContent-Length: 8\r\n").encode()
            with socket.create_connection(server.server_address) as client:
                client.sendall(head + b"\r\n{")
                client.settimeout(1)
                self.assertEqual(client.recv(1), b"")
            with socket.create_connection(server.server_address) as client:
                client.sendall(head + b"Content-Length: 8\r\n\r\n{}      ")
                client.settimeout(1)
                self.assertIn(b"400", client.recv(4096).split(b"\r\n", 1)[0])
            self.wait_for(lambda: self.active(server) == 0)

    def test_server_close_releases_incomplete_connections(self):
        with self.serving() as server:
            with socket.create_connection(server.server_address) as client:
                self.wait_for(lambda: self.active(server) == 1)
                server.server_close()
                client.settimeout(1)
                self.assertEqual(client.recv(1), b"")
                self.wait_for(lambda: self.active(server) == 0)

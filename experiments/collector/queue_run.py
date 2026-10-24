"""Bounded memory-queue experiment: OTLP acceptance is not durable delivery."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

BINARY_SHA = 'd4fa834fae73cb37e6058955a3833894d1ec815a1fc3529b0c2aaa99d8b22a2e'


def reconcile(submitted, accepted, attempted, committed):
    sets = [set(values) for values in (submitted, accepted, attempted, committed)]
    sent, acked, tried, done = sets
    if len(sent) != len(submitted) or not acked <= sent or not tried <= sent or not done <= tried:
        raise AssertionError('invalid event identity accounting')
    return {'submitted': sorted(sent), 'http_accepted': sorted(acked),
            'receiver_attempted': sorted(tried), 'backend_committed': sorted(done),
            'accepted_not_committed': sorted(acked - done),
            'not_http_accepted': sorted(sent - acked)}


def send(port, identity):
    request = ExportLogsServiceRequest()
    record = request.resource_logs.add().scope_logs.add().log_records.add()
    record.body.string_value = identity
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    try:
        connection.request('POST', '/v1/logs', request.SerializeToString(),
                           {'Content-Type': 'application/x-protobuf'})
        response = connection.getresponse()
        response.read()
        return response.status
    finally:
        connection.close()


def run_case(binary, directory, mode):
    directory.mkdir(parents=True, exist_ok=False)
    database = directory / 'observer.sqlite'
    with sqlite3.connect(database) as db:
        db.executescript('CREATE TABLE attempts(id TEXT); CREATE TABLE commits(id TEXT);')
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    state = {'discard': False, 'error': None}

    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                body = self.rfile.read(int(self.headers['Content-Length']))
                if self.headers.get('Content-Encoding') == 'gzip':
                    body = gzip.decompress(body)
                request = ExportLogsServiceRequest.FromString(body)
                ids = [r.body.string_value for a in request.resource_logs
                       for b in a.scope_logs for r in b.log_records]
                with sqlite3.connect(database) as db:
                    db.executemany('INSERT INTO attempts VALUES (?)', [(x,) for x in ids])
                entered.set()
                if not release.wait(30):
                    raise TimeoutError('receiver release barrier')
                if not state['discard']:
                    with sqlite3.connect(database) as db:
                        db.executemany('INSERT INTO commits VALUES (?)', [(x,) for x in ids])
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-protobuf')
                self.send_header('Content-Length', '0')
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as exc:
                state['error'] = repr(exc)
            finally:
                completed.set()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    config = {'receivers': {'otlp': {'protocols': {'http': {'endpoint': f'127.0.0.1:{port}'}}}},
              'exporters': {'otlp_http': {'endpoint': f'http://127.0.0.1:{server.server_port}',
                  'timeout': '25s', 'retry_on_failure': {'enabled': False},
                  'sending_queue': {'enabled': True, 'queue_size': 2, 'num_consumers': 1}}},
              'service': {'telemetry': {'metrics': {'level': 'none'}},
                          'pipelines': {'logs': {'receivers': ['otlp'], 'exporters': ['otlp_http']}}}}
    config_path = directory / 'collector.json'
    config_path.write_text(json.dumps(config, indent=2))
    env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}
    process = None
    statuses = {}
    log = (directory / 'collector.log').open('wb')

    def start():
        proc = subprocess.Popen([str(binary), '--config', str(config_path)],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError('collector exited before readiness; inspect collector.log')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    return proc
            except OSError:
                time.sleep(.02)
        proc.kill()
        proc.wait()
        raise TimeoutError('collector startup')

    try:
        process = start()
        statuses['inflight'] = send(port, 'inflight')
        assert statuses['inflight'] == 200
        assert entered.wait(5), 'no receiver attempt before queue fill'
        statuses['queued'] = send(port, 'queued')
        assert statuses['queued'] == 200
        if mode == 'saturation':
            statuses['overflow'] = send(port, 'overflow')
            assert statuses['overflow'] == 503, statuses
            release.set()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with sqlite3.connect(database) as db:
                    if db.execute('SELECT count(*) FROM commits').fetchone()[0] == 2:
                        break
                time.sleep(.02)
        else:
            # The first backend request is held before commit; the second is queued.
            process.kill()
            process.wait(timeout=5)
            state['discard'] = True
            release.set()
            assert completed.wait(5), 'old receiver handler did not finish'
            state['discard'] = False
            process = start()
            statuses['after_restart'] = send(port, 'after_restart')
            assert statuses['after_restart'] == 200
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with sqlite3.connect(database) as db:
                    if db.execute('SELECT count(*) FROM commits WHERE id=?', ('after_restart',)).fetchone()[0]:
                        break
                time.sleep(.02)
        with sqlite3.connect(database) as db:
            attempted = [r[0] for r in db.execute('SELECT id FROM attempts')]
            committed = [r[0] for r in db.execute('SELECT id FROM commits')]
        assert state['error'] is None, state
        result = reconcile(list(statuses), [k for k, v in statuses.items() if v == 200], attempted, committed)
        if mode == 'saturation':
            assert result['backend_committed'] == ['inflight', 'queued'], result
            assert result['accepted_not_committed'] == [], result
        else:
            assert result['backend_committed'] == ['after_restart'], result
            assert result['accepted_not_committed'] == ['inflight', 'queued'], result
        result.update(case=mode, statuses=statuses, guarantee='synthetic observation only')
        (directory / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
        return result
    finally:
        release.set()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collector', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    binary = args.collector.resolve()
    if hashlib.sha256(binary.read_bytes()).hexdigest() != BINARY_SHA:
        raise ValueError('unexpected collector binary')
    args.output.mkdir(parents=True, exist_ok=False)
    for mode in ('saturation', 'inflight_restart'):
        print(json.dumps(run_case(binary, args.output.resolve() / mode, mode)))

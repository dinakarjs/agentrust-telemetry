"""Real SDK emitters over loopback HTTP; Collector behavior is tested separately."""
import gzip
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.protobuf import json_format
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from agentrust_telemetry import OTelLogEmitter, SchemaValidator, TelemetryClient
from fidelity import check_fidelity, projection


class EmitterTests(unittest.TestCase):
    def test_python_and_typescript_emitters(self):
        root = Path(__file__).resolve().parents[2]
        event = json.loads((root / 'conformance/fixtures/valid/usage.json').read_text())
        event['time_unix_nano'] = str(int(event['time_unix_nano']) + 123)
        for language in ('python', 'typescript'):
            with self.subTest(language=language):
                observed, failures = [], []

                class Receiver(BaseHTTPRequestHandler):
                    def log_message(self, *args):
                        pass

                    def do_POST(self):
                        try:
                            if self.headers.get('Transfer-Encoding') == 'chunked':
                                chunks = []
                                while True:
                                    size = int(self.rfile.readline().split(b';')[0], 16)
                                    if size == 0:
                                        assert self.rfile.readline() == b'\r\n'
                                        break
                                    chunks.append(self.rfile.read(size))
                                    assert self.rfile.read(2) == b'\r\n'
                                body = b''.join(chunks)
                            else:
                                body = self.rfile.read(int(self.headers['Content-Length']))
                            if self.headers.get('Content-Encoding') == 'gzip':
                                body = gzip.decompress(body)
                            request = ExportLogsServiceRequest()
                            if 'json' in self.headers.get('Content-Type', ''):
                                json_format.Parse(body.decode(), request)
                            else:
                                request.ParseFromString(body)
                            observed.extend(projection(r) for a in request.resource_logs
                                            for b in a.scope_logs for r in b.log_records)
                        except Exception as exc:
                            failures.append(repr(exc))
                        self.send_response(200)
                        self.send_header('Content-Type', 'application/x-protobuf')
                        self.send_header('Content-Length', '0')
                        self.end_headers()

                server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                endpoint = f'http://127.0.0.1:{server.server_port}/v1/logs'
                evidence = []
                try:
                    if language == 'python':
                        class Evidence:
                            def append(self, item):
                                evidence.append(item)
                        provider = LoggerProvider()
                        provider.add_log_record_processor(SimpleLogRecordProcessor(OTLPLogExporter(endpoint=endpoint)))
                        try:
                            client = TelemetryClient(SchemaValidator.bundled(), evidence_sink=Evidence(),
                                                     log_emitter=OTelLogEmitter(provider.get_logger('test')))
                            self.assertTrue(client.emit(event).log_emitted)
                            provider.force_flush()
                        finally:
                            provider.shutdown()
                    else:
                        with tempfile.TemporaryDirectory() as directory:
                            folder = Path(directory)
                            (folder / 'input.json').write_text(json.dumps([event]))
                            env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}
                            subprocess.run(['node', str(Path(__file__).with_name('emit.mjs')), endpoint,
                                            str(folder / 'input.json'), str(folder / 'evidence.ndjson')],
                                           env=env, check=True, timeout=15, capture_output=True)
                            evidence = [json.loads(line) for line in (folder / 'evidence.ndjson').read_text().splitlines()]
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)
                self.assertEqual(failures, [])
                self.assertEqual(check_fidelity([event], evidence, observed)['unique_delivered'], 1)

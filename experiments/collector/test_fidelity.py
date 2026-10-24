import copy
import json
from pathlib import Path
import unittest

from fidelity import check_fidelity


class FidelityTests(unittest.TestCase):
    def setUp(self):
        event = json.loads((Path(__file__).resolve().parents[2] / 'conformance/fixtures/valid/usage.json').read_text())
        self.events = [{**event, 'event_id': str(i)} for i in range(3)]
        self.observed = [{'body': copy.deepcopy(e), 'timestamp_ns': e['time_unix_nano'],
                         'event_name': e['event_type'], 'attributes': {
                             'agentrust.telemetry.spec_version': e['spec_version'],
                             'agentrust.event.id': e['event_id'], 'agentrust.run.id': e['run_id'],
                             'agentrust.workflow.id': e['workflow_id'], 'gen_ai.agent.id': e['agent_id'],
                             'agentrust.usage.scope': e['scope']}} for e in self.events]

    def test_intact_and_reordered(self):
        self.assertFalse(check_fidelity(self.events, self.events, self.observed)['reordered'])
        self.assertTrue(check_fidelity(self.events, self.events, self.observed[::-1])['reordered'])

    def test_independent_corruptions(self):
        for field in ('body', 'timestamp_ns', 'event_name', 'attributes'):
            with self.subTest(field=field):
                damaged = copy.deepcopy(self.observed)
                if field == 'body':
                    damaged[0][field]['input_tokens'] += 1
                elif field == 'attributes':
                    damaged[0][field].pop('gen_ai.agent.id')
                else:
                    damaged[0][field] = 'wrong'
                with self.assertRaises(AssertionError):
                    check_fidelity(self.events, self.events, damaged)

    def test_missing_duplicate_unexpected_and_evidence(self):
        for rows in (self.observed[:-1], self.observed + self.observed[:1]):
            with self.subTest(rows=len(rows)), self.assertRaises(AssertionError):
                check_fidelity(self.events, self.events, rows)
        unexpected = copy.deepcopy(self.observed)
        unexpected[0]['body']['event_id'] = 'foreign'
        with self.assertRaises(AssertionError):
            check_fidelity(self.events, self.events, unexpected)
        with self.assertRaises(AssertionError):
            check_fidelity(self.events, self.events[::-1], self.observed)
        self.assertEqual(check_fidelity(self.events, self.events, self.observed + self.observed[:1],
                                       allow_retries=True)['duplicate_attempts'], 1)

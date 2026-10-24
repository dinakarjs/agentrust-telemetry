import unittest
from queue_run import reconcile


class AccountingTests(unittest.TestCase):
    def test_acceptance_is_not_commit(self):
        result = reconcile(['a', 'b', 'c'], ['a', 'b'], ['a'], [])
        self.assertEqual(result['accepted_not_committed'], ['a', 'b'])
        self.assertEqual(result['not_http_accepted'], ['c'])

    def test_valid_delivery_and_repeated_attempts(self):
        result = reconcile(['a'], ['a'], ['a', 'a'], ['a'])
        self.assertEqual(result['accepted_not_committed'], [])

    def test_foreign_and_unobserved_commit_rejected(self):
        for accepted, attempted, committed in [(['x'], [], []), ([], ['x'], []),
                                               (['a'], [], ['a']), ([], [], ['x'])]:
            with self.subTest(case=(accepted, attempted, committed)), self.assertRaises(AssertionError):
                reconcile(['a'], accepted, attempted, committed)

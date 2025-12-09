import unittest
import time
from contextlib import contextmanager
from unittest import skipIf

from source.rest_call import execute_request, get_data


class TestQueryDataReady(unittest.TestCase):
    conn = None
    db_name = None
    testing_ready = True

    @classmethod
    def load_tests(cls, loader, tests, pattern):
        """Force test order so test_system_query always runs first."""
        ordered = []
        for t in tests:
            if t._testMethodName == "test_system_query":
                ordered.insert(0, t)
            else:
                ordered.append(t)
        return loader.suiteClass(ordered)

    def setUp(self):
        assert self.conn
        assert self.db_name

    @contextmanager
    def query_context(self, query: str):
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    def test_system_query(self):
        command = "get databases where format=json"

        with self.query_context(query=command):
            try:
                response = execute_request(
                    func="GET",
                    conn=self.conn,
                    headers={"command": command, "User-Agent": "AnyLog/1.23"}
                )

                self.assertEqual(int(response.status_code), 200)

                content = response.json()

                if "system_query" not in content:
                    TestQueryDataReady.testing_ready = False

                self.assertIn("system_query", content)

            except Exception:
                TestQueryDataReady.testing_ready = False
                raise AssertionError("`system_query` logical database DNE")


    def test_row_count(self):
        # Runtime skip based on prior test result
        skipIf(not TestQueryDataReady.testing_ready, "System Query failed")

        query = f"sql {self.db_name} format=json and stat=false and include=(power_plant, power_plant_pv, t1) SELECT count(*) AS row_count FROM rand_data"
        expected_row_count = 3105

        row_count = 0
        for _ in range(3):
            result = get_data(conn=self.conn, query=query)
            row_count = int(result.json()['Query'][0]['row_count'])
            if row_count == expected_row_count:
                break
            time.sleep(30)

        if row_count != expected_row_count:
            TestQueryDataReady.testing_ready = False

        self.assertEqual(row_count, expected_row_count)

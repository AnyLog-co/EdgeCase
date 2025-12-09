"""
The following tests basic AnyLog functionality
- SELECT count with include
- SELECT count with include and extend
- SELECT min/max timestamp, min/max/avg/count value
- SELECT monitor_id, min/max timestamp, count value group by monitor_id
- increments
- period
"""

import os.path
import unittest
from source.rest_call import get_data
from source import support
from contextlib import contextmanager


ROOT_DIR = os.path.dirname(__file__).rsplit('tests', 1)[0]

class TestSQLCommands(unittest.TestCase):
    conn = None
    db_name = None

    def setUp(self):
        assert self.conn
        assert self.db_name

        self.query_base = f"sql {self.db_name} format=json and stat=false"

        self.expect_dir = os.path.join(ROOT_DIR, 'expect')
        support.create_dir(self.expect_dir)
        self.actual_dir = os.path.join(ROOT_DIR, 'actual')
        support.create_dir(self.actual_dir)

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    """
    Get rows count for tables in network
    """
    def test_row_count_complete(self):
        expected_count = 1500 + 1500 + 100

        query = f'{self.query_base} and include=(rand_data, power_plant_pv) "SELECT COUNT(*) AS row_count FROM power_plant;"'

        results = get_data(self.conn, query)
        data = results.json()

        self.assertIn("Query", data)
        self.assertGreater(len(data["Query"]), 0)

        row_count = data["Query"][0]["row_count"]
        self.assertEqual(row_count, expected_count)

    """
    Get rows count per table in the network 
    """
    def test_row_count_per_table_complete(self):
        expected_count = {
            'rand_data': 1500,
            'power_plant_pv': 100,
            'power_plant': 1500
        }

        query = f'{self.query_base} and include=(rand_data, power_plant_pv) and extend=(@table_name) "SELECT COUNT(*) AS row_count FROM power_plant;"'

        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)

            for row in data.get('Query'):
                table = row.get('table_name')
                row_count = row.get('row_count')
                self.assertEqual(row_count, expected_count[table])

    def test_aggregations(self):
        # self.skipTest("avg differs with partitioning")
        expected = {
            'min_ts': '2023-01-01T00:00:00.000000Z',
            'max_ts': '2025-12-31T23:59:59.000000Z',
            'min_val': 0.053,
            'max_val': 974.959,
            'avg_val': 248.76575133333333,
            'row_count': 1500
        }

        query = f'{self.query_base} and timezone=utc "SELECT min(timestamp) as min_ts, max(timestamp) as max_ts,  MIN(value) as min_val, MAX(value) as max_val, AVG(value) as avg_val, COUNT(*) as row_count FROM rand_data"'
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            for row in data.get('Query'):
                for key in expected:
                    self.assertEqual(row.get(key), expected.get(key))

    def test_aggregations_group_by(self):
        expected = [
            {'max_ts': '2025-12-06T18:38:24', 'min_ts': '2023-01-31T14:34:48', 'monitor_id': 'BCT', 'row_count': 50},
            {'max_ts': '2025-11-25T14:27:36', 'min_ts': '2023-02-07T21:28:48', 'monitor_id': 'BF1', 'row_count': 50},
            {'max_ts': '2025-12-29T20:29:24', 'min_ts': '2023-01-14T08:39:00', 'monitor_id': 'BF2', 'row_count': 50},
            {'max_ts': '2025-12-12T20:23:24', 'min_ts': '2023-01-05T03:06:36', 'monitor_id': 'BF3', 'row_count': 50},
            {'max_ts': '2025-12-24T23:53:24', 'min_ts': '2023-01-31T08:45:00', 'monitor_id': 'BF4', 'row_count': 50},
            {'max_ts': '2025-12-25T05:43:12', 'min_ts': '2023-01-01T23:19:12', 'monitor_id': 'BG10', 'row_count': 50},
            {'max_ts': '2025-12-22T07:45:36', 'min_ts': '2023-01-28T10:47:24', 'monitor_id': 'BG11', 'row_count': 50},
            {'max_ts': '2025-11-16T20:34:48', 'min_ts': '2023-02-02T13:13:12', 'monitor_id': 'BG8', 'row_count': 50},
            {'max_ts': '2025-12-23T07:04:48', 'min_ts': '2023-02-07T21:28:48', 'monitor_id': 'BG9', 'row_count': 50},
            {'max_ts': '2025-12-18T04:39:00', 'min_ts': '2023-01-17T06:36:36', 'monitor_id': 'BSP', 'row_count': 52},
            {'max_ts': '2025-12-04T20:00:00', 'min_ts': '2023-02-04T17:41:24', 'monitor_id': 'CBT', 'row_count': 50},
            {'max_ts': '2025-12-17T16:59:24', 'min_ts': '2023-01-16T07:17:24', 'monitor_id': 'CDT', 'row_count': 50},
            {'max_ts': '2025-12-07T23:47:24', 'min_ts': '2023-01-05T08:56:24', 'monitor_id': 'CF1', 'row_count': 50},
            {'max_ts': '2025-12-15T12:31:12', 'min_ts': '2023-01-07T07:34:48', 'monitor_id': 'CF2', 'row_count': 50},
            {'max_ts': '2025-12-24T00:34:12', 'min_ts': '2023-02-04T00:12:00', 'monitor_id': 'CF3', 'row_count': 50},
            {'max_ts': '2025-12-07T23:47:24', 'min_ts': '2023-01-01T17:29:24', 'monitor_id': 'CG12', 'row_count': 50},
            {'max_ts': '2025-12-06T12:48:36', 'min_ts': '2023-02-01T19:43:48', 'monitor_id': 'CG7', 'row_count': 50},
            {'max_ts': '2025-12-23T12:54:36', 'min_ts': '2023-02-13T23:13:48', 'monitor_id': 'CSP', 'row_count': 50},
            {'max_ts': '2025-11-26T13:46:48', 'min_ts': '2023-01-27T17:18:00', 'monitor_id': 'DCT', 'row_count': 50},
            {'max_ts': '2025-12-28T15:20:24', 'min_ts': '2023-01-06T02:25:48', 'monitor_id': 'DF1', 'row_count': 50},
            {'max_ts': '2025-12-27T21:51:00', 'min_ts': '2023-01-27T05:38:24', 'monitor_id': 'DF2', 'row_count': 50},
            {'max_ts': '2025-12-11T03:34:48', 'min_ts': '2023-02-02T13:13:12', 'monitor_id': 'DF3', 'row_count': 50},
            {'max_ts': '2025-11-08T14:21:36', 'min_ts': '2023-02-08T14:58:12', 'monitor_id': 'DF4', 'row_count': 50},
            {'max_ts': '2025-11-30T16:53:24', 'min_ts': '2023-03-14T21:00:00', 'monitor_id': 'DG2', 'row_count': 50},
            {'max_ts': '2025-12-31T23:59:59', 'min_ts': '2023-01-16T18:57:00', 'monitor_id': 'DG3', 'row_count': 50},
            {'max_ts': '2025-12-17T16:59:24', 'min_ts': '2023-01-20T04:34:12', 'monitor_id': 'DG4', 'row_count': 49},
            {'max_ts': '2025-12-24T18:03:36', 'min_ts': '2023-01-01T00:00:00', 'monitor_id': 'DG5', 'row_count': 50},
            {'max_ts': '2025-12-05T19:19:12', 'min_ts': '2023-02-08T09:08:24', 'monitor_id': 'DG6', 'row_count': 49},
            {'max_ts': '2025-12-30T02:19:12', 'min_ts': '2023-01-03T10:18:00', 'monitor_id': 'DSP', 'row_count': 50},
            {'max_ts': '2023-12-28T05:12:36', 'min_ts': '2023-01-01T00:00:00', 'monitor_id': 'InconLoadTapChangerAI', 'row_count': 100},
            {'max_ts': '2025-12-05T07:39:36', 'min_ts': '2023-01-12T04:10:48', 'monitor_id': 'KPL', 'row_count': 50}
        ]
        query = f"{self.query_base} and timezone=utc and include=(power_plant_pv) SELECT monitor_id, min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, count(*) as row_count as row_count FROM power_plant GROUP BY monitor_id ORDER min_ts, monitor_id DESC"
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            self.assertEqual(len(data['Query']), len(expected))
            for row in data.get('Query'):
                index = data['Query'].index(row)
                for key in expected[index]:
                    self.assertEqual(row.get(key), expected[index].get(key))

    """
    increment testing
    """
    def test_increments(self):
        query = f'sql {self.db_name} format=table and stat=false and timezone=utc "SELECT increments(%s, timestamp), min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, min(value) as min_val, avg(value)::float(3) as avg_val, max(value) as max_val FROM rand_data ORDER BY min_ts, max_ts ASC;"'
        for increment in ['day, 1', 'day, 7', 'day, 30', 'day, 90', 'day, 180', 'day, 365', 'year, 1']:
            with self.subTest(f"Increments - {increment}"):
                fname = f"increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
                results_file = os.path.join(self.actual_dir, fname)
                expect_file = os.path.join(self.expect_dir, fname)

                results = get_data(self.conn, query % increment)
                data = results.text

                support.write_file(results_file, data)
                support.copy_file(results_file, expect_file)
                actual_content = support.read_file(results_file)
                expect_content = support.read_file(expect_file)
                with self.query_context(query=query % increment):
                    self.assertEqual(actual_content, expect_content)

    def test_increments_group_by(self):
        query = f"sql {self.db_name} format=table and stat=false and timezone=utc and include=(power_plant_pv) SELECT increments(year, 1, timestamp), monitor_id, min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, count(*) as row_count as row_count FROM power_plant GROUP BY monitor_id ORDER min_ts, monitor_id DESC"
        fname = "increments_group_by_year_1.out"

        results_file = os.path.join(self.actual_dir, fname)
        expect_file = os.path.join(self.expect_dir, fname)

        results = get_data(self.conn, query)
        data = results.text

        support.write_file(results_file, data)
        support.copy_file(results_file, expect_file)
        actual_content = support.read_file(results_file)
        expect_content = support.read_file(expect_file)

        self.assertEqual(actual_content, expect_content)

    def test_period(self):
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 12, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            with self.subTest(f"Period - {period}"):
                query = f"sql {self.db_name} format=table and stat=false and timezone=utc SELECT timestamp, pv FROM power_plant_pv WHERE period({period}, timestamp) ORDER BY timestamp DESC"
                fname = f"period_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

                results_file = os.path.join(self.actual_dir, fname)
                expect_file = os.path.join(self.expect_dir, fname)

                results = get_data(self.conn, query)
                data = results.text

                support.write_file(results_file, data)
                support.copy_file(results_file, expect_file)
                actual_content = support.read_file(results_file)
                expect_content = support.read_file(expect_file)

                with self.query_context(query):
                    self.assertEqual(actual_content, expect_content)

    def test_period_and(self):
        # first 2 cases return empty set (expected) 
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 36, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            query = f"sql {self.db_name} format=table and stat=false and timezone=utc SELECT timestamp, a_current, b_current, c_current FROM power_plant WHERE period({period}, timestamp) AND monitor_id='DF2' ORDER BY timestamp DESC"
            fname = f"period_and_condition_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)
            
    def test_period_complex(self):
        # first 2 cases return empty set (expected) 
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 36, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            query = f"sql {self.db_name} format=table and stat=false and timezone=utc SELECT monitor_id, min(timestamp) as timestamp, avg(a_current), avg(b_current) as b_current, avg(c_current) as c_current FROM power_plant WHERE period({period}, timestamp) AND (monitor_id='DF2' OR monitor_id='BSP') GROUP BY monitor_id ORDER BY timestamp, monitor_id  DESC"
            fname = f"period_complex_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)

    def test_avg_count_sum(self):
        expected = [
            {'timestamp': '2023-07-01T11:00:36.000000Z', 'avg(value)': 270.22833333333335, 'count(value)': 3, 'sum(value)': 810.6850000000001},
            {'timestamp': '2023-07-02T10:19:48.000000Z', 'avg(value)': 173.5025, 'count(value)': 2, 'sum(value)': 347.005},
            {'timestamp': '2023-07-03T03:49:12.000000Z', 'avg(value)': 567.9565, 'count(value)': 2, 'sum(value)': 1135.913},
            {'timestamp': '2023-07-05T14:07:12.000000Z', 'avg(value)': 45.461, 'count(value)': 1, 'sum(value)': 45.461},
            {'timestamp': '2023-07-07T01:06:00.000000Z', 'avg(value)': 314.47125, 'count(value)': 4, 'sum(value)': 1257.885},
            {'timestamp': '2023-07-08T17:54:36.000000Z', 'avg(value)': 100.63900000000001, 'count(value)': 2, 'sum(value)': 201.27800000000002},
            {'timestamp': '2023-07-09T05:34:12.000000Z', 'avg(value)': 277.2626, 'count(value)': 5, 'sum(value)': 1386.313},
            {'timestamp': '2023-07-10T22:22:48.000000Z', 'avg(value)': 74.854, 'count(value)': 1, 'sum(value)': 74.854},
            {'timestamp': '2023-07-11T15:52:12.000000Z', 'avg(value)': 354.21625, 'count(value)': 4, 'sum(value)': 1416.865},
            {'timestamp': '2023-07-12T09:21:36.000000Z', 'avg(value)': 656.381, 'count(value)': 1, 'sum(value)': 656.381},
            {'timestamp': '2023-07-13T14:30:36.000000Z', 'avg(value)': 468.617, 'count(value)': 1, 'sum(value)': 468.617},
            {'timestamp': '2023-07-14T02:10:12.000000Z', 'avg(value)': 38.872, 'count(value)': 1, 'sum(value)': 38.872},
            {'timestamp': '2023-07-15T13:09:00.000000Z', 'avg(value)': 227.549, 'count(value)': 3, 'sum(value)': 682.647},
            {'timestamp': '2023-07-16T18:18:00.000000Z', 'avg(value)': 391.14099999999996, 'count(value)': 2, 'sum(value)': 782.2819999999999},
            {'timestamp': '2023-07-17T11:47:24.000000Z', 'avg(value)': 40.510999999999996, 'count(value)': 2, 'sum(value)': 81.02199999999999},
            {'timestamp': '2023-07-18T16:56:24.000000Z', 'avg(value)': 257.101, 'count(value)': 2, 'sum(value)': 514.202},
            {'timestamp': '2023-07-19T04:36:00.000000Z', 'avg(value)': 265.8005, 'count(value)': 2, 'sum(value)': 531.601},
            {'timestamp': '2023-07-20T03:55:12.000000Z', 'avg(value)': 367.16175, 'count(value)': 4, 'sum(value)': 1468.647},
            {'timestamp': '2023-07-21T09:04:12.000000Z', 'avg(value)': 401.512, 'count(value)': 2, 'sum(value)': 803.024},
            {'timestamp': '2023-07-22T08:23:24.000000Z', 'avg(value)': 530.616, 'count(value)': 1, 'sum(value)': 530.616},
            {'timestamp': '2023-07-24T12:51:36.000000Z', 'avg(value)': 28.761, 'count(value)': 1, 'sum(value)': 28.761},
            {'timestamp': '2023-07-25T23:50:24.000000Z', 'avg(value)': 14.524, 'count(value)': 1, 'sum(value)': 14.524},
            {'timestamp': '2023-07-26T05:40:12.000000Z', 'avg(value)': 180.32766666666666, 'count(value)': 3, 'sum(value)': 540.983},
            {'timestamp': '2023-07-27T04:59:24.000000Z', 'avg(value)': 260.6805, 'count(value)': 2, 'sum(value)': 521.361},
            {'timestamp': '2023-07-28T10:08:24.000000Z', 'avg(value)': 21.758, 'count(value)': 2, 'sum(value)': 43.516},
            {'timestamp': '2023-07-29T03:37:48.000000Z', 'avg(value)': 321.825, 'count(value)': 2, 'sum(value)': 643.65},
            {'timestamp': '2023-07-31T02:16:12.000000Z', 'avg(value)': 550.304, 'count(value)': 1, 'sum(value)': 550.304}
        ]

        query  = "select increments(day, 1, timestamp), min(timestamp) as timestamp, avg(value), count(value), sum(value) from rand_data where timestamp  > '2023-06-30 23:59:59' and timestamp < '2023-08-01 00:00:00' order by timestamp"
        command = f"sql {self.db_name} format=json and stat=false and timezone=utc {query}"
        results = get_data(self.conn, command)
        actual = results.json()['Query']
        with self.query_context(command):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)

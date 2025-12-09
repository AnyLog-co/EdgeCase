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
import datetime
import random

from source.rest_call import get_data
from source import support
from contextlib import contextmanager



ROOT_DIR = os.path.dirname(__file__).rsplit('tests', 1)[0]

class TestTimestampCommands(unittest.TestCase):
    conn = None
    db_name = None

    def setUp(self):
        assert self.conn
        assert self.db_name

        self.query_base = f"sql {self.db_name} format=json and stat=false "

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
    def test_basic_timestamp(self):
        expect = {'min(timestamp)': '2023-01-01T00:00:00.000000Z', 'max(timestamp)': '2025-12-31T23:59:59.000000Z',
                  'count(*)': 1500}
        query = f"{self.query_base} and timezone=utc SELECT min(timestamp), max(timestamp), count(*) FROM rand_data"

        with self.query_context(query):
            results = get_data(self.conn, query)
            actual = results.json().get('Query')
            self.assertEqual(len(actual), 1)
            self.assertEqual(expect, actual[0])


    def test_format_timezones(self):
       # This could fail in different timezones due to T/Z in timestamp(s)
        timezones = {
            "utc": {'min(timestamp)': '2023-01-01T00:00:00.000000Z', 'max(timestamp)': '2025-12-31T23:59:59.000000Z', 'count(*)': 1500},
            "pt":  {'min(timestamp)': '2022-12-31 16:00:00.000000',  'max(timestamp)': '2025-12-31 15:59:59.000000',  'count(*)': 1500},
            "et":  {'min(timestamp)': '2022-12-31 19:00:00.000000',  'max(timestamp)': '2025-12-31 18:59:59.000000',  'count(*)': 1500},
            "il":  {'min(timestamp)': '2023-01-01T00:00:00.000000Z', 'max(timestamp)': '2025-12-31T23:59:59.000000Z', 'count(*)': 1500},
        }

        for timezone in timezones:
            query = f"{self.query_base} and timezone={timezone} SELECT min(timestamp), max(timestamp), count(*) FROM rand_data"
            with self.query_context(query):
                results = get_data(self.conn, query)
                actual = results.json().get('Query')
                self.assertEqual(actual[0], timezones[timezone])

    def test_sql_timezone(self):
        timezones = {
            "utc".upper(): {'min(timestamp)': '2023-01-01 00:00:00', 'max(timestamp)': '2025-12-31 23:59:59', 'count(*)': 1500},
            "America/Los_Angeles": {'min(timestamp)': '2022-12-31 16:00:00', 'max(timestamp)': '2025-12-31 15:59:59', 'count(*)': 1500},
            "Europe/Paris": {'min(timestamp)': '2023-01-01 01:00:00', 'max(timestamp)': '2026-01-01 00:59:59', 'count(*)': 1500},
            "Asia/Dubai": {'min(timestamp)': '2023-01-01 04:00:00', 'max(timestamp)': '2026-01-01 03:59:59', 'count(*)': 1500},
            "America/Sao_Paulo": {'min(timestamp)': '2022-12-31 21:00:00', 'max(timestamp)': '2025-12-31 20:59:59', 'count(*)': 1500}
        }

        for timezone in timezones:
            query = f"{self.query_base} SELECT min(timestamp)::timezone('{timezone}'), max(timestamp)::timezone('{timezone}'), count(*) FROM rand_data"
            with self.query_context(query):
                results = get_data(self.conn, query)
                actual = results.json().get('Query')
                self.assertEqual(actual[0], timezones[timezone])

    def test_sql_format(self):
        timezone = "UTC"
        formats=[
            "%Y-%m-%dT%H:%M:%S.%fZ",  # 2025-12-08T23:13:35.123456Z
            "%Y-%m-%d %H:%M:%S.%f",   # 2025-12-08 23:13:35.123456
            "%Y-%m-%d %H:%M:%S",      # 2025-12-08 23:13:35
            "%Y-%m-%dT%H:%M:%SZ",     # 2025-12-08T23:13:35Z
            "%Y-%m-%d %H:%M",         # 2025-12-08 23:13
            "%d/%m/%Y %H:%M:%S",      # 08/12/2025 23:13:35
            "%d %b %Y %H:%M:%S",      # 08 Dec 2025 23:13:35
            "%b %d, %Y %H:%M:%S",     # Dec 08, 2025 23:13:35
            "%B %d %Y %H:%M"          # December 08 2025 23:13
        ]

        query = f"{self.query_base} SELECT min(timestamp)::datetime('%s') AS timestamp FROM rand_data ORDER BY timestamp LIMIT 1"

        for frmt in formats:
            with self.query_context(query % frmt):
                results = get_data(self.conn, query % frmt)
                timestamp_str = results.json()['Query'][0]['timestamp'] # adjust based on your `get_data` output
                # Try parsing using the same format
                try:
                    datetime.datetime.strptime(timestamp_str, frmt)
                except ValueError:
                    raise AssertionError(f"Timestamp '{timestamp_str}' does not match format '{frmt}'")

    def test_sql_tz_format(self):
        default_tz = "%Y-%m-%d %H:%M:%S",  # 2025-12-08 23:13:35
        previous_frmt = None

        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",  # 2025-12-08T23:13:35.123456Z
            "%Y-%m-%d %H:%M:%S.%f",  # 2025-12-08 23:13:35.123456
            "%Y-%m-%dT%H:%M:%SZ",  # 2025-12-08T23:13:35Z
            "%Y-%m-%d %H:%M",  # 2025-12-08 23:13
            "%d/%m/%Y %H:%M:%S",  # 08/12/2025 23:13:35
            "%d %b %Y %H:%M:%S",  # 08 Dec 2025 23:13:35
            "%b %d, %Y %H:%M:%S",  # Dec 08, 2025 23:13:35
            "%B %d %Y %H:%M"  # December 08 2025 23:13
        ]

        timezones = {
            "utc".upper(): '2023-01-01 00:00:00',
            "America/Los_Angeles": '2022-12-31 16:00:00',
            "Europe/Paris": '2023-01-01 01:00:00',
            "Asia/Dubai": '2023-01-01 04:00:00',
            "America/Sao_Paulo": '2022-12-31 21:00:00'
        }

        query = f"SELECT min(timestamp)::timezone('%s')::datetime('%s') AS timestamp FROM rand_data"

        for timezone in timezones:
            frmt = None

            while True:
                frmt = random.choice(formats)
                if frmt != previous_frmt:
                    break
            previous_frmt = frmt

            command = f"{self.query_base} {query % (timezone, frmt)}"

            with self.query_context(command):
                results = get_data(self.conn, command)
                timestamp_str = results.json()['Query'][0]['timestamp']
                try:
                    parsed = datetime.datetime.strptime(timestamp_str, frmt)
                except ValueError:
                    raise AssertionError(f"Timestamp '{timestamp_str}' does not match format '{frmt}'")

                expected_dt = datetime.datetime.strptime(timezones[timezone], "%Y-%m-%d %H:%M:%S")
                if parsed.replace(microsecond=0) != expected_dt:
                    raise AssertionError(
                        f"Timestamp '{timestamp_str}' does not match expected value '{expected_value}' for timezone '{tz}'"
                    )


if __name__ == "__main__":
    TestTimestampCommands.db_name = 'new_company'
    TestTimestampCommands.conn = '50.116.13.109:32149'
    unittest.main(verbosity=2)

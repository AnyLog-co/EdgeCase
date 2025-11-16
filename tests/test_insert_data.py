import json
import unittest
import source.rest_call as rest_call
from contextlib import contextmanager

DATA = [
    {"user": "Mike", "value1": 3, "value2": 3.14},
    {"user": "Bruce", "value1": 4, "value2": 6.14},
    {"user": "Kyle", "value1": 6},
    {"value1": 8, "value2": 18},
]

class TestInserts(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    operator = None
    db_name = None
    skip_insert = None
    def setUp(self):
        # Ensure required parameters are set
        self.query = '172.23.160.85:32149'
        self.operator = '172.23.160.85:32149'
        self.db_name = 'new_company'
        self.skip_insert = True
        # assert self.query
        # assert self.operator
        # assert self.db_name

        if not self.skip_insert:
            self.insert_data()

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise


    def insert_data(self):
        for row in DATA:
            rest_call.put_data(conn=self.operator, payload=json.dumps(row), dbms=self.db_name, table="t1")

    def test_row_count(self):
        query =  f"sql {self.db_name} format=json and stat=false select count(*) as row_count from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertIn("row_count", content[0])
            self.assertEqual(content[0].get("row_count"), 4)

    def test_raw_data(self):
        expected = [
            {'user': '', 'value1': 8, 'value2': 18.0},
            {'user': 'Bruce', 'value1': 4, 'value2': 6.14},
            {'user': 'Kyle', 'value1': 6, 'value2': ''},
            {'user': 'Mike', 'value1': 3, 'value2': 3.14}
        ]


        query =  f"sql {self.db_name} format=json and stat=false select  user, value1, value2 from t1 order by user"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            # print(content)
            self.assertEqual(content, expected)

    def test_avg_values(self):
        expected = {
            'value1': 5.25,
            'value2': 9.093333333333334
        }

        query = f"sql {self.db_name} format=json and stat=false select  avg(value1) as value1, avg(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content[0], expected)

    def test_values_count(self):
        expected = {"user": 3, 'value1': 4, 'value2': 3}
        query = f"sql {self.db_name} format=json and stat=false select  count(user) as user, count(value1) as value1, count(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content[0], expected)

    def test_name_where(self):
        self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where user IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where user IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

    def test_value_where(self):
        self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where value2 IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where value2 IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

if __name__ == '__main__':
    # Set class variables dynamically
    TestInserts.query = '172.23.160.85:32149'
    TestInserts.operator = '172.23.160.85:32149'
    TestInserts.db_name = 'new_company'

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)

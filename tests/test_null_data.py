import unittest
import source.rest_call as rest_call
from contextlib import contextmanager


class TestNullData(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    db_name = None

    @classmethod
    def setUpClass(self):
        # Ensure required parameters are set
        assert self.query
        assert self.db_name

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    def test_row_count(self):
        query =  f"sql {self.db_name} format=json and stat=false select count(*) as row_count from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertIn("row_count", content[0])
            self.assertEqual(content[0].get("row_count"), 5)

    def test_raw_data(self):
        expected = [
            {'acct': '',      'value1': 8, 'value2': 5},
            {'acct': 'Bruce', 'value1': 4, 'value2': 7},
            {'acct': 'Don',   'value1': 3, 'value2': 3},
            {'acct': 'Kyle',  'value1': 6, 'value2': ''},
            {'acct': 'Mike',  'value1': 3, 'value2': 3}
        ]

        query =  f"sql {self.db_name} format=json and stat=false select  acct, value1, value2 from t1 order by acct"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, expected)


    def test_avg_values(self):
        expected = {
            'value1': 4.8,
            'value2': 4.5
        }

        query = f"sql {self.db_name} format=json and stat=false select  avg(value1) as value1, avg(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            # print(content)
            self.assertEqual(content[0], expected)

    def test_values_count(self):
        expected = {'acct': 4, 'value1': 5, 'value2': 4}
        query = f"sql {self.db_name} format=json and stat=false select  count(acct) as acct, count(value1) as value1, count(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            # print(content)
            self.assertEqual(content[0], expected)

    def test_name_where(self):
        # self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and stat=false select acct, value1, value2 FROM t1 where acct IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, [{'acct': None, 'value1': 8, 'value2': 5}])

        query = f'sql {self.db_name} format=json and stat=false select acct, value1, value2 FROM t1 where acct IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, [
                {'acct': 'Mike', 'value1': 3, 'value2': 3},
                {'acct': 'Bruce', 'value1': 4, 'value2': 7},
                {'acct': 'Kyle', 'value1': 6, 'value2': None},
                {'acct': 'Don', 'value1': 3, 'value2': 3}
            ])

    def test_value_where(self):
        # self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and stat=false select acct, value1, value2 FROM t1 where value2 IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, [{'acct': 'Kyle', 'value1': 6, 'value2': None}])

        query = f'sql {self.db_name} format=json and stat=false select acct, value1, value2 FROM t1 where value2 IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, [
                {'acct': 'Mike', 'value1': 3, 'value2': 3},
                {'acct': 'Bruce', 'value1': 4, 'value2': 7},
                {'acct': None, 'value1': 8, 'value2': 5},
                {'acct': 'Don', 'value1': 3, 'value2': 3}]
            )

if __name__ == '__main__':
    # insert_data(conn='10.0.0.169:32149', db_name='new_company')
    # Set class variables dynamically
    TestNullData.query = '10.0.0.169:32149'
    TestNullData.operator = '10.0.0.169:32149'
    TestNullData.db_name = 'new_company'
    TestNullData.skip_insert = True

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)

import os.path
import unittest
import shutil
from rest_call import get_data
import support

ROOT_DIR = os.path.dirname(__file__)

class TestPowerPlantData(unittest.TestCase):
    def setUp(self, conn:str='172.23.160.85:32149', db_name='new_company'):
        self.conn = conn
        self.db_name = db_name
        self.query_base = f"sql {db_name} format=json and stat=false"

        self.expect_dir = os.path.join(ROOT_DIR, 'expect')
        support.create_dir(self.expect_dir)
        self.actual_dir = os.path.join(ROOT_DIR, 'actual')
        support.create_dir(self.actual_dir)

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

        self.assertIn("Query", data)
        for row in data.get('Query'):
            table = row.get('table_name')
            row_count = row.get('row_count')
            self.assertEqual(row_count, expected_count[table])


    """
    Basic increment test against rand_data
    """
    def test_small_increments(self):
        query = f"sql {self.db_name} format=table and stat=false SELECT increments(%s, timestamp), min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, min(value) as min_val, avg(value)::float(3) as avg_val, max(value) as max_val FROM rand_data WHERE timestamp >= '2024-12-20 00:00:00' AND timestamp <= '2025-01-10 23:59:59' ORDER BY min_ts DESC"
        for increment in ['second, 1', 'second, 30', 'minute, 1', 'minute, 5', 'minute, 15', 'minute, 30',
                          'hour, 1', 'hour, 6', 'hour, 12', 'hour, 24']:

            fname = f"small_increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query % increment)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            self.assertEqual(actual_content, expect_content)

    def test_increments(self):
        query = f'sql {self.db_name} format=table and stat=false "SELECT increments(%s, timestamp), min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, min(value) as min_val, avg(value)::float(3) as avg_val, max(value) as max_val FROM rand_data ORDER BY max_ts ASC;"'
        for increment in ['day, 1', 'day, 7', 'day, 30', 'day, 90', 'day, 180', 'day, 365', 'year, 1']:

            fname = f"increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query % increment)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            self.assertEqual(actual_content, expect_content)


if __name__ == "__main__":
    unittest.main(verbosity=2)

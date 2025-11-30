"""
The following tests basic AnyLog functionality
- `get status` validate node is returning "running"
- validate databases get created
- validate tables
- validate columns in tables
- validate configurations for operator
- validate configurations for query
"""

import os
import unittest

from source.rest_call import get_data
from contextlib import contextmanager
import random

ROOT_DIR = os.path.dirname(__file__).rsplit('tests', 1)[0]
DATA_TYPES_EQUIVALENTS = {
            'decimal': ['decimal', 'numeric', 'float', 'double precision', 'real'],
            'float': ['float', 'double precision', 'real', 'numeric', 'decimal'],
            'int': ['int', 'integer', 'bigint', 'smallint'],
            'char': ['char', 'character', 'character varying', 'varchar'],
            'timestamp without time zone': ['timestamp', 'timestamp without time zone'],
            'timestamp with time zone': ['timestamptz', 'timestamp with time zone'],
            'bool': ['boolean', 'bool'],
        }


class TestAnyLogCommands(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    operator = None
    db_name = None

    def setUp(self):
        # Ensure required parameters are set
        assert self.query
        assert self.operator
        assert self.db_name

        # if self.is_standalone is None:
        #     self.is_standalone = False

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    def test_get_status(self):
        command = "get status where format=json"
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        if self.operator:
            if self.assertIsInstance(self.operator, list):
                for conn in self.operator:
                    results = get_data(conn, command, destination="")
                    with self.query_context(command):
                        self.assertIn("Status", results.json())
                        data = results.json()
                        assert 'running' in data.get('Status') and 'not running' not in data.get('Status')
            elif isinstance(self.operator, str):
                results = get_data(self.operator, command, destination="")
                with self.query_context(command):
                    self.assertIn("Status", results.json())
                    data = results.json()
                    assert 'running' in data.get('Status') and 'not running' not in data.get('Status')
        if self.query:
            results = get_data(self.query, command, destination="")
            with self.query_context(command):
                self.assertIn("status", results.json())
                data = results.json()
                assert 'running' in data.get('status') and 'not running' not in data.get('status')

    """
    Validate operator related logical databases 
    """
    def test_operator_databases(self):
        if not self.operator:
            self.skipTest("Mising connection information for operator")
        command = "get databases where format=json"
        for conn in self.operator:
            result = get_data(conn, command, destination="")
            with self.query_context(command):
                self.assertIn(self.db_name, list(result.json().keys()))
                self.assertIn('almgm', list(result.json().keys()))

    """
    Validate system_query is running 
    """
    def test_system_query_database(self):
        if not self.query:
            self.skipTest("Mising connection information for query")
        command = "get databases where format=json"
        result = get_data(self.query, command, destination="")
        with self.query_context(command):
            self.assertIn("system_query", list(result.json().keys()))

    """
    Validate basic processes for operator node are enabled 
    """
    def test_operator_processes(self):
        if not self.operator:
            self.skipTest("Mising connection information for operator")
        command = "get processes where format=json"
        for conn in self.operator:
            result = get_data(conn, command, destination="")
            with self.query_context(command):
                data = result.json()
                for key in ['TCP', 'REST', 'Operator', 'Blockchain Sync', 'Scheduler', 'Blobs Archiver']:
                    self.assertIn(key, list(data.keys()))
                    self.assertIn('Status', data.get(key))
                    self.assertIn('Running', data.get(key).get('Status'))

    """
    Validate basic processes for query node are enabled 
    """
    def test_query_processes(self):
        if not self.query:
            self.skipTest("Mising connection information for query")
        command = "get processes where format=json"
        result = get_data(self.query, command, destination="")
        with self.query_context(command):
            data = result.json()
            for key in ['TCP', 'REST', 'Blockchain Sync', 'Scheduler']:
                self.assertIn(key, list(data.keys()))
                self.assertIn('Status', data.get(key))
                self.assertIn('Running', data.get(key).get('Status'))


    """
    For rand_data, power_plant, power_plant_pv validate column types 
    """
    def test_check_tables(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn =  random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        command = f"get data nodes where format=json and dbms={self.db_name}"
        result = get_data(conn, command, destination="")
        data = result.json()
        with self.query_context(command):
            self.assertGreater(len(data), 0)
            for row in data:
                self.assertIn('DBMS', row)
                self.assertIn('Table', row)
                self.assertEqual(row.get('DBMS'), self.db_name)
                assert row.get('Table') in ['rand_data', 'power_plant', 'power_plant_pv']

    # @unittest.skip(reason="data type inconsistent due to partitioning")
    def test_table_columns(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list): 
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        expected = {
            'rand_data': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'timestamp': 'timestamp without time zone', 'value': 'decimal'
            },
            'power_plant': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'monitor_id': 'char', 'timestamp': 'timestamp without time zone',
                'a_n_voltage': 'int', 'a_current': 'int', 'b_n_voltage': 'int', 'realpower': 'int', 'c_current': 'int',
                'c_n_voltage': 'int', 'commsstatus': 'char', 'energymultiplier': 'int', 'frequency': 'int',
                'powerfactor': 'int', 'b_current': 'int', 'reactivepower': 'int'
            },
            'power_plant_pv': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'monitor_id': 'character varying', 'timestamp': 'timestamp without time zone',
                'pv': 'float'
            }
        }

        # Map equivalent types across DBs/drivers
        for table in expected:
            with self.subTest(f"Testing: {self.db_name}.{table}"):
                command = f"get columns where dbms={self.db_name} and table={table} and format=json"
                result = get_data(conn, command, destination="")
                data = result.json()

                with self.query_context(command):
                    for column, data_type in data.items():
                        self.assertIn(column, expected[table])
                        if "char" not in data_type:
                            self.assertEqual(data_type, expected[table][column])
                        else:
                            self.assertIn(expected[table][column], data_type)

if __name__ == '__main__':
    # Set class variables dynamically
    TestAnyLogCommands.query = '172.23.160.85:32149'
    TestAnyLogCommands.operator = '172.23.160.85:32149'
    TestAnyLogCommands.db_name = 'new_company'
    TestAnyLogCommands.is_standalone = False

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)

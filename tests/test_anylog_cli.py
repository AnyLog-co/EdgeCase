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
    is_standalone = True  # Node is a standalone instance (master, operator and query in 1 container)

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
                'tsd_id': 'int', 'monitor_id': 'char(4)', 'timestamp': 'timestamp without time zone',
                'a_n_voltage': 'int', 'a_current': 'int', 'b_n_voltage': 'int', 'realpower': 'int', 'c_current': 'int',
                'c_n_voltage': 'int', 'commsstatus': 'char(4)', 'energymultiplier': 'int', 'frequency': 'int',
                'powerfactor': 'int', 'b_current': 'int', 'reactivepower': 'int'
            },
            'power_plant_pv': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'monitor_id': 'character varying', 'timestamp': 'timestamp without time zone',
                'pv': 'float'
            }
        }

        # Map equivalent types across DBs/drivers
        for table, columns in expected.items():
            command = f"get columns where dbms={self.db_name} and table={table} and format=json"
            result = get_data(conn, command, destination="")
            actual = result.json()
            with self.query_context(command):
                for col, expected_type in columns.items():
                    actual_type = actual.get(col)
                    if not actual_type:
                        self.fail(f"Column '{col}' missing in table '{table}'")

                    expected_type_lower = expected_type.lower()
                    actual_type_lower = actual_type.lower()

                    if expected_type_lower in DATA_TYPES_EQUIVALENTS:
                        self.assertIn(
                            actual_type_lower,
                            DATA_TYPES_EQUIVALENTS[expected_type_lower],
                            msg=f"Column '{col}' in table '{table}': expected {expected_type}, got {actual_type}"
                        )
                    else:
                        # fallback to exact match if type not mapped
                        if "char(" in actual_type_lower:
                            pass

                        self.assertEqual(
                            actual_type_lower,
                            expected_type_lower,
                            msg=f"Column '{col}' in table '{table}': expected {expected_type}, got {actual_type}"
                        )

    """
    Check the numbr of policies created for config, master and operator
    """
    def test_policy_count(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        command = "blockchain get (config, master, operator) bring.count"

        result = get_data(conn, command, destination="")
        with self.query_context(command):
            try:
                result = int(result.text)
            except Exception as error:
                self.fail(f"Failed to convert result to int type (Error: {error})")

            if self.is_standalone:
                self.assertEqual(result, 2)
            else:
                self.assertGreaterEqual(result, 4)

    """
    Logic to check that each table policy has an associated cluster and clusters with defined table(s) has an
    associated policy
    """
    def test_table_cluster_count(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        command = "blockchain get cluster"
        result = get_data(conn, command, destination="")
        clusters = result.json()
        command = "blockchain get table"
        result = get_data(conn, command, destination="")
        tables = result.json()

        with self.subTest("basic_count"):
            # the overall number of tables should be less than the number of clusters
            self.assertLess(len(tables), len(clusters))
            if self.is_standalone:
                self.assertEqual(len(tables), len(clusters)-1)

        with self.subTest("clusters_per_table"): # assert that each table has at least 1 associated cluster policy
            for table in tables:
                database = table['table']['dbms']
                name = table['table']['name']
                cluster_count = 0
                for cluster in clusters:
                    if cluster['cluster'].get('table'):
                        table_info = cluster['cluster'].get('table')
                        db_name = table_info[0].get('dbms')
                        tbl_name = table_info[0].get('name')
                        if db_name == database and tbl_name == name:
                            cluster_count += 1
                self.assertGreaterEqual(cluster_count, 1, f"No associated cluster for table {database}.{name}")

        with self.subTest("table_in_clusters"):
            # assert that each child cluster (ie cluster that's associated with table) has the associated table define
            table_count = {}
            for cluster in clusters:
                if cluster['cluster'].get('table'):
                    table_info =  cluster['cluster'].get('table')
                    database = table_info[0].get('dbms')
                    name = table_info[0].get('name')
                    if f"{database}.{name}" not in table_count:
                        table_count[f"{database}.{name}"] = 0
                        for table in tables:
                            db_name = table['table']['dbms']
                            tbl_name = table['table']['name']
                            if database == db_name and name == tbl_name:
                                table_count[f"{database}.{name}"] += 1
            assert all(table_count[k] == 1 for k in table_count)


    """
    Validate policy format for both nodes and configs
    """
    def test_policy_format(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        for policy_type in ["config", "master", "operator", "publisher", "query"]:
            with self.subTest(f"policy check - {policy_type}"):
                command = f"blockchain get {policy_type} bring.{random.choice(['first','last'])}"
                result = get_data(conn, command, destination="")
                policy = result.json()
                if policy:
                    self.assertNotEquals(policy[0].get(policy_type), None)
                    policy_info = policy[0].get(policy_type)

                    for key in ["name", "company", "ip", "port", "rest_port"]:
                        self.assertNotEquals(policy_info.get(key), None, f"Fails on key: `{key}`")

                    if policy_type == "config":
                        self.assertNotEquals(policy_info.get("script"), None, f"Fails on key: `script`")
                        assert isinstance(policy_info.get("script"), list)
                    elif policy_type == "operator":
                        self.assertNotEquals(policy_info.get("cluster"), None, f"Fails on key: `cluster`")
                        self.assertNotEquals(policy_info.get("main"), None, f"Fails on key: `main`")
                        assert  isinstance(policy_info.get("main"), bool)

    """
    Assert that operators are associated with a cluster that's root and not a child 
    """
    def test_operator_clusters(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        command = "blockchain get operator bring.unique [*][cluster] separator=,"
        result = get_data(conn, command, destination="")
        policies = result.text.split(",")
        for policy in policies:
            with self.subTest(f"Check cluster {policy}"):
                command = f"blockchain get cluster where id={policy}"
                result = get_data(conn=conn, query=command, destination="")
                result = result.json()[0]
                if result:
                    self.assertNotEquals(result.get("cluster"), None)
                    self.assertEqual(result["cluster"].get("parent"),None)


    """
    Assert that child clusters do have a parent 
    """
    def test_child_clusters(self):
        if not self.operator and not self.query:
            self.skipTest("Mising connection information for operator and query")
        elif not self.query and self.operator:
            if isinstance(self.operator, list):
                conn = random.chocie(self.operator)
            else:
                conn = self.operator
        else:
            conn = self.query

        command = "blockchain get cluster bring.unique [*][parent] separator=,"
        result = get_data(conn, command, destination="")
        policies = result.text.split(",")
        for policy in policies:
            with self.subTest(f"Check cluster {policy}"):
                command = f"blockchain get cluster where id={policy}"
                result = get_data(conn=conn, query=command, destination="")
                result = result.json()[0]
                if result:
                    self.assertNotEquals(result.get("cluster"), None)
                    self.assertEqual(result["cluster"].get("parent"),None)


if __name__ == '__main__':
    # Set class variables dynamically
    TestAnyLogCommands.query = '172.23.160.85:32149'
    TestAnyLogCommands.operator = '172.23.160.85:32149'
    TestAnyLogCommands.db_name = 'new_company'
    TestAnyLogCommands.is_standalone = False

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)

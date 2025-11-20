"""
The following provide blockchain (policy) testing related to validating relationship and formatting
"""
import unittest
from source.rest_call import get_data
import random
from contextlib import contextmanager


class TestBlockchainPolicies(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    is_standalone = True  # Node is a standalone instance (master, operator and query in 1 container)

    def setUp(self):
        # Ensure required parameters are set
        assert self.query
        assert self.is_standalone in [True, False]

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise


    """
    Check the numbr of policies created for config, master and operator
    """
    def test_policy_count(self):
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
                self.assertEqual(len(tables), len(clusters) - 1)

        with self.subTest("clusters_per_table"):  # assert that each table has at least 1 associated cluster policy
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
                    table_info = cluster['cluster'].get('table')
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
        conn = self.query
        for policy_type in ["config", "master", "operator", "publisher", "query"]:
            with self.subTest(f"policy check - {policy_type}"):
                command = f"blockchain get {policy_type} bring.{random.choice(['first', 'last'])}"
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
                        assert isinstance(policy_info.get("main"), bool)

    """
    Assert that operators are associated with a cluster that's root and not a child 
    """
    def test_operator_clusters(self):
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
                    self.assertEqual(result["cluster"].get("parent"), None)


    """
    Validate the parent cluster(s) for the child(ren) exist
    """
    def test_child_clusters(self):
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
                    self.assertEqual(result["cluster"].get("parent"), None)


# AnyLog Unit Testing 

The following provides a platform to easy add and execute testing against AnyLog / EdgeLake. 

The goal is whenever we encounter a bug, we add more data and test cases to be executed.

**Directory Structure**: 
```tree
/mnt/c/Users/oshad/Generic-Code/unit-testing$ tree -L 2
.
├── README.md
├── anylog_test_suit.py    # <-- main
├── data                   # <-- JSON data file
├── expect                 # <-- Output file with expected results
├── source                 # <-- support python scripts
└── tests                  # <-- unit tests 
```

The file [data/get_data.py](data/get_data.py) is used to generate the data files for AnyLog. 
It should not run again unless we lose the data; in which case we'd need to reset the files in expected.

Sample commands can be found as part of [Validating Tests](#create-and-validate-expected-results)

```shell
unit-testing$ python3 .\anylog_test_suit.py --help 
usage: anylog_test_suit.py [-h] [--query QUERY] [--operator OPERATOR] [--db-name DB_NAME] [--sort-timestamps [SORT_TIMESTAMPS]] [--batch [BATCH]]
                           [--skip-insert [SKIP_INSERT]] [--skip-test [SKIP_TEST]] [--verbose VERBOSE] [--select-test SELECT_TEST]

options:
  -h, --help            show this help message and exit
  --query QUERY         Query node IP:port
  --operator OPERATOR   Comma-separated operator node IPs
  --db-name DB_NAME     Logical database name
  --sort-timestamps [SORT_TIMESTAMPS]
                        Insert values in chronological order
  --batch [BATCH]       Insert a single data batch
  --skip-insert [SKIP_INSERT]
                        Skip data insertion
  --skip-test [SKIP_TEST]
                        Skip running unit tests
  --verbose VERBOSE     Test verbosity level (0, 1, 2)
  --select-test SELECT_TEST
                        (comma separated) specific test(s) to run

List of Tests
  - anylog:  test_check_tables, test_get_status, test_operator_databases, test_operator_processes, test_query_processes, test_system_query_database, test_table_columns  
  - sql:     test_aggregations, test_aggregations_group_by, test_increments, test_increments_group_by, test_period, test_period_and, test_period_complex, test_row_count_complete, test_row_count_per_table_complete, test_small_increments

```

The test does the following steps: 
1. Insert data 
2. [test_anylog_cli.py](tests/test_anylog_cli.py)   - execute basic `get` commands that validate connectivity, configurations and that the data exists 
3. [test_sql_queries.py](tests/test_sql_queries.py) - execute an array of common `SELECT` statements - expected results were do against a table without partitioning
   * aggregations
   * increments 
   * group by 
   * where 
   * period 
   * raw data

**Missing**: 
1. automatically deploying a small network  
   * 1 docker container that has everything 
   * Master, operator, query 
   * Master, 2 operator, query
   * Master, 2 operator (HA), query
   * Master, 3 operators (2 HA), query 
2. Insert data using POST and MQTT 
3. Remove data and associated blockchain policies from network 
4. teardown the entire network (used for overnight / testing) if everything passed 
5. store summary to file(s)


## Updating Code

### Adding New Data 
Add JSON file(s) to [data](data) directory - [insert_data.py](source/insert_data.py) will automatically grab the JSON file(s) 
and publish them to the operator node(s).

### New Test Cases

#### Option 1

To implement a new function in an exiting file simply add the test with the keyword "test_" in front of it. 

For example, lets say I want to add a test that gets a row count and the length of time data has been inserting.

```python
# file: test_sql_queries.py

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
    # ... older tests ... 
    def test_timediff_count(self):
        query = f"{self.query_base} SELECT min(timestamp) as min, max(timestamp) as max, min(timestamp)::timediff(max(timestamp)) as diff, count(*) as count FROM pp_pm"
        expected = {
           "min": "2025-01-12T08:15:22Z",
           "max": "2025-01-12T14:47:09Z",
           "timediff": "06:31:47",
           "count": 4821
        }
        
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            self.assertEqual(data['Query'][0], expected)
```

#### Option 2 

Implement a new test program all together

**Phase 1**: Creating a new file 
1. Copy an exiting test class into your repository
2. Remove all `test_` content
3. Update `setUp()` with needed params 
4. Create your own `test_` test cases 

**Phase 2**: Update [anylog_test_suit.py](anylog_test_suit.py)
1. Import the new class 
2. Update [`_print_test_cases`](anylog_test_suit.py#L17) method with new nickname and call to generate list of tests
3. Above `main`, add a new function that calls the new unittest 
4. Update `main` to call the new unittests under `if not args.skip_test`. 

#### Create and Validate Expected Results

Steps to create expected results and validate
1. Start an EdgeLake instance that has all three parts and is partitioned by `insert_timestamp` (assuming you query by `timestamp` )
```shell
docker run -it --detach-keys=ctrl-d \
  -e INIT_TYPE=prod \
  -e NODE_TYPE=master-operator \
--network host --name anylog --rm anylogco/edgelake:latest 
```
2. Run [anylog_test_suit.py](anylog_test_suit.py) until it passes with the right results 
```shell
# first time - insert data + run the full test 
python3 anylog_test_suit.py \
  --query [specify query]  \
  --operator [sepcify operator] \
  --db-name [db name]

# Every consequent time, you can just run the failed/specific test(s)
python3 anylog_test_suit.py \
  --skip-insert \
  --query [specify query]  \
  --operator [sepcify operator] \
  --db-name [db name] \
  --select-test anylog,my_tests.test3
```

3. Rerun testing against the updated code


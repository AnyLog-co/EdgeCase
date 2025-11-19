""""
:requirements:
    - 1 query
    - 2 or more operators on the same cluster
    - user needs to specify to run this test because the `--operator` value should be TCP and not REST credentials
:logic:
    - run each query against specific destination(s)
    - validate data is the same for all operators in cluster
        -> SELECT min(ts), max(ts), count(*)
        -> SELECT min(ts), max(ts), min(val), max(val), avg(val)
        -> SELECT id, min(ts), max(ts), min(val), max(val), avg(val) group by id
        -> SELECT increments
        -> WHERE period
:complex:
    - if an (main) operator is killed data still returned
"""
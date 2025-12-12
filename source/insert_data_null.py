import json
import random
import source.rest_call as rest_call

DATA = [
    # full data
    {"timestamp": "2025-11-16 12:20:43.058968", "acct": "Mike",  "value1": 3,  "value2":3},
    {"timestamp": "2025-11-16 12:22:43.058968", "acct": "Bruce", "value1": 4,  "value2": 7},
    # missing value2
    {"timestamp": "2025-11-16 12:24:43.058968", "acct": "Kyle",  "value1": 6},
    # missing acct
    {"timestamp": "2025-11-16 12:26:43.058968",                  "value1": 8,  "value2": 5},
    # missing timestamp
    {                                           "acct": "Don",  "value1": 3,  "value2":3}, # fails to insert when timestamp is NULL
]


def insert_data(conns:list, db_name:str):
    conn = random.choice(conns)
    for row in DATA:
        rest_call.put_data(conn=conn, payload=json.dumps(row), dbms=db_name, table="t1")
        if DATA.index(row) == 1:
            rest_call.flush_buffer(conn=conn)
        if len(conns) > 1:
            prev_conn = conn
            while conn == prev_conn:
                conn = random.choice(conns)

    rest_call.flush_buffer(conn=conn)

if __name__ == '__main__':
    insert_data(conns=['50.116.13.109:32149'], db_name='new_company')
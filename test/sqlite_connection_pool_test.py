# coding=utf-8
import os
import random
import sqlite3
import threading
import time

from pava.utils.async_utils import async_execute

from pava.dependency.cuttlepool import CuttlePool
from pava.utils.object_utils import main_thread_hang

CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS test_table (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        name   TEXT
    )
"""





pool = SQLitePool(factory=sqlite3.connect, database='/Users/bytedance/Documents/test.sqlite', capacity=32)
pool.get_resource().cursor().execute(CREATE_SQL)
write_count = 1


def _write_handle():
    # while True:
    con = pool.get_resource()
    con.cursor().execute("BEGIN TRANSACTION;")
    con.cursor().execute("UPDATE test_table SET name = '1'")
    con.cursor().execute("INSERT INTO test_table (name) VALUES ('%s')" % random.random())
    # con.commit()
    global write_count
    write_count += 1
    print(str(threading.current_thread()) + str(int(time.time())) + "Insert_" + str(write_count))


def _read_handle():
    while True:
        con = pool.get_resource()
        res = con.cursor().execute("SELECT * FROM test_table LIMIT 1").fetchone()
        # res = con.cursor().execute("SELECT COUNT(*) FROM test_table LIMIT 1").fetchone()
        if res is None or res[0] is None:
            return
        print(str(int(time.time())) + "Read" + str(res[1]))


if __name__ == '__main__':
    # threading.Thread(target=_write_handle).start()
    # _write_handle()
    # os._exit(1)
    threading.Thread(target=_read_handle).start()

    main_thread_hang()

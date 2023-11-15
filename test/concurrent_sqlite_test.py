# coding=utf-8
import sqlite3
import random
import threading
import time

from pava.decorator.decorator_impl.synchronized_decorator import synchronized

db_path = "/Users/monetto/Documents/test.sqlite"

connection = sqlite3.connect(db_path, check_same_thread=False)
connection2 = sqlite3.connect(db_path, check_same_thread=False)
connection3 = sqlite3.connect(db_path, check_same_thread=False)


write_count = 0


def write_handle():
    while True:
        _write_handle()


def read_handle():
    while True:
        _read_handle()


# @synchronized("11")
def _write_handle():
    connection2.cursor().execute("INSERT INTO test_table (name) VALUES ('%s')" % random.random())
    connection2.commit()
    global write_count
    write_count += 1
    print(str(int(time.time())) + "Insert_" + str(write_count))


# @synchronized("11")
def _read_handle():
    res = connection3.cursor().execute("SELECT count(1) FROM test_table LIMIT 1").fetchone()
    if res is None or res[0] is None:
        return
    print(str(int(time.time())) + "Read" + str(res[0]))


if __name__ == '__main__':
    c = connection.cursor()
    c.execute(CREATE_SQL)
    connection.commit()
    time_sleep(1)
    threading.Thread(target=write_handle).start()
    threading.Thread(target=read_handle).start()

    time_sleep(1000)

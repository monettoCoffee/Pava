# coding=utf-8
import sqlite3

from pava.dependency.cuttlepool import CuttlePool


class SQLiteConnectionPool(CuttlePool):
    def __init__(self, db_path, capacity=4):
        """
        复制自 CuttlePool 官方 Demo
        :param db_path:
        :param capacity:
        """
        CuttlePool.__init__(
            self,
            factory=sqlite3.connect,
            database=db_path,
            capacity=capacity
        )

    def normalize_resource(self, resource):
        resource.row_factory = None

    def ping(self, resource):
        try:
            result = resource.execute('SELECT 1').fetchall()
            return (1,) in result
        except sqlite3.Error:
            return False

    def get_connection(self):
        return CuttlePool.get_resource(
            self,
            resource_wrapper=None
        )

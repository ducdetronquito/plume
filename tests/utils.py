# Built-in dependencies
import os

# Internal dependencies
from plume import SQLiteDB


class BaseTest:
    def setup(self):
        self.db = SQLiteDB('test.db')

    def teardown(self):
        os.remove('test.db')


def collection_is_registered(db, collection_name):
    query = (
        "SELECT name FROM sqlite_master WHERE "
        "type='table' AND name='{}';"
    ).format(collection_name)

    result = db._connection.execute(query).fetchone()
    return result


def table_info(db, table_name):
    query = 'PRAGMA table_info({})'.format(table_name)
    return db._connection.execute(query).fetchall()

# Built-in dependencies
import os

# Internal dependencies
from plume import Collection, Database
from utils import collection_is_registered


class TestDatabase:

    def test_create_database(self):
        assert not os.path.exists('test.db')
        db = Database('test.db')
        assert os.path.exists('test.db')

    def test_create_plume_master_if_not_exists(self):
        db = Database('test.db')
        assert collection_is_registered(db, 'plume_master')

    def test_new_database_does_not_contain_collections(self):
        db = Database('test.db')
        assert len(db._collections) == 0
        rows = db._connection.execute(
            'SELECT count(*) FROM plume_master'
        ).fetchone()
        assert rows[0] == 0

    def test_returns_collection_if_not_exists(self):
        db = Database('test.db')
        collection = db.users
        assert isinstance(collection, Collection)
        assert collection._name == 'users'
        assert collection._registered is False

    def teardown(self):
        os.remove('test.db')

# Built-in dependencies
import json
import os

# Internal dependencies
from plume import SQLiteDB
from utils import collection_is_registered


class TestCollectionCreation:

    def setup(self):
        self.db = SQLiteDB('test.db')

    def test_create_and_register_collection(self):
        assert not collection_is_registered(self.db, 'users')
        collection = self.db.users
        collection._register()
        assert collection_is_registered(self.db, 'users')
        assert collection._name == 'users'
        assert collection._registered is True
        assert 'users' in self.db._collections

    def teardown(self):
        os.remove('test.db')


class TestCollectionInsertOne:

    def setup(self):
        self.db = SQLiteDB('test.db')

    def test_insert_document_add_new_row(self):
        self.db.actors.insert_one({
            'name': 'Bakery Cumbersome'
        })
        row = self.db._connection.execute(
            'SELECT count(_data) FROM actors'
        ).fetchone()
        assert row[0] == 1

    def test_retrieve_document_values(self):
        self.db.actors.insert_one({
            'name': 'Bakery Cumbersome'
        })
        
        document = self.db._connection.execute(
            'SELECT _data FROM actors'
        ).fetchone()[0]
        actor = json.loads(document)
        assert actor['name'] == 'Bakery Cumbersome'

    def teardown(self):
        os.remove('test.db')

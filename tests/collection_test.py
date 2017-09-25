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


class TestCollectionFind:

    def setup(self):
        self.db = SQLiteDB('test.db')
        self.db.users.insert_one({'name': 'Boby', 'age': 10})
        self.db.users.insert_one({'name': 'John', 'age': 20})
        self.db.users.insert_one({'name': 'Poopy', 'age': 30})

    def test_equal_selector(self):
        result = self.db.users.find({'age': {'$eq': 20}})
        assert len(result) == 1
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'John'

    def test_not_equal_selector(self):
        result = self.db.users.find({'age': {'$ne': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Boby'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Poopy'

    def test_greater_than_selector(self):
        result = self.db.users.find({'age': {'$gt': 10}})
        assert len(result) == 2
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'John'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Poopy'

    def test_greater_than_equals_selector(self):
        result = self.db.users.find({'age': {'$gte': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'John'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Poopy'

    def test_lower_than_selector(self):
        result = self.db.users.find({'age': {'$lt': 30}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Boby'
        assert result[1]['age'] == 20
        assert result[1]['name'] == 'John'

    def test_lower_than_equal_selector(self):
        result = self.db.users.find({'age': {'$lte': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Boby'
        assert result[1]['age'] == 20
        assert result[1]['name'] == 'John'

    def teardown(self):
        os.remove('test.db')

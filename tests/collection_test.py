# Built-in dependencies
import json

# Internal dependencies
from utils import BaseTest, collection_is_registered, table_info


class TestCollectionCreation(BaseTest):

    def test_create_and_register_collection(self):
        assert not collection_is_registered(self.db, 'users')
        collection = self.db.users
        collection._register()
        assert collection_is_registered(self.db, 'users')
        assert collection._name == 'users'
        assert collection._registered is True
        assert 'users' in self.db._collections


class TestCollectionInsertOne(BaseTest):

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

    def test_insert_one_with_single_field_index(self):
        self.db.actors.create_index({
            'name': str,
        })
        self.db.actors.insert_one({
            'name': 'Bakery Cumbersome'
        })
        document = self.db._connection.execute(
            'SELECT _data, name FROM actors'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == {
            'name': 'Bakery Cumbersome'
        }
        assert document[1] == 'Bakery Cumbersome'

    def test_insert_one_with_single_field_index_with_missing_field(self):
        self.db.actors.create_index({
            'name': str,
        })
        self.db.actors.insert_one({
            'age': 42
        })
        document = self.db._connection.execute(
            'SELECT _data, name FROM actors'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == {
            'age': 42
        }
        assert document[1] is None


class TestCollectionInsertMany(BaseTest):

    def test_insert_document_add_new_rows(self):
        self.db.actors.insert_many([
            {'name': 'Bakery Cumbersome'},
            {'name': 'Beezlebub Cabbagepatch'},
            {'name': 'Bombadil Cottagecheese'},
        ])
        row = self.db._connection.execute(
            'SELECT count(_data) FROM actors'
        ).fetchone()
        assert row[0] == 3

    def test_retrieve_document_values(self):
        self.db.actors.insert_many([
            {'name': 'Bakery Cumbersome'},
            {'name': 'Beezlebub Cabbagepatch'},
            {'name': 'Bombadil Cottagecheese'},
        ])

        documents = self.db._connection.execute(
            'SELECT _data FROM actors'
        ).fetchall()
        assert len(documents) == 3
        actor_1 = json.loads(documents[0][0])
        actor_2 = json.loads(documents[1][0])
        actor_3 = json.loads(documents[2][0])
        assert actor_1['name'] == 'Bakery Cumbersome'
        assert actor_2['name'] == 'Beezlebub Cabbagepatch'
        assert actor_3['name'] == 'Bombadil Cottagecheese'

    def test_insert_with_single_field_index(self):
        self.db.actors.create_index({
            'name': str,
        })
        self.db.actors.insert_many([
            {'name': 'Bakery Cumbersome'},
            {'name': 'Beezlebub Cabbagepatch'},
            {'name': 'Bombadil Cottagecheese'},
        ])
        documents = self.db._connection.execute(
            'SELECT _data, name FROM actors'
        ).fetchall()
        assert len(documents) == 3

        actor_1 = json.loads(documents[0][0])
        assert actor_1['name'] == 'Bakery Cumbersome'
        assert documents[0][1] == 'Bakery Cumbersome'

        actor_2 = json.loads(documents[1][0])
        assert actor_2['name'] == 'Beezlebub Cabbagepatch'
        assert documents[1][1] == 'Beezlebub Cabbagepatch'

        actor_3 = json.loads(documents[2][0])
        assert actor_3['name'] == 'Bombadil Cottagecheese'
        assert documents[2][1] == 'Bombadil Cottagecheese'

    def test_insert_with_single_field_index_with_missing_field(self):
        self.db.actors.create_index({
            'name': str,
        })
        self.db.actors.insert_many([
            {'name': 'Bakery Cumbersome'},
            {'age': 42},
            {'name': 'Bombadil Cottagecheese'},
        ])
        documents = self.db._connection.execute(
            'SELECT _data, name FROM actors'
        ).fetchall()
        assert len(documents) == 3

        actor_1 = json.loads(documents[0][0])
        assert actor_1['name'] == 'Bakery Cumbersome'
        assert documents[0][1] == 'Bakery Cumbersome'

        actor_2 = json.loads(documents[1][0])
        assert actor_2['age'] == 42
        assert documents[1][1] is None

        actor_3 = json.loads(documents[2][0])
        assert actor_3['name'] == 'Bombadil Cottagecheese'
        assert documents[2][1] == 'Bombadil Cottagecheese'


class TestCollectionFind(BaseTest):

    def setup(self):
        super().setup()
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


class TestCollectionFindWithIndex(BaseTest):

    def test_find_on_single_indexed_text_field(self):
        self.db.users.create_index({'name': str})
        self.db.users.insert_one({'name': 'Boby', 'age': 10})
        self.db.users.insert_one({'name': 'John', 'age': 20})
        self.db.users.insert_one({'name': 'Poopy', 'age': 30})
        result = self.db.users.find({
            'name': {'$eq': 'John'}
        })
        assert len(result) == 1
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'John'

    def test_find_on_single_indexed_integer_field(self):
        self.db.users.create_index({'age': int})
        self.db.users.insert_one({'name': 'Boby', 'age': 10})
        self.db.users.insert_one({'name': 'John', 'age': 20})
        self.db.users.insert_one({'name': 'Poopy', 'age': 30})
        result = self.db.users.find({
            'age': {'$gt': 10}
        })
        assert len(result) == 2
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'John'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Poopy'


class TestCollectionCreateIndex(BaseTest):

    def test_create_index_on_single_text_field(self):
        self.db.users.create_index({
            'name': str,
        })
        row = self.db._connection.execute(
            'SELECT indexed_fields FROM plume_master '
            'WHERE collection_name = "users";'
        ).fetchone()
        indexed_fields = json.loads(row[0])
        assert len(indexed_fields) == 1
        assert indexed_fields[0] == 'name'
        columns = table_info(self.db, 'users')
        assert len(columns) == 3
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'

    def test_create_index_on_single_integer_field(self):
        self.db.users.create_index({
            'age': int,
        })
        row = self.db._connection.execute(
            'SELECT indexed_fields FROM plume_master '
            'WHERE collection_name = "users";'
        ).fetchone()
        indexed_fields = json.loads(row[0])
        assert len(indexed_fields) == 1
        assert indexed_fields[0] == 'age'
        columns = table_info(self.db, 'users')
        assert len(columns) == 3
        assert columns[2][1] == 'age'
        assert columns[2][2] == 'INTEGER'

    def test_create_index_on_single_real_field(self):
        self.db.users.create_index({
            'size': float,
        })
        row = self.db._connection.execute(
            'SELECT indexed_fields FROM plume_master '
            'WHERE collection_name = "users";'
        ).fetchone()
        indexed_fields = json.loads(row[0])
        assert len(indexed_fields) == 1
        assert indexed_fields[0] == 'size'
        columns = table_info(self.db, 'users')
        assert len(columns) == 3
        assert columns[2][1] == 'size'
        assert columns[2][2] == 'REAL'

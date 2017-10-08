# Built-in dependencies
import json

# Internal dependencies
from utils import (
    ACTORS, BaseTest, collection_is_registered, index_list,
    ReadingOpBaseTest, table_info
)


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
        self.db.actors.create_index([('name', str)])
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
        self.db.actors.create_index([('name', str)])
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

    def test_insert_one_with_index_on_single_nest_field(self):
        self.db.users.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        inserted_document = {
            'name': 'Bakery Cumbersome',
            'meta': {
                'social_media': {
                    'mastodon_profile': 'Bakery@Cumbersome'
                }
            }
        }
        self.db.users.insert_one(inserted_document)
        document = self.db._connection.execute(
            'SELECT _data, "meta.social_media.mastodon_profile" FROM users'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == inserted_document
        assert document[1] == 'Bakery@Cumbersome'

    def test_insert_one_with_multiple_single_field_indexes(self):
        self.db.actors.create_index([('name', str)])
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_one(ACTORS[0])
        rows = self.db._connection.execute(
            'SELECT _data, "name", "age" FROM "actors"'
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert json.loads(row[0]) == ACTORS[0]
        assert row[1] == 'Bakery Cumbersome'
        assert row[2] == 10


class TestCollectionInsertMany(BaseTest):

    def test_insert_document_add_new_rows(self):
        self.db.actors.insert_many(ACTORS)
        row = self.db._connection.execute(
            'SELECT count(_data) FROM actors'
        ).fetchone()
        assert row[0] == 3

    def test_retrieve_document_values(self):
        self.db.actors.insert_many(ACTORS)
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
        self.db.actors.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        self.db.actors.insert_many(ACTORS)
        documents = self.db._connection.execute(
            'SELECT _data, "meta.social_media.mastodon_profile" FROM actors'
        ).fetchall()
        assert len(documents) == 3

        actor_1 = json.loads(documents[0][0])
        assert actor_1['name'] == 'Bakery Cumbersome'
        assert documents[0][1] == 'Bakery@Cumbersome'

        actor_2 = json.loads(documents[1][0])
        assert actor_2['name'] == 'Beezlebub Cabbagepatch'
        assert documents[1][1] == 'Beezlebub@Cabbagepatch'

        actor_3 = json.loads(documents[2][0])
        assert actor_3['name'] == 'Bombadil Cottagecheese'
        assert documents[2][1] == 'Bombadil@Cottagecheese'

    def test_insert_with_single_field_index_with_missing_field(self):
        self.db.actors.create_index([('name', str)])
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

    def test_insert_with_single_nested_field_index(self):
        self.db.actors.create_index([('name', str)])
        self.db.actors.insert_many(ACTORS)
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

    def test_insert_many_with_multiple_single_field_indexes(self):
        self.db.actors.create_index([('name', str)])
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        rows = self.db._connection.execute(
            'SELECT _data, "name", "age" FROM "actors"'
        ).fetchall()
        assert len(rows) == 3
        assert json.loads(rows[0][0]) == ACTORS[0]
        assert rows[0][1] == 'Bakery Cumbersome'
        assert rows[0][2] == 10
        assert json.loads(rows[1][0]) == ACTORS[1]
        assert rows[1][1] == 'Beezlebub Cabbagepatch'
        assert rows[1][2] == 20
        assert json.loads(rows[2][0]) == ACTORS[2]
        assert rows[2][1] == 'Bombadil Cottagecheese'
        assert rows[2][2] == 30


class TestCollectionFind(ReadingOpBaseTest):

    def setup_class(cls):
        super().setup_class(cls)
        cls.db.actors.insert_many(ACTORS)

    def test_equal_selector(self):
        result = self.db.actors.find({'age': {'$eq': 20}})
        assert len(result) == 1
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'Beezlebub Cabbagepatch'

    def test_equal_selector_on_nested_field(self):
        result = self.db.actors.find({
            'meta.social_media.mastodon_profile': {
                '$eq': 'Beezlebub@Cabbagepatch'
            }
        })
        assert len(result) == 1
        assert result[0]['name'] == 'Beezlebub Cabbagepatch'
        assert (
            result[0]['meta']['social_media']['mastodon_profile']
            == 'Beezlebub@Cabbagepatch'
        )

    def test_not_equal_selector(self):
        result = self.db.actors.find({'age': {'$ne': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Bakery Cumbersome'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Bombadil Cottagecheese'

    def test_greater_than_selector(self):
        result = self.db.actors.find({'age': {'$gt': 10}})
        assert len(result) == 2
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'Beezlebub Cabbagepatch'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Bombadil Cottagecheese'

    def test_greater_than_equals_selector(self):
        result = self.db.actors.find({'age': {'$gte': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 20
        assert result[0]['name'] == 'Beezlebub Cabbagepatch'
        assert result[1]['age'] == 30
        assert result[1]['name'] == 'Bombadil Cottagecheese'

    def test_lower_than_selector(self):
        result = self.db.actors.find({'age': {'$lt': 30}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Bakery Cumbersome'
        assert result[1]['age'] == 20
        assert result[1]['name'] == 'Beezlebub Cabbagepatch'

    def test_lower_than_equal_selector(self):
        result = self.db.actors.find({'age': {'$lte': 20}})
        assert len(result) == 2
        assert result[0]['age'] == 10
        assert result[0]['name'] == 'Bakery Cumbersome'
        assert result[1]['age'] == 20
        assert result[1]['name'] == 'Beezlebub Cabbagepatch'

    def test_projection_include_field(self):
        query = {'age': {'$gt': 10}}
        projection = {'name': 1}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 1
        assert documents[0]['name'] == 'Beezlebub Cabbagepatch'
        assert len(documents[1]) == 1
        assert documents[1]['name'] == 'Bombadil Cottagecheese'

    def test_projection_include_nested_field(self):
        query = query = {'age': {'$gt': 10}}
        projection = {'meta.social_media.mastodon_profile': 1}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2

        assert len(documents[0]) == 1
        assert (
            documents[0]['meta']['social_media']['mastodon_profile'] ==
            'Beezlebub@Cabbagepatch'
        )
        assert len(documents[1]) == 1
        assert (
            documents[1]['meta']['social_media']['mastodon_profile'] ==
            'Bombadil@Cottagecheese'
        )

    def test_projection_exclude_field(self):
        query = {'age': {'$lte': 20}}
        projection = {'meta': 0}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 3
        assert documents[0]['name'] == 'Bakery Cumbersome'
        assert documents[0]['age'] == 10
        assert documents[0]['size'] == 1.6
        assert len(documents[1]) == 3
        assert documents[1]['name'] == 'Beezlebub Cabbagepatch'
        assert documents[1]['age'] == 20
        assert documents[1]['size'] == 1.7

    def test_projection_exclude_nested_field(self):
        query = {'age': {'$lte': 20}}
        projection = {'meta.social_media.mastodon_profile': 0}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2

        assert len(documents[0]) == 4
        assert documents[0]['name'] == 'Bakery Cumbersome'
        assert documents[0]['age'] == 10
        assert documents[0]['size'] == 1.6
        assert 'mastodon_profile' not in documents[0]['meta']['social_media']
        assert documents[0]['meta']['social_media']['mastodon_followers'] == 10

        assert len(documents[1]) == 4
        assert documents[1]['name'] == 'Beezlebub Cabbagepatch'
        assert documents[1]['age'] == 20
        assert documents[1]['size'] == 1.7
        assert 'mastodon_profile' not in documents[1]['meta']['social_media']
        assert documents[1]['meta']['social_media']['mastodon_followers'] == 20


class TestCollectionFindOne(ReadingOpBaseTest):

    def setup_class(cls):
        super().setup_class(cls)
        cls.db.actors.insert_many(ACTORS)

    def test_equal_selector(self):
        document = self.db.actors.find_one({'age': {'$eq': 20}})
        assert document['age'] == 20
        assert document['name'] == 'Beezlebub Cabbagepatch'

    def test_equal_selector_on_nested_field(self):
        document = self.db.actors.find_one({
            'meta.social_media.mastodon_profile': {
                '$eq': 'Beezlebub@Cabbagepatch'
            }
        })
        assert document['name'] == 'Beezlebub Cabbagepatch'
        assert (
            document['meta']['social_media']['mastodon_profile']
            == 'Beezlebub@Cabbagepatch'
        )

    def test_not_equal_selector(self):
        document = self.db.actors.find_one({'age': {'$ne': 20}})
        assert document['age'] == 10
        assert document['name'] == 'Bakery Cumbersome'

    def test_greater_than_selector(self):
        document = self.db.actors.find_one({'age': {'$gt': 10}})
        assert document['age'] == 20
        assert document['name'] == 'Beezlebub Cabbagepatch'

    def test_greater_than_equals_selector(self):
        document = self.db.actors.find_one({'age': {'$gte': 20}})
        assert document['age'] == 20
        assert document['name'] == 'Beezlebub Cabbagepatch'

    def test_lower_than_selector(self):
        document = self.db.actors.find_one({'age': {'$lt': 30}})
        assert document['age'] == 10
        assert document['name'] == 'Bakery Cumbersome'

    def test_lower_than_equal_selector(self):
        document = self.db.actors.find_one({'age': {'$lte': 20}})
        assert document['age'] == 10
        assert document['name'] == 'Bakery Cumbersome'

    def test_projection_include_field(self):
        query = {'age': {'$lte': 20}}
        projection = {'name': 1}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 1
        assert document['name'] == 'Bakery Cumbersome'

    def test_projection_include_nested_field(self):
        query = {'name': {'$eq': 'Bakery Cumbersome'}}
        projection = {'meta.social_media.mastodon_profile': 1}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 1
        print(document)
        assert (
            document['meta']['social_media']['mastodon_profile'] ==
            'Bakery@Cumbersome'
        )

    def test_projection_exclude_field(self):
        query = {'age': {'$lte': 20}}
        projection = {'meta': 0}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 3
        assert document['name'] == 'Bakery Cumbersome'
        assert document['age'] == 10
        assert document['size'] == 1.6

    def test_projection_exclude_nested_field(self):
        query = {'age': {'$lte': 20}}
        projection = {'meta.social_media.mastodon_profile': 0}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 4
        assert document['name'] == 'Bakery Cumbersome'
        assert document['age'] == 10
        assert document['size'] == 1.6
        assert 'mastodon_profile' not in document['meta']['social_media']
        assert document['meta']['social_media']['mastodon_followers'] == 10


class TestCollectionFindWithIndex(BaseTest):

    def test_find_on_single_indexed_text_field(self):
        self.db.users.create_index([('name', str)])
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
        self.db.users.create_index([('age', int)])
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

    def test_find_on_nested_indexed_field(self):
        self.db.actors.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        self.db.actors.insert_many(ACTORS)
        result = self.db.actors.find({
            'meta.social_media.mastodon_profile': {
                '$eq': 'Beezlebub@Cabbagepatch'
            }
        })
        assert len(result) == 1
        assert result[0]['name'] == 'Beezlebub Cabbagepatch'
        assert (
            result[0]['meta']['social_media']['mastodon_profile']
            == 'Beezlebub@Cabbagepatch'
        )

    def test_selection_and_projection_on_index_field_only(self):
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        query = {'age': {'$gt': 10}}
        projection = {'age': 1}
        result = self.db.actors.find(query, projection)
        assert len(result) == 2
        assert len(result[0]) == 1
        assert result[0]['age'] == 20
        assert len(result[1]) == 1
        assert result[1]['age'] == 30

    def test_projection_include_non_indexed_field(self):
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        query = {'age': {'$lte': 20}}
        projection = {'name': 1}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2
        assert documents[0]['name'] == 'Bakery Cumbersome'
        assert documents[1]['name'] == 'Beezlebub Cabbagepatch'

    def test_projection_include_nested_indexed_field(self):
        self.db.actors.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        self.db.actors.insert_many(ACTORS)
        query = {
            'meta.social_media.mastodon_profile': {
                '$ne': 'Bakery@Cumbersome'
            }
        }
        projection = {'meta.social_media.mastodon_profile': 1}
        documents = self.db.actors.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 1
        assert (
            documents[0]['meta']['social_media']['mastodon_profile'] ==
            'Beezlebub@Cabbagepatch'
        )
        assert len(documents[1]) == 1
        assert (
            documents[1]['meta']['social_media']['mastodon_profile'] ==
            'Bombadil@Cottagecheese'
        )


class TestCollectionFindOneWithIndex(BaseTest):

    def test_find_on_single_indexed_integer_field(self):
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        document = self.db.actors.find_one({
            'age': {'$gt': 10}
        })
        assert document['name'] == 'Beezlebub Cabbagepatch'
        assert document['age'] == 20

    def test_find_on_nested_indexed_field(self):
        self.db.actors.create_index([
            ('meta.social_media.mastodon_followers', int)
        ])
        self.db.actors.insert_many(ACTORS)
        document = self.db.actors.find_one({
            'meta.social_media.mastodon_followers': {
                '$gt': 10
            }
        })
        assert document['name'] == 'Beezlebub Cabbagepatch'
        assert (
            document['meta']['social_media']['mastodon_followers']
            == 20
        )

    def test_projection_include_indexed_field(self):
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        query = {'age': {'$lte': 20}}
        projection = {'age': 1}
        document = self.db.actors.find_one(query, projection)

        assert len(document) == 1
        print(document)
        assert document['age'] == 10

    def test_projection_include_non_indexed_field(self):
        self.db.actors.create_index([('age', int)])
        self.db.actors.insert_many(ACTORS)
        query = {'age': {'$lte': 20}}
        projection = {'name': 1}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 1
        assert document['name'] == 'Bakery Cumbersome'

    def test_projection_include_nested_indexed_field(self):
        self.db.actors.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        self.db.actors.insert_many(ACTORS)
        query = {'name': {'$eq': 'Bakery Cumbersome'}}
        projection = {'meta.social_media.mastodon_profile': 1}
        document = self.db.actors.find_one(query, projection)
        assert len(document) == 1
        assert (
            document['meta']['social_media']['mastodon_profile'] ==
            'Bakery@Cumbersome'
        )


class TestCollectionCreateIndex(BaseTest):

    def test_create_index_on_single_text_field(self):
        self.db.actors.create_index([
            ('name', str)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'actors_index_name'
                }
            ],
            'indexed_fields': ['name'],
            'formated_indexed_fields': ['"name"']
        }

        columns = table_info(self.db, 'actors')
        assert len(columns) == 3
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'actors_index_name'

    def test_create_index_on_single_integer_field(self):
        self.db.actors.create_index([
            ('age', int)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['age', 'INTEGER', 'ASC']],
                    'name': 'actors_index_age'
                }
            ],
            'indexed_fields': ['age'],
            'formated_indexed_fields': ['"age"']
        }

        columns = table_info(self.db, 'actors')
        assert len(columns) == 3
        assert columns[2][1] == 'age'
        assert columns[2][2] == 'INTEGER'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'actors_index_age'

    def test_create_index_on_single_real_field(self):
        self.db.actors.create_index([
            ('size', float)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['size', 'REAL', 'ASC']],
                    'name': 'actors_index_size'
                }
            ],
            'indexed_fields': ['size'],
            'formated_indexed_fields': ['"size"']
        }

        columns = table_info(self.db, 'actors')
        assert len(columns) == 3
        assert columns[2][1] == 'size'
        assert columns[2][2] == 'REAL'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'actors_index_size'

    def test_create_index_on_nested_field(self):
        self.db.actors.create_index([
            ('meta.social_media.mastodon_profile', str)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [
                        ['meta.social_media.mastodon_profile', 'TEXT', 'ASC']
                    ],
                    'name': 'actors_index_meta.social_media.mastodon_profile'
                }
            ],
            'indexed_fields': ['meta.social_media.mastodon_profile'],
            'formated_indexed_fields': ['"meta.social_media.mastodon_profile"']
        }

        columns = table_info(self.db, 'actors')
        assert len(columns) == 3
        assert columns[2][1] == 'meta.social_media.mastodon_profile'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == (
            'actors_index_meta.social_media.mastodon_profile'
        )

    def test_create_multiple_single_field_indexes(self):
        self.db.actors.create_index([
            ('name', str),
        ])
        self.db.actors.create_index([
            ('age', int),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'actors_index_name'
                },
                {
                    'keys': [['age', 'INTEGER', 'ASC']],
                    'name': 'actors_index_age'
                }
            ],
            'indexed_fields': ['name', 'age'],
            'formated_indexed_fields': ['"name"', '"age"']
        }

        columns = table_info(self.db, 'actors')
        assert len(columns) == 4
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'
        assert columns[3][1] == 'age'
        assert columns[3][2] == 'INTEGER'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 2
        assert indexes[0][1] == 'actors_index_age'
        assert indexes[1][1] == 'actors_index_name'

    def test_create_index_with_multiple_fields(self):
        self.db.actors.create_index([
            ('name', str),
            ('age', int),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [
                        ['name', 'TEXT', 'ASC'],
                        ['age', 'INTEGER', 'ASC'],
                    ],
                    'name': 'actors_index_name_age'
                }
            ],
            'indexed_fields': ['name', 'age'],
            'formated_indexed_fields': ['"name"', '"age"']
        }
        columns = table_info(self.db, 'actors')
        assert len(columns) == 4
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'
        assert columns[3][1] == 'age'
        assert columns[3][2] == 'INTEGER'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'actors_index_name_age'

    def test_create_index_on_same_field(self):
        self.db.actors.create_index([
            ('name', str),
        ])
        self.db.actors.create_index([
            ('name', str),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "actors";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'actors_index_name'
                }
            ],
            'indexed_fields': ['name'],
            'formated_indexed_fields': ['"name"']
        }
        columns = table_info(self.db, 'actors')
        assert len(columns) == 3
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"actors"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'actors_index_name'

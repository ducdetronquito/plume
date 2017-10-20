# Built-in dependencies
import json

# Internal dependencies
from utils import ACTORS, BaseTest


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

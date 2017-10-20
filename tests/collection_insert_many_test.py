# Built-in dependencies
import json

# Internal dependencies
from utils import ACTORS, BaseTest


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

# Built-in dependencies
import json

# Internal dependencies
from factories import Persona
from utils import WritingBaseTest, with_index


class InsertManyBaseTest(WritingBaseTest):

    def setup(self):
        super().setup()
        self.personas = Persona.create_batch(3)


class TestCollectionInsertMany(InsertManyBaseTest):

    def test_insert_document_add_new_rows(self):
        self.db.personas.insert_many(self.personas)
        row = self.db._connection.execute(
            'SELECT count(_data) FROM personas'
        ).fetchone()
        assert row[0] == 3

    def test_retrieve_document_values(self):
        self.db.personas.insert_many(self.personas)
        documents = self.db._connection.execute(
            'SELECT _data FROM personas'
        ).fetchall()
        assert len(documents) == 3
        assert json.loads(documents[0][0]) == self.personas[0]
        assert json.loads(documents[1][0]) == self.personas[1]
        assert json.loads(documents[2][0]) == self.personas[2]


class TestCollectionInsertManyWithIndex(InsertManyBaseTest):

    @with_index(index=[('name', str)])
    def test_insert_with_single_field_index(self):
        self.db.personas.insert_many(self.personas)
        documents = self.db._connection.execute(
            'SELECT _data, name FROM personas'
        ).fetchall()
        assert len(documents) == len(self.personas)

        assert json.loads(documents[0][0]) == self.personas[0]
        assert documents[0][1] == self.personas[0]['name']

        assert json.loads(documents[1][0]) == self.personas[1]
        assert documents[1][1] == self.personas[1]['name']

        assert json.loads(documents[2][0]) == self.personas[2]
        assert documents[2][1] == self.personas[2]['name']

    @with_index(index=[('meta.mastodon_profile', str)])
    def test_insert_with_single_nested_field_index(self):
        self.db.personas.insert_many(self.personas)
        documents = self.db._connection.execute(
            'SELECT _data, "meta.mastodon_profile" FROM "personas"'
        ).fetchall()
        assert len(documents) == len(self.personas)

        assert json.loads(documents[0][0]) == self.personas[0]
        assert documents[0][1] == (
            self.personas[0]['meta']['mastodon_profile']
        )

        assert json.loads(documents[1][0]) == self.personas[1]
        assert documents[1][1] == (
            self.personas[1]['meta']['mastodon_profile']
        )

        assert json.loads(documents[2][0]) == self.personas[2]
        assert documents[2][1] == (
            self.personas[2]['meta']['mastodon_profile']
        )

    @with_index(index=[('name', str)])
    def test_insert_with_single_field_index_with_missing_field(self):
        self.personas[1].pop('name')
        self.db.personas.insert_many(self.personas)
        documents = self.db._connection.execute(
            'SELECT _data, name FROM personas'
        ).fetchall()
        assert len(documents) == len(self.personas)

        assert json.loads(documents[0][0]) == self.personas[0]
        assert documents[0][1] == self.personas[0]['name']

        assert json.loads(documents[1][0]) == self.personas[1]
        assert documents[1][1] is None

        assert json.loads(documents[2][0]) == self.personas[2]
        assert documents[2][1] == self.personas[2]['name']

    @with_index(index=[('name', str)])
    @with_index(index=[('age', int)])
    def test_insert_many_with_multiple_single_field_indexes(self):
        self.db.personas.insert_many(self.personas)
        rows = self.db._connection.execute(
            'SELECT _data, "name", "age" FROM "personas"'
        ).fetchall()
        assert len(rows) == len(self.personas)
        assert json.loads(rows[0][0]) == self.personas[0]
        assert rows[0][1] == self.personas[0]['name']
        assert rows[0][2] == self.personas[0]['age']
        assert json.loads(rows[1][0]) == self.personas[1]
        assert rows[1][1] == self.personas[1]['name']
        assert rows[1][2] == self.personas[1]['age']
        assert json.loads(rows[2][0]) == self.personas[2]
        assert rows[2][1] == self.personas[2]['name']
        assert rows[2][2] == self.personas[2]['age']

# Built-in dependencies
import json

# Internal dependencies
from factories import Persona
from utils import WritingBaseTest, with_index


class InsertOneBaseTest(WritingBaseTest):
    def setup(self):
        super().setup()
        self.persona = Persona()


class TestCollectionInsertOne(InsertOneBaseTest):

    def test_insert_document_add_new_row(self):
        self.db.personas.insert_one(self.persona)
        row = self.db._connection.execute(
            'SELECT count(_data) FROM personas'
        ).fetchone()
        assert row[0] == 1

    def test_retrieve_document_values(self):
        self.db.personas.insert_one(self.persona)
        document = self.db._connection.execute(
            'SELECT _data FROM personas'
        ).fetchone()[0]
        actor = json.loads(document)
        assert actor['name'] == self.persona['name']


class TestCollectionInsertOneWithIndex(InsertOneBaseTest):

    @with_index(index=[('name', str)])
    def test_insert_one_with_single_field_index(self):
        self.db.personas.insert_one(self.persona)
        document = self.db._connection.execute(
            'SELECT _data, name FROM personas'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == self.persona
        assert document[1] == self.persona['name']

    @with_index(index=[('name', str)])
    def test_insert_one_with_single_field_index_with_missing_field(self):
        self.persona.pop('name')
        self.db.personas.insert_one(self.persona)
        document = self.db._connection.execute(
            'SELECT _data, name FROM personas'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == self.persona
        assert document[1] is None

    @with_index(index=[('meta.mastodon_profile', str)])
    def test_insert_one_with_index_on_single_nest_field(self):
        self.db.personas.insert_one(self.persona)
        document = self.db._connection.execute(
            'SELECT _data, "meta.mastodon_profile" FROM "personas"'
        ).fetchone()
        assert len(document) == 2
        assert json.loads(document[0]) == self.persona
        assert document[1] == (
            self.persona['meta']['mastodon_profile']
        )

    @with_index(index=[('name', str)])
    @with_index(index=[('age', int)])
    def test_insert_one_with_multiple_single_field_indexes(self):
        self.db.personas.insert_one(self.persona)
        rows = self.db._connection.execute(
            'SELECT _data, "name", "age" FROM "personas"'
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert json.loads(row[0]) == self.persona
        assert row[1] == self.persona['name']
        assert row[2] == self.persona['age']

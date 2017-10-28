# Built-in dependencies
import json

# Internal dependencies
from factories import Persona
from utils import index_list, table_info, WritingBaseTest


class TestCollectionCreateIndex(WritingBaseTest):

    def test_create_index_on_single_text_field(self):
        self.db.personas.create_index([
            ('name', str)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'personas_index_name'
                }
            ],
            'indexed_fields': ['name'],
            'formated_indexed_fields': ['"name"']
        }

        columns = table_info(self.db, 'personas')
        assert len(columns) == 3
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'personas_index_name'

    def test_create_index_on_single_integer_field(self):
        self.db.personas.create_index([
            ('age', int)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['age', 'INTEGER', 'ASC']],
                    'name': 'personas_index_age'
                }
            ],
            'indexed_fields': ['age'],
            'formated_indexed_fields': ['"age"']
        }

        columns = table_info(self.db, 'personas')
        assert len(columns) == 3
        assert columns[2][1] == 'age'
        assert columns[2][2] == 'INTEGER'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'personas_index_age'

    def test_create_index_on_single_real_field(self):
        self.db.personas.create_index([
            ('size', float)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['size', 'REAL', 'ASC']],
                    'name': 'personas_index_size'
                }
            ],
            'indexed_fields': ['size'],
            'formated_indexed_fields': ['"size"']
        }

        columns = table_info(self.db, 'personas')
        assert len(columns) == 3
        assert columns[2][1] == 'size'
        assert columns[2][2] == 'REAL'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'personas_index_size'

    def test_create_index_on_nested_field(self):
        self.db.personas.create_index([
            ('meta.mastodon_profile', str)
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [
                        ['meta.mastodon_profile', 'TEXT', 'ASC']
                    ],
                    'name': 'personas_index_meta.mastodon_profile'
                }
            ],
            'indexed_fields': ['meta.mastodon_profile'],
            'formated_indexed_fields': ['"meta.mastodon_profile"']
        }

        columns = table_info(self.db, 'personas')
        assert len(columns) == 3
        assert columns[2][1] == 'meta.mastodon_profile'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == (
            'personas_index_meta.mastodon_profile'
        )

    def test_create_multiple_single_field_indexes(self):
        self.db.personas.create_index([
            ('name', str),
        ])
        self.db.personas.create_index([
            ('age', int),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'personas_index_name'
                },
                {
                    'keys': [['age', 'INTEGER', 'ASC']],
                    'name': 'personas_index_age'
                }
            ],
            'indexed_fields': ['name', 'age'],
            'formated_indexed_fields': ['"name"', '"age"']
        }

        columns = table_info(self.db, 'personas')
        assert len(columns) == 4
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'
        assert columns[3][1] == 'age'
        assert columns[3][2] == 'INTEGER'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 2
        assert indexes[0][1] == 'personas_index_age'
        assert indexes[1][1] == 'personas_index_name'

    def test_create_index_with_multiple_fields(self):
        self.db.personas.create_index([
            ('name', str),
            ('age', int),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])

        assert indexes == {
            'indexes': [
                {
                    'keys': [
                        ['name', 'TEXT', 'ASC'],
                        ['age', 'INTEGER', 'ASC'],
                    ],
                    'name': 'personas_index_name_age'
                }
            ],
            'indexed_fields': ['name', 'age'],
            'formated_indexed_fields': ['"name"', '"age"']
        }
        columns = table_info(self.db, 'personas')
        assert len(columns) == 4
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'
        assert columns[3][1] == 'age'
        assert columns[3][2] == 'INTEGER'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'personas_index_name_age'

    def test_create_index_on_same_field(self):
        self.db.personas.create_index([
            ('name', str),
        ])
        self.db.personas.create_index([
            ('name', str),
        ])
        row = self.db._connection.execute(
            'SELECT indexes FROM plume_master '
            'WHERE collection_name = "personas";'
        ).fetchone()
        indexes = json.loads(row[0])
        assert indexes == {
            'indexes': [
                {
                    'keys': [['name', 'TEXT', 'ASC']],
                    'name': 'personas_index_name'
                }
            ],
            'indexed_fields': ['name'],
            'formated_indexed_fields': ['"name"']
        }
        columns = table_info(self.db, 'personas')
        assert len(columns) == 3
        assert columns[2][1] == 'name'
        assert columns[2][2] == 'TEXT'

        indexes = index_list(self.db, '"personas"')
        assert len(indexes) == 1
        assert indexes[0][1] == 'personas_index_name'


class TestCollectionCreateIndexOnExistingData(WritingBaseTest):

    def setup(self):
        super().setup()
        self.personas = Persona.create_batch(3)
        self.db.personas.insert_many(self.personas)

    def test_create_index_on_single_text_field(self):
        self.db.personas.create_index([
            ('name', str)
        ])

        rows = self.db._connection.execute(
            'SELECT _data, name FROM personas'
        ).fetchall()

        assert len(rows) == 3
        assert json.loads(rows[0][0]) == self.personas[0]
        assert rows[0][1] == self.personas[0]['name']
        assert json.loads(rows[1][0]) == self.personas[1]
        assert rows[1][1] == self.personas[1]['name']
        assert json.loads(rows[2][0]) == self.personas[2]
        assert rows[2][1] == self.personas[2]['name']

    def test_create_index_on_single_integer_field(self):
        self.db.personas.create_index([
            ('age', int)
        ])
        rows = self.db._connection.execute(
            'SELECT _data, age FROM personas'
        ).fetchall()

        assert len(rows) == 3
        assert json.loads(rows[0][0]) == self.personas[0]
        assert rows[0][1] == self.personas[0]['age']
        assert json.loads(rows[1][0]) == self.personas[1]
        assert rows[1][1] == self.personas[1]['age']
        assert json.loads(rows[2][0]) == self.personas[2]
        assert rows[2][1] == self.personas[2]['age']

    def test_create_index_on_nested_field(self):
        self.db.personas.create_index([
            ('meta.mastodon_profile', str)
        ])
        rows = self.db._connection.execute(
            'SELECT _data, "meta.mastodon_profile" FROM personas'
        ).fetchall()

        assert len(rows) == 3
        assert json.loads(rows[0][0]) == self.personas[0]
        assert (
            rows[0][1] == self.personas[0]['meta']['mastodon_profile']
        )
        assert json.loads(rows[1][0]) == self.personas[1]
        assert (
            rows[1][1] == self.personas[1]['meta']['mastodon_profile']
        )
        assert json.loads(rows[2][0]) == self.personas[2]
        assert (
            rows[2][1] == self.personas[2]['meta']['mastodon_profile']
        )

    def test_create_multiple_single_field_indexes(self):
        self.db.personas.create_index([
            ('name', str),
        ])
        self.db.personas.create_index([
            ('age', int),
        ])
        rows = self.db._connection.execute(
            'SELECT _data, name, age FROM personas'
        ).fetchall()

        assert len(rows) == 3
        assert json.loads(rows[0][0]) == self.personas[0]
        assert rows[0][1] == self.personas[0]['name']
        assert rows[0][2] == self.personas[0]['age']
        assert json.loads(rows[1][0]) == self.personas[1]
        assert rows[1][1] == self.personas[1]['name']
        assert rows[1][2] == self.personas[1]['age']
        assert json.loads(rows[2][0]) == self.personas[2]
        assert rows[2][1] == self.personas[2]['name']
        assert rows[2][2] == self.personas[2]['age']

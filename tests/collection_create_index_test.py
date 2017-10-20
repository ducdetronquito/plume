# Built-in dependencies
import json

# Internal dependencies
from utils import BaseTest, index_list, table_info


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

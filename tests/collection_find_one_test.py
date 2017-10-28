# Internal dependencies
from factories import Persona
from utils import ReadingBaseTest, with_index, WritingBaseTest


class TestCollectionFindOne(ReadingBaseTest):

    def setup_class(cls):
        super().setup_class(cls)
        cls.personas = [
            Persona(age=10, meta__mastodon_followers=10),
            Persona(age=20, meta__mastodon_followers=20),
            Persona(age=30, meta__mastodon_followers=30)
        ]
        cls.db.personas.insert_many(cls.personas)

    def setup(self):
        self.persoans = self.personas

    def test_equal_selector(self):
        document = self.db.personas.find_one({
            'age': {'$eq': self.personas[1]['age']}
        })
        assert document == self.personas[1]

    def test_equal_selector_on_nested_field(self):
        document = self.db.personas.find_one({
            'meta.mastodon_profile': {
                '$eq': self.personas[2]['meta']['mastodon_profile']
            }
        })
        assert document == self.personas[2]

    def test_not_equal_selector(self):
        document = self.db.personas.find_one({
            'age': {'$ne': self.personas[1]['age']}
        })
        assert document == self.personas[0]

    def test_greater_than_selector(self):
        document = self.db.personas.find_one({
            'age': {'$gt': self.personas[0]['age']}
        })
        assert document == self.personas[1]

    def test_greater_than_equals_selector(self):
        document = self.db.personas.find_one({
            'age': {'$gte': self.personas[1]['age']}
        })
        assert document == self.personas[1]

    def test_lower_than_selector(self):
        document = self.db.personas.find_one({
            'age': {'$lt': self.personas[2]['age']}
        })
        assert document == self.personas[0]

    def test_lower_than_equal_selector(self):
        document = self.db.personas.find_one({
            'age': {'$lte': self.personas[1]['age']}
        })
        assert document == self.personas[0]

    def test_projection_include_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'name': 1}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 1
        assert document['name'] == self.personas[0]['name']

    def test_projection_include_nested_field(self):
        query = {'name': {'$eq': self.personas[0]['name']}}
        projection = {'meta.mastodon_profile': 1}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 1
        assert (
            document['meta']['mastodon_profile'] ==
            self.personas[0]['meta']['mastodon_profile']
        )

    def test_projection_exclude_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'meta': 0}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 3
        assert document['name'] == self.personas[0]['name']
        assert document['age'] == self.personas[0]['age']
        assert document['size'] == self.personas[0]['size']

    def test_projection_exclude_nested_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'meta.mastodon_profile': 0}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 4
        assert document['name'] == self.personas[0]['name']
        assert document['age'] == self.personas[0]['age']
        assert document['size'] == self.personas[0]['size']
        assert 'mastodon_profile' not in document['meta']
        assert document['meta']['mastodon_followers'] == (
            self.personas[0]['meta']['mastodon_followers']
        )


class TestCollectionFindOneWithIndex(WritingBaseTest):

    def setup(self):
        super().setup()
        self.personas = [
            Persona(age=10, meta__mastodon_followers=10),
            Persona(age=20, meta__mastodon_followers=20),
            Persona(age=30, meta__mastodon_followers=30)
        ]
        self.db.personas.insert_many(self.personas)

    @with_index(index=[('age', int)])
    def test_find_on_single_indexed_integer_field(self):
        document = self.db.personas.find_one({
            'age': {'$gt': self.personas[0]['age']}
        })
        assert document == self.personas[1]

    @with_index(index=[('meta.mastodon_followers', int)])
    def test_find_on_nested_indexed_field(self):
        document = self.db.personas.find_one({
            'meta.mastodon_followers': {
                '$gt': self.personas[0]['meta']['mastodon_followers']
            }
        })
        assert document == self.personas[1]

    @with_index(index=[('age', int)])
    def test_projection_include_indexed_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'age': 1}
        document = self.db.personas.find_one(query, projection)

        assert len(document) == 1
        assert document['age'] == self.personas[0]['age']

    @with_index(index=[('age', int)])
    def test_projection_include_non_indexed_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'name': 1}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 1
        assert document['name'] == self.personas[0]['name']

    @with_index(index=[('meta.mastodon_profile', str)])
    def test_projection_include_nested_indexed_field(self):
        query = {'name': {'$eq': self.personas[0]['name']}}
        projection = {'meta.mastodon_profile': 1}
        document = self.db.personas.find_one(query, projection)
        assert len(document) == 1
        assert (
            document['meta']['mastodon_profile'] ==
            self.personas[0]['meta']['mastodon_profile']
        )

# Internal dependencies
from factories import Persona
from utils import ReadingBaseTest, with_index, WritingBaseTest


class TestCollectionFind(ReadingBaseTest):

    def setup_class(cls):
        super().setup_class(cls)
        cls.personas = [
            Persona(age=10, meta__mastodon_followers=10),
            Persona(age=20, meta__mastodon_followers=20),
            Persona(age=30, meta__mastodon_followers=30)
        ]
        cls.db.personas.insert_many(cls.personas)

    def setup(self):
        self.personas = self.__class__.personas

    def test_equal_selector(self):
        result = self.db.personas.find({
            'age': {'$eq': self.personas[1]['age']}
        })
        assert len(result) == 1
        assert result[0] == self.personas[1]

    def test_equal_selector_on_nested_field(self):
        result = self.db.personas.find({
            'meta.mastodon_profile': {
                '$eq': self.personas[1]['meta']['mastodon_profile']
            }
        })
        assert len(result) == 1
        assert result[0] == self.personas[1]

    def test_not_equal_selector(self):
        result = self.db.personas.find({
            'age': {'$ne': self.personas[1]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[0]
        assert result[1] == self.personas[2]

    def test_greater_than_selector(self):
        result = self.db.personas.find({
            'age': {'$gt': self.personas[0]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[1]
        assert result[1] == self.personas[2]

    def test_greater_than_equals_selector(self):
        result = self.db.personas.find({
            'age': {'$gte': self.personas[1]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[1]
        assert result[1] == self.personas[2]

    def test_lower_than_selector(self):
        result = self.db.personas.find({
            'age': {'$lt': self.personas[2]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[0]
        assert result[1] == self.personas[1]

    def test_lower_than_equal_selector(self):
        result = self.db.personas.find({
            'age': {'$lte': self.personas[1]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[0]
        assert result[1] == self.personas[1]

    def test_projection_include_field(self):
        query = {'age': {'$gt': self.personas[0]['age']}}
        projection = {'name': 1}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 1
        assert documents[0]['name'] == self.personas[1]['name']
        assert len(documents[1]) == 1
        assert documents[1]['name'] == self.personas[2]['name']

    def test_projection_include_nested_field(self):
        query = query = {'age': {'$gt': self.personas[0]['age']}}
        projection = {'meta.mastodon_profile': 1}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 1
        assert (
            documents[0]['meta']['mastodon_profile'] ==
            self.personas[1]['meta']['mastodon_profile']
        )
        assert len(documents[1]) == 1
        assert (
            documents[1]['meta']['mastodon_profile'] ==
            self.personas[2]['meta']['mastodon_profile']
        )

    def test_projection_exclude_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'meta': 0}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 3
        assert documents[0]['name'] == self.personas[0]['name']
        assert documents[0]['age'] == self.personas[0]['age']
        assert documents[0]['size'] == self.personas[0]['size']
        assert len(documents[1]) == 3
        assert documents[1]['name'] == self.personas[1]['name']
        assert documents[1]['age'] == self.personas[1]['age']
        assert documents[1]['size'] == self.personas[1]['size']

    def test_projection_exclude_nested_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'meta.mastodon_profile': 0}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 4
        assert documents[0]['name'] == self.personas[0]['name']
        assert documents[0]['age'] == self.personas[0]['age']
        assert documents[0]['size'] == self.personas[0]['size']
        assert 'mastodon_profile' not in documents[0]['meta']
        assert documents[0]['meta']['mastodon_followers'] == (
            self.personas[0]['meta']['mastodon_followers']
        )

        assert len(documents[1]) == 4
        assert documents[1]['name'] == self.personas[1]['name']
        assert documents[1]['age'] == self.personas[1]['age']
        assert documents[1]['size'] == self.personas[1]['size']
        assert 'mastodon_profile' not in documents[1]['meta']
        assert documents[1]['meta']['mastodon_followers'] == (
            self.personas[1]['meta']['mastodon_followers']
        )


class TestCollectionFindWithIndex(WritingBaseTest):

    def setup(self):
        super().setup()
        self.personas = [
            Persona(age=10, meta__mastodon_followers=10),
            Persona(age=20, meta__mastodon_followers=20),
            Persona(age=30, meta__mastodon_followers=30)
        ]
        self.db.personas.insert_many(self.personas)

    @with_index(index=[('name', str)])
    def test_find_on_single_indexed_text_field(self):
        result = self.db.personas.find({
            'name': {'$eq': self.personas[0]['name']}
        })
        assert len(result) == 1
        assert result[0] == self.personas[0]

    @with_index(index=[('age', int)])
    def test_find_on_single_indexed_integer_field(self):
        result = self.db.personas.find({
            'age': {'$gt': self.personas[0]['age']}
        })
        assert len(result) == 2
        assert result[0] == self.personas[1]
        assert result[1] == self.personas[2]

    @with_index(index=[('meta.mastodon_profile', str)])
    def test_find_on_nested_indexed_field(self):
        result = self.db.personas.find({
            'meta.mastodon_profile': {
                '$eq': self.personas[1]['meta']['mastodon_profile']
            }
        })
        assert len(result) == 1
        assert result[0] == self.personas[1]

    @with_index(index=[('age', int)])
    def test_selection_and_projection_on_index_field_only(self):
        query = {'age': {'$gt': self.personas[0]['age']}}
        projection = {'age': 1}
        result = self.db.personas.find(query, projection)
        assert len(result) == 2
        assert len(result[0]) == 1
        assert result[0]['age'] == self.personas[1]['age']
        assert len(result[1]) == 1
        assert result[1]['age'] == self.personas[2]['age']

    @with_index(index=[('age', int)])
    def test_projection_include_non_indexed_field(self):
        query = {'age': {'$lte': self.personas[1]['age']}}
        projection = {'name': 1}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert documents[0]['name'] == self.personas[0]['name']
        assert documents[1]['name'] == self.personas[1]['name']

    @with_index(index=[('meta.mastodon_followers', int)])
    def test_projection_include_nested_indexed_field(self):
        query = {
            'meta.mastodon_followers': {
                '$gt': self.personas[0]['meta']['mastodon_followers']
            }
        }
        projection = {'meta.mastodon_followers': 1}
        documents = self.db.personas.find(query, projection)
        assert len(documents) == 2
        assert len(documents[0]) == 1
        assert (
            documents[0]['meta']['mastodon_followers'] ==
            self.personas[1]['meta']['mastodon_followers']
        )
        assert len(documents[1]) == 1
        assert (
            documents[1]['meta']['mastodon_followers'] ==
            self.personas[2]['meta']['mastodon_followers']
        )

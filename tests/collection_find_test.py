# Internal dependencies
from utils import ACTORS, BaseTest, ReadingOpBaseTest


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

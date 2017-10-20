# Internal dependencies
from utils import ACTORS, BaseTest, ReadingOpBaseTest


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

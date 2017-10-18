# Built-in dependencies
import os

# Internal dependencies
from plume import Database

ACTORS = [
    {
        'name': 'Bakery Cumbersome',
        'age': 10,
        'size': 1.6,
        'meta': {
            'social_media': {
                'mastodon_profile': 'Bakery@Cumbersome',
                'mastodon_followers': 10
            }
        }
    },
    {
        'name': 'Beezlebub Cabbagepatch',
        'age': 20,
        'size': 1.7,
        'meta': {
            'social_media': {
                'mastodon_profile': 'Beezlebub@Cabbagepatch',
                'mastodon_followers': 20
            }
        }
    },
    {
        'name': 'Bombadil Cottagecheese',
        'age': 30,
        'size': 1.8,
        'meta': {
            'social_media': {
                'mastodon_profile': 'Bombadil@Cottagecheese',
                'mastodon_followers': 30
            }
        }
    },
]


class BaseTest:
    def setup(self):
        self.db = Database('test.db')

    def teardown(self):
        os.remove('test.db')


class ReadingOpBaseTest:
    def setup_class(cls):
        cls.db = Database('test.db')

    def teardown_class(self):
        os.remove('test.db')


def collection_is_registered(db, collection_name):
    query = (
        "SELECT name FROM sqlite_master WHERE "
        "type='table' AND name='{}';"
    ).format(collection_name)

    result = db._connection.execute(query).fetchone()
    return result


def table_info(db, table_name):
    query = 'PRAGMA table_info({})'.format(table_name)
    return db._connection.execute(query).fetchall()


def index_list(db, table_name):
    query = 'PRAGMA index_list({})'.format(table_name)
    return db._connection.execute(query).fetchall()

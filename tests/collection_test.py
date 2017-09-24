# Built-in dependencies
import os

# Internal dependencies
from plume import Collection, SQLiteDB
from utils import collection_is_registered


class TestCollection:
    
    def setup(self):
        self.db = SQLiteDB('test.db')
    
    def test_create_and_register_collection(self):
        assert not collection_is_registered(self.db, 'users')
        collection = self.db.users
        collection._create()
        assert collection_is_registered(self.db, 'users')
        assert collection._name == 'users'
        assert collection._created is True
        assert 'users' in self.db._collections
    
    def teardown(self):
        os.remove('test.db')

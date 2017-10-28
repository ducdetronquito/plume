# Internal dependencies
from utils import WritingBaseTest, collection_is_registered


class TestCollectionCreation(WritingBaseTest):

    def test_create_and_register_collection(self):
        assert not collection_is_registered(self.db, 'users')
        collection = self.db.users
        collection._register()
        assert collection_is_registered(self.db, 'users')
        assert collection._name == 'users'
        assert collection._registered is True
        assert 'users' in self.db._collections

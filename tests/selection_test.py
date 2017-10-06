# Internal dependencies
from plume import Collection, SelectQuery


class TestSQLSelection:

    def setup_class(cls):
        cls._collection = Collection(None, 'users')

    def test_select_non_indexed_field(self):
        select_query = SelectQuery(self._collection, set([]), {
            'name': {'$eq': 'John'}
        })

        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users'
        assert sql_query == expected

    def test_select_indexed_text_field(self):
        select_query = SelectQuery(self._collection, set(['name']), {
            'name': {'$eq': 'John'}
        })

        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users WHERE "name" = "John"'
        assert sql_query == expected

    def test_select_indexed_integer_field(self):
        select_query = SelectQuery(self._collection, set(['age']), {
            'age': {'$eq': 42}
        })

        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users WHERE "age" = 42'
        assert sql_query == expected

    def test_select_indexed_float_field(self):
        select_query = SelectQuery(self._collection, set(['size']), {
            'size': {'$eq': 1.66}
        })

        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users WHERE "size" = 1.66'
        assert sql_query == expected

    def test_select_implicit_and(self):
        indexed_fields = set(['age'])
        query = {'age': {'$gt': 18, '$lt': 42}}
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users WHERE "age" > 18 AND "age" < 42'
        assert sql_query == expected

    def test_select_implicit_and_on_non_indexed_field(self):
        indexed_fields = set(['name'])
        query = {'age': {'$gt': 18, '$lt': 42}}
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = "SELECT _data FROM users"
        assert sql_query == expected

    def test_select_and_two_fields(self):
        indexed_fields = set(['age', 'name'])
        query = {
            '$and': [
                {'name': 'Mario'},
                {'age': {'$gt': 18}}, {'age': {'$lt': 42}}
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = (
            'SELECT _data FROM users WHERE '
            '"name" = "Mario" AND "age" > 18 AND "age" < 42'
        )
        assert sql_query == expected

    def test_select_and(self):
        indexed_fields = set(['age'])
        query = {
            '$and': [{'age': {'$gt': 18}}, {'age': {'$lt': 42}}]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users WHERE "age" > 18 AND "age" < 42'
        assert sql_query == expected

    def test_select_and_with_implicit_and(self):
        indexed_fields = set(['age', 'size'])
        query = {
            '$and': [
                {'age': {'$gt': 18, '$lt': 42}},
                {'size': {'$gt': 1.60, '$lt': 1.90}},
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = (
            'SELECT _data FROM users WHERE '
            '"age" > 18 AND "age" < 42 AND "size" > 1.6 AND "size" < 1.9'
        )
        assert sql_query == expected

    def test_select_or_on_same_indexed_field(self):
        indexed_fields = set(['name'])
        query = {
            '$or': [
                {'name': 'Mario'},
                {'name': 'Luigi'},
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = (
            'SELECT _data FROM users WHERE '
            '"name" = "Mario" OR "name" = "Luigi"'
        )
        assert sql_query == expected

    def test_select_or_in_implicit_and(self):
        indexed_fields = set(['age', 'name'])
        query = {
            'age': 42,
            '$or': [
                {'name': 'Mario'},
                {'name': 'Luigi'},
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = (
            'SELECT _data FROM users WHERE '
            '"age" = 42 AND "name" = "Mario" OR "name" = "Luigi"'
        )
        assert sql_query == expected

    def test_select_or_with_nested_and_clause(self):
        indexed_fields = set(['age', 'name'])
        query = {
            '$or': [
                {'name': 'Mario'},
                {'name': 'Luigi'},
                {
                    '$and': [
                        {'age': {'$gt': 18}},
                        {'age': {'$lt': 42}},
                    ]
                }
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = (
            'SELECT _data FROM users WHERE '
            '"name" = "Mario" OR "name" = "Luigi" OR "age" > 18 AND "age" < 42'
        )
        assert sql_query == expected

    def test_select_or_with_nested_with_clause_not_indexed(self):
        indexed_fields = set(['name'])
        query = {
            '$or': [
                {'name': 'Mario'},
                {'name': 'Luigi'},
                {
                    '$and': [
                        {'age': {'$gt': 18}},
                        {'age': {'$lt': 42}},
                    ]
                }
            ]
        }
        select_query = SelectQuery(self._collection, indexed_fields, query)
        sql_query = select_query.sql_query()
        expected = 'SELECT _data FROM users'
        assert sql_query == expected

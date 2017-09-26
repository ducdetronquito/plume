# Internal dependencies
from plume import Selection


class TestSelection:

    def test_sql_selection_on_indexed_text_field(self):
        selection = Selection('users', set(['name']), {
            'name': {'$eq': 'John'}
        })

        query = selection.sql_query()
        expected = 'SELECT _data, name FROM users WHERE name = "John"'
        assert query == expected

    def test_sql_selection_on_indexed_integer_field(self):
        selection = Selection('users', set(['age']), {
            'age': {'$eq': 42}
        })

        query = selection.sql_query()
        expected = 'SELECT _data, age FROM users WHERE age = 42'
        assert query == expected

    def test_sql_election_on_index_field(self):
        selection = Selection('users', set([]), {
            'name': {'$eq': 'John'}
        })

        query = selection.sql_query()
        expected = 'SELECT _data FROM users'
        assert query == expected

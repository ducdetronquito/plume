# Built-in dependencies
import json
# Internal dependencies
from utils import WritingBaseTest, with_documents, with_index


class TestCollectionReplaceOne(WritingBaseTest):

    @with_documents(number=3)
    def test_replace(self):
        searched_name = self.personas[0]['name']
        query = {'name': searched_name}
        replacement = {'name': 'Barister Candycrush'}
        rows = self.db._connection.execute(
            'SELECT id, _data FROM personas'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == searched_name:
                initial_id = row[0]
                break

        self.db.personas.replace_one(query, replacement)

        rows = self.db._connection.execute(
            'SELECT id, _data FROM personas'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Barister Candycrush':
                new_id = row[0]
                new_document = document
                break

        assert new_id == initial_id
        assert new_document == replacement

    @with_documents(number=3)
    def test_upsert(self):
        query = {'name': 'Undefined'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data FROM personas'
        ).fetchall()
        names = [json.loads(row[1])['name'] for row in rows]
        assert 'Undefined' not in names

        count = self.db._connection.execute(
            'SELECT count(*) FROM personas'
        ).fetchone()[0]
        assert count == 3

        self.db.personas.replace_one(query, replacement, True)

        count = self.db._connection.execute(
            'SELECT count(*) FROM personas'
        ).fetchone()[0]
        assert count == 4

        rows = self.db._connection.execute(
            'SELECT id, _data FROM personas'
        ).fetchall()

        assert rows[3][0] == 4
        assert json.loads(rows[3][1]) == replacement


class TestCollectionReplaceOneWithIndex(WritingBaseTest):

    @with_index(index=[('name', str)])
    @with_documents(number=3)
    def test_replace(self):
        searched_name = self.personas[0]['name']
        query = {'name': searched_name}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM personas'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == searched_name:
                initial_id = row[0]
                break

        self.db.personas.replace_one(query, replacement)

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM personas'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Barister Candycrush':
                new_id = row[0]
                new_document = document
                new_name = row[2]
                break

        assert new_id == initial_id
        assert new_document == replacement
        assert new_name == 'Barister Candycrush'

    @with_index(index=[('name', str)])
    @with_documents(number=3)
    def test_upsert(self):
        query = {'name': 'Undefined'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data FROM personas'
        ).fetchall()
        names = [json.loads(row[1])['name'] for row in rows]
        assert 'Undefined' not in names

        count = self.db._connection.execute(
            'SELECT count(*) FROM personas'
        ).fetchone()[0]
        assert count == 3

        self.db.personas.replace_one(query, replacement, True)

        count = self.db._connection.execute(
            'SELECT count(*) FROM personas'
        ).fetchone()[0]
        assert count == 4

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM personas'
        ).fetchall()

        assert rows[3][0] == 4
        assert json.loads(rows[3][1]) == replacement
        assert rows[3][2] == 'Barister Candycrush'

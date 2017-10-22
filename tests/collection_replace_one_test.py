# Built-in dependencies
import json

# Internal dependencies
from utils import ACTORS, BaseTest


class TestCollectionReplaceOne(BaseTest):

    def test_replace(self):
        self.db.actors.insert_many(ACTORS)
        query = {'name': 'Beezlebub Cabbagepatch'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data FROM actors'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Beezlebub Cabbagepatch':
                initial_id = row[0]

        self.db.actors.replace_one(query, replacement)

        rows = self.db._connection.execute(
            'SELECT id, _data FROM actors'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Barister Candycrush':
                new_id = row[0]
                new_document = document

        assert new_id == initial_id
        assert new_document == replacement

    def test_upsert(self):
        self.db.actors.insert_many(ACTORS)
        query = {'name': 'Undefined'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data FROM actors'
        ).fetchall()
        names = [json.loads(row[1])['name'] for row in rows]
        assert 'Undefined' not in names

        count = self.db._connection.execute(
            'SELECT count(*) FROM actors'
        ).fetchone()[0]
        assert count == 3

        self.db.actors.replace_one(query, replacement, True)

        count = self.db._connection.execute(
            'SELECT count(*) FROM actors'
        ).fetchone()[0]
        assert count == 4

        rows = self.db._connection.execute(
            'SELECT id, _data FROM actors'
        ).fetchall()

        assert rows[3][0] == 4
        assert json.loads(rows[3][1]) == replacement


class TestCollectionReplaceOneWithIndex(BaseTest):

    def test_replace(self):
        self.db.actors.create_index(('name', str))
        self.db.actors.insert_many(ACTORS)
        query = {'name': 'Beezlebub Cabbagepatch'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM actors'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Beezlebub Cabbagepatch':
                initial_id = row[0]

        self.db.actors.replace_one(query, replacement)

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM actors'
        ).fetchall()
        for row in rows:
            document = json.loads(row[1])
            if document['name'] == 'Barister Candycrush':
                new_id = row[0]
                new_document = document
                new_name = row[2]

        assert new_id == initial_id
        assert new_document == replacement
        assert new_name == 'Barister Candycrush'

    def test_upsert(self):
        self.db.actors.create_index(('name', str))
        self.db.actors.insert_many(ACTORS)
        query = {'name': 'Undefined'}
        replacement = {'name': 'Barister Candycrush'}

        rows = self.db._connection.execute(
            'SELECT id, _data FROM actors'
        ).fetchall()
        names = [json.loads(row[1])['name'] for row in rows]
        assert 'Undefined' not in names

        count = self.db._connection.execute(
            'SELECT count(*) FROM actors'
        ).fetchone()[0]
        assert count == 3

        self.db.actors.replace_one(query, replacement, True)

        count = self.db._connection.execute(
            'SELECT count(*) FROM actors'
        ).fetchone()[0]
        assert count == 4

        rows = self.db._connection.execute(
            'SELECT id, _data, name FROM actors'
        ).fetchall()

        assert rows[3][0] == 4
        assert json.loads(rows[3][1]) == replacement
        assert rows[3][2] == 'Barister Candycrush'

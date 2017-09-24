# Built-in dependencies
import json
import sqlite3


class Collection:
    __slots__ = ('_db', '_name', '_registered')

    def __init__(self, db, name: str, **kwargs):
        self._db = db
        self._name = name
        self._registered = kwargs.get('registered', False)

    def _register(self):
        if self._registered:
            return

        create_collection_query = """
            CREATE TABLE {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                _data TEXT NOT NULL
            );
        """.format(self._name)
        self._db._connection.execute(create_collection_query)

        self._db._connection.execute(
            "INSERT INTO plume_master(collection_name)"
            " VALUES (?)", (self._name,)
        )
        self._db._connection.commit()
        self._db._collections[self._name] = self
        self._registered = True

    def insert_one(self, document: dict):
        if not self._registered:
            self._register()

        json_str = json.dumps(document)
        insert_one_query = """
            INSERT INTO {}(_data) VALUES (?)
        """.format(self._name)
        self._db._connection.execute(insert_one_query, (json_str,))
        self._db._connection.commit()


class SQLiteDB:
    __slots__ = ('_collections', '_connection', '_db_name',)

    def __init__(self, db_name: str):
        self._db_name = db_name
        self._collections = {}
        self._connection = sqlite3.connect(self._db_name)

        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS plume_master ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
            "collection_name TEXT UNIQUE NOT NULL)"
        )

        collections = self._connection.execute(
            "SELECT collection_name FROM plume_master;"
        ).fetchall()

        for collection_name, _indexes in collections:
            collection = Collection(self, collection_name)
            self._collections[collection_name] = collection

    def __getattr__(self, collection_name: str) -> Collection:
        try:
            return self._collections[collection_name]
        except KeyError:
            return Collection(self, collection_name)

# Built-in dependencies
import sqlite3


class Collection:
    __slots__ = ('_created', '_db', '_name')

    def __init__(self, db, name, **kwargs):
        self._db = db
        self._name = name
        self._created = kwargs.get('created', False)

    def _create(self):
        if self._created:
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
        self._created = True


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

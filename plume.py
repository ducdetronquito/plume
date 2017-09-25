# Built-in dependencies
import json
import sqlite3


class ComparisonSelector:
    __slots__ = ('field', 'value')

    def __init__(self, field: str, value) -> None:
        self.field = field
        self.value = value


class EqualSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) == self.value


class GreaterThanSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) > self.value


class GreaterThanOrEqualSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) >= self.value


class LowerThanSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) < self.value


class LowerThanOrEqualSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) <= self.value


class NotEqualSelector(ComparisonSelector):
    __slots__ = ()

    def allows(self, document: dict) -> bool:
        return document.get(self.field) != self.value


class Selection:
    __slots__ = ('_selectors',)

    SELECTORS = {
        '$eq': EqualSelector,
        '$gt': GreaterThanSelector,
        '$gte': GreaterThanOrEqualSelector,
        '$lt': LowerThanSelector,
        '$lte': LowerThanOrEqualSelector,
        '$ne': NotEqualSelector,
    }

    def __init__(self, query: dict) -> None:
        self._selectors = []

        for field, selectors in query.items():
            for name, value in selectors.items():
                selector = self.SELECTORS[name](field, value)
                self._selectors.append(selector)

    def match(self, document: dict) -> bool:
        for selector in self._selectors:
            if not selector.allows(document):
                return False

        return True


class Collection:
    __slots__ = ('_db', '_name', '_indexed_fields', '_registered')

    INDEX_TYPES = {
        str: 'TEXT',
        int: 'INTEGER',
        float: 'REAL',
    }

    def __init__(self, db, name: str, **kwargs) -> None:
        self._db = db
        self._name = name
        self._registered = kwargs.get('registered', False)
        self._indexed_fields = set(kwargs.get('indexed_fields', []))

    def _register(self):
        if self._registered:
            return

        create_collection_query = """
            CREATE TABLE {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                _data BLOB NOT NULL
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

    def create_index(self, index: dict) -> None:
        if not self._registered:
            self._register()

        # Create a dedicated column for each indexed field.
        create_column_query = "ALTER TABLE {} ADD COLUMN {} {}"
        for field, _type in index.items():
            self._db._connection.execute(
                create_column_query.format(
                    self._name,
                    field,
                    self.INDEX_TYPES[_type]
                )
            )

        # Create the Index on the column previously created.
        field_names = list(index.keys())
        index_name = '_'.join((self._name, 'index', *field_names))
        csv_fields = ', '.join(field_names)
        create_index = "CREATE INDEX {} ON {}({})".format(
            index_name,
            self._name,
            csv_fields
        )
        self._db._connection.execute(create_index)

        # Register the new index on the master table for given table.
        for field in field_names:
            self._indexed_fields.add(field)
        json_indexes = json.dumps(list(self._indexed_fields))
        update_master = (
            'UPDATE plume_master SET indexed_fields = ? '
            'WHERE collection_name = "{}"'
        ).format(self._name)
        self._db._connection.execute(update_master, [json_indexes])

    def find(self, query: dict) -> list:
        selection = Selection(query)

        select_query = "SELECT _data FROM {}".format(self._name)
        result = self._db._connection.execute(select_query).fetchall()

        documents = (json.loads(row[0]) for row in result)
        return [
            document for document in documents if selection.match(document)
        ]

    def insert_one(self, document: dict) -> None:
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

    def __init__(self, db_name: str) -> None:
        self._db_name = db_name
        self._collections = {}
        self._connection = sqlite3.connect(self._db_name)

        self._connection.execute(
            'CREATE TABLE IF NOT EXISTS plume_master ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
            'collection_name TEXT UNIQUE NOT NULL,'
            'indexed_fields TEXT DEFAULT "[]")'
        )

        collections = self._connection.execute(
            "SELECT collection_name, indexed_fields FROM plume_master;"
        ).fetchall()

        for collection_name, indexed_fields in collections:
            collection = Collection(
                self,
                collection_name,
                indexed_fields=indexed_fields
            )
            self._collections[collection_name] = collection

    def __getattr__(self, collection_name: str) -> Collection:
        try:
            return self._collections[collection_name]
        except KeyError:
            return Collection(self, collection_name)

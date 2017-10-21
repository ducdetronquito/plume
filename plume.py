# Built-in dependencies
import json
import operator
import sqlite3


ASCENDING = 'ASC'
DESCENDING = 'DESC'


class BadProjection(ValueError):
    pass


def _nested_get(document: dict, field: str):
    nested_fields = field.split('.')
    value = document
    try:
        for field in nested_fields:
            value = value[field]
        return value
    except KeyError:
        return None


def _nested_pop(document, field):
    *parent_fields, last_field = field.split('.')
    parent = document
    try:
        for field in parent_fields:
            parent = parent[field]
        return parent.pop(last_field, None)
    except KeyError:
        return None


def _nested_set(document, field, value):
    *parent_fields, last_field = field.split('.')
    parent = document
    try:
        for field in parent_fields:
            parent = parent.setdefault(field, {})
        parent[last_field] = value
    except KeyError:
        return None


class Selector:

    def get_selector(self, key: str, expression):
        if key[0] != '$':
            if not isinstance(expression, dict):
                return Equal(key, expression)
            elif len(expression) == 1:
                operator, value = expression.popitem()
                return self.get_field_selector(key, operator, value)
            else:
                return ImplicitAnd(key, expression)
        elif key == '$and':
            return And(expression)
        elif key == '$or':
            return Or(expression)

    def get_field_selector(self, field: str, operator: str, value):
        if operator == '$eq':
            return Equal(field, value)
        elif operator == '$gt':
            return GreaterThan(field, value)
        elif operator == '$gte':
            return GreaterThanOrEqual(field, value)
        elif operator == '$lt':
            return LowerThan(field, value)
        elif operator == '$lte':
            return LowerThanOrEqual(field, value)
        elif operator == '$ne':
            return NotEqual(field, value)


class And(Selector):
    __slots__ = ('_selectors',)

    def __init__(self, expressions: list) -> None:
        self._selectors = []
        for expression in expressions:
            key, value = expression.popitem()
            selector = self.get_selector(key, value)
            self._selectors.append(selector)

    def is_empty(self) -> bool:
        return False if self._selectors else True

    def match(self, document: dict) -> bool:
        for selector in self._selectors:
            if not selector.match(document):
                return False
        return True

    def sql(self, indexed_fields: set) -> str:
        index_clauses = []
        non_index_selectors = []
        for selector in self._selectors:
            clause = selector.sql(indexed_fields)
            if clause:
                index_clauses.append(clause)
            else:
                non_index_selectors.append(selector)

        if index_clauses:
            self._selectors = non_index_selectors
            return ' AND '.join(index_clauses)


class ImplicitAnd(And):
    __slots__ = ('_selectors',)

    def __init__(self, field: str, expression: dict) -> None:
        self._selectors = []
        for operator, value in expression.items():
            if operator == '$and':
                selector = And(value)
            elif operator == '$or':
                selector = Or(value)
            else:
                selector = self.get_field_selector(field, operator, value)
            self._selectors.append(selector)


class Or(Selector):
    __slots__ = ('_selectors',)

    def __init__(self, expressions: list) -> None:
        self._selectors = []
        for expression in expressions:
            key, value = expression.popitem()
            selector = self.get_selector(key, value)
            self._selectors.append(selector)

    def match(self, document: dict) -> bool:
        for selector in self._selectors:
            if selector.match(document):
                return True
        return False

    def sql(self, indexed_fields: set) -> str:
        index_clauses = []
        for selector in self._selectors:
            clause = selector.sql(indexed_fields)
            if clause:
                index_clauses.append(clause)

        if len(index_clauses) != len(self._selectors):
            return None

        or_clause = ' OR '.join('(' + clause + ')' for clause in index_clauses)
        self._selectors = []
        return '(' + or_clause + ')'


class ComparisonSelector(Selector):
    __slots__ = ('_field', '_value')

    def __init__(self, field: str, value) -> None:
        self._field = field
        self._value = value

    def match(self, document: dict) -> bool:
        return self._operator(
            _nested_get(document, self._field),
            self._value
        )

    def sql(self, indexed_fields: set) -> bool:
        if self._field in indexed_fields:
            return ' '.join([
                '"' + self._field + '"', self._sql_operator, str(self._value)
            ])


class Equal(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '='
    _operator = operator.__eq__

    def sql(self, indexed_fields: set) -> str:
        if self._field not in indexed_fields:
            return None

        if isinstance(self._value, str):
            value = '"' + self._value + '"'
        else:
            value = self._value
        return ' '.join([
            '"' + self._field + '"', self._sql_operator, str(value)
        ])


class GreaterThan(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '>'
    _operator = operator.__gt__


class GreaterThanOrEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '>='
    _operator = operator.__ge__


class LowerThan(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '<'
    _operator = operator.__lt__


class LowerThanOrEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '<='
    _operator = operator.__le__


class NotEqual(ComparisonSelector):
    __slots__ = ()
    _sql_operator = '!='
    _operator = operator.__ne__

    def sql(self, indexed_fields: set) -> str:
        if self._field in indexed_fields:
            if isinstance(self._value, str):
                value = '"' + self._value + '"'
            else:
                value = self._value
            return ' '.join([
                '"' + self._field + '"', self._sql_operator, str(value)
            ])


class Transaction:
    """
        A transactional context manager.

        Within a Transation a RESERVED lock is acquired immediately,
        meaning that only concurrent reading operations are allowed during
        this time.
    """
    __slots__ = ('_connection')

    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        self._connection.execute('BEGIN IMMEDIATE')

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._connection.execute('ROLLBACK')
        else:
            self._connection.execute('COMMIT')


def transactional(fun):
    """Decorator that wrap an operation into a transaction."""
    def transactional_wrapper(*args, **kwargs):
        connection = args[0]._db._connection
        if connection.in_transaction:
            return fun(*args, **kwargs)

        with Transaction(connection):
            return fun(*args, **kwargs)

    return transactional_wrapper


class SelectQuery:
    __slots__ = (
        '_collection', '_exclude_fields', '_include_fields',
        '_indexed_fields', '_limit', '_query_tree',
    )

    def __init__(self, collection, indexed_fields: set,
                 query: dict, projection: dict=None, limit: int=None) -> None:
        self._collection = collection
        self._indexed_fields = set(indexed_fields)
        self._limit = limit
        query = [{key: value} for key, value in query.items()]
        self._query_tree = And(query)

        self._include_fields = set()
        self._exclude_fields = set()
        if projection is None:
            return
        for field, presence in projection.items():
            if presence == 1:
                self._include_fields.add(field)
            elif presence == 0:
                self._exclude_fields.add(field)

        if self._include_fields and self._exclude_fields:
            msg = 'A projection can only include or exclude fields:\n {}'
            raise BadProjection(msg.format(projection))

    def match_many(self, documents: list) -> list:
        # If all filters have been applyed on indexed field
        # we can return the documents directly.
        if not self._query_tree._selectors:
            return list(documents)

        return (
            document for document in documents
            if self._query_tree.match(document)
        )

    def match_one(self, documents: list) -> dict:
        if documents and self._query_tree.is_empty():
            return list(documents)[0]

        for document in documents:
            if self._query_tree.match(document):
                return document

    def _projection_is_index_only(self):
        return (self._include_fields and self._include_fields.issubset(
            self._indexed_fields)
        )

    def _skim(self, document: dict) -> dict:
        if self._include_fields:
            new_document = {}
            for field in self._include_fields:
                value = _nested_get(document, field)
                _nested_set(new_document, field, value)
            return new_document
        elif self._exclude_fields:
            for field in self._exclude_fields:
                _nested_pop(document, field)
            return document
        else:
            return document

    def _sql_query(self) -> str:
        select_query = ['SELECT']
        where_clause = self._query_tree.sql(self._indexed_fields)

        if (self._projection_is_index_only() and self._query_tree.is_empty()):
            fields = ', '.join(
                '"' + field + '"' for field in self._indexed_fields
            )
            select_query.append(fields)
        else:
            select_query.append('_data')
        select_query += ['FROM', self._collection._name]

        if where_clause is not None:
            select_query += ['WHERE', where_clause]

        if self._limit and not self._query_tree._selectors:
            select_query += ['LIMIT', str(self._limit)]

        return ' '.join(select_query)

    def execute(self):
        sql_query = self._sql_query()
        result = self._collection._db._connection.execute(sql_query).fetchall()
        if (self._projection_is_index_only() and self._query_tree.is_empty()):
            # If the selection and the projection have been applied on
            # indexed fields only, we have to rebuild a dictionary from
            # the value of each returned row.
            if result and self._limit == 1:
                new_document = {}
                for field, value in zip(self._include_fields, result[0]):
                    _nested_set(new_document, field, value)
                return new_document

            documents = []
            for row in result:
                new_document = {}
                for field, value in zip(self._include_fields, row):
                    _nested_set(new_document, field, value)
                documents.append(new_document)
            return documents

        partially_matching_documents = (json.loads(row[0]) for row in result)
        if self._limit == 1:
            matching_document = self.match_one(partially_matching_documents)
            return self._skim(matching_document)
        else:
            matching_documents = self.match_many(partially_matching_documents)
            return [self._skim(document) for document in matching_documents]


class Collection:
    __slots__ = (
        '_db', '_formated_indexed_fields', '_indexed_fields',
        '_indexes', '_name', '_registered'
    )

    INDEX_TYPES = {
        str: 'TEXT',
        int: 'INTEGER',
        float: 'REAL',
    }

    def __init__(self, db, name: str, **kwargs) -> None:
        self._db = db
        self._name = name
        self._registered = kwargs.get('registered', False)
        self._indexes = kwargs.get('indexes', {})
        self._indexes.setdefault('indexes', [])
        self._indexed_fields = self._indexes.setdefault(
            'indexed_fields', []
        )
        self._formated_indexed_fields = self._indexes.setdefault(
            'formated_indexed_fields', []
        )

    @transactional
    def _register(self):
        if self._registered:
            return

        create_collection_query = (
            'CREATE TABLE {} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
            '_data BLOB NOT NULL);'
        ).format(self._name)
        self._db._connection.execute(create_collection_query)

        self._db._connection.execute(
            'INSERT INTO plume_master(collection_name)'
            ' VALUES (?)', (self._name,)
        )

        self._db._collections[self._name] = self
        self._registered = True

    def _prepare_index_keys(self, keys: list) -> list:
        """
            Returns a valid list of index keys.

            An index key is a list of 3 values that describe an index
            on a field. These 3 values are the field name, the field type,
            and the index order.
        """
        # Allows the user to use a shortcut for single field index
        # by providing only a tuple of keys.
        if isinstance(keys, tuple):
            keys = [keys]

        # Replace index type with a SQLite compatible type, and
        # add a default index direction (ASCENDING) if not indicated.
        index_keys = [list(key) for key in keys]
        for key in index_keys:
            key[1] = self.INDEX_TYPES[key[1]]
            if len(key) == 2:
                key.append(ASCENDING)

        return index_keys

    def _prepare_index_columns(self, index_keys: list) -> list:
        """Create a new column for non-indexed fields."""
        indexed_fields = self._indexes['indexed_fields']
        keys_to_index = [
            key for key in index_keys
            if key[0] not in indexed_fields
        ]
        create_column_query = ''.join(
            ('ALTER TABLE ', self._name, ' ADD COLUMN "{}" {}')
        )
        for field_name, _type, order in keys_to_index:
            query = create_column_query.format(field_name, _type)
            self._db._connection.execute(query)

        fields_to_index = [key[0] for key in keys_to_index]
        return fields_to_index

    def _feed_index(self, indexed_fields: list) -> None:
        row_number = self._db._connection.execute(
            'SELECT count(id) FROM ' + self._name
        ).fetchone()[0]

        if row_number == 0:
            return

        formated_indexed_fields = [
            '"' + field + '"' for field in indexed_fields
        ]
        select_query = 'SELECT id, _data FROM ' + self._name
        csv_fields = ' = ?, '.join(formated_indexed_fields)
        csv_fields += ' = ?'
        update_query = (
            'UPDATE ' + self._name + ' SET ' + csv_fields + ' WHERE id = ?'
        )
        cursor = self._db._connection.execute(select_query)
        for _id, document in cursor.fetchall():
            document = json.loads(document)
            index_values = [
                _nested_get(document, field) for field in indexed_fields
            ]
            index_values.append(_id)
            self._db._connection.execute(update_query, index_values)

    @transactional
    def create_index(self, keys: list, **kwargs) -> None:
        if not self._registered:
            self._register()

        index_keys = self._prepare_index_keys(keys)

        # Check if the index already exists (all key are equivalent)
        for index in self._indexes['indexes']:
            if index['keys'] == index_keys:
                return

        # Create a new column for non-indexed fields
        new_indexed_fields = self._prepare_index_columns(index_keys)

        # Copy data from document to new index column
        self._feed_index(new_indexed_fields)

        # Create the index
        index_name = kwargs.get('name')
        indexed_fields = [key[0] for key in index_keys]
        if index_name is None:
            index_name = '_'.join((self._name, 'index', *indexed_fields))

        formated_indexed_fields = [
            '"' + field + '"' for field in indexed_fields
        ]
        csv_fields = ', '.join(formated_indexed_fields)
        create_index = 'CREATE INDEX IF NOT EXISTS "{}" ON {}({})'.format(
            index_name,
            self._name,
            csv_fields
        )
        self._db._connection.execute(create_index)

        # Register the new index in the master table.
        index = {
            'name': index_name,
            'keys': index_keys,
        }
        self._indexes['indexes'].append(index)
        self._indexes['indexed_fields'] += new_indexed_fields
        self._indexed_fields = self._indexes['indexed_fields']
        new_formated_indexed_fields = [
            '"' + field + '"' for field in new_indexed_fields
        ]
        self._indexes['formated_indexed_fields'] += new_formated_indexed_fields
        self._formated_indexed_fields = (
            self._indexes['formated_indexed_fields']
        )

        json_indexes = json.dumps(self._indexes)
        update_master = (
            'UPDATE plume_master SET indexes = ? '
            'WHERE collection_name = "{}"'
        ).format(self._name)
        self._db._connection.execute(update_master, [json_indexes])

    def find(self, query: dict, projection: dict=None,
             limit: int=None) -> list:
        if not self._registered:
            self._register()

        select_query = SelectQuery(
            self, self._indexed_fields, query, projection, limit
        )
        return select_query.execute()

    def find_one(self, query: dict, projection: dict=None):
        if not self._registered:
            self._register()

        select_query = SelectQuery(
            self, self._indexed_fields, query, projection, 1
        )
        return select_query.execute()

    @transactional
    def insert_one(self, document: dict) -> None:
        if not self._registered:
            self._register()

        fields = ['_data']
        values = [json.dumps(document)]
        placeholders = ['?']

        if self._indexed_fields:
            fields += self._formated_indexed_fields
            values += (
                _nested_get(document, field)
                for field in self._indexed_fields
            )
            placeholders += (len(self._formated_indexed_fields) * ['?'])

        insert_one_query = 'INSERT INTO {}({}) VALUES ({})'.format(
            self._name,
            ', '.join(fields),
            ', '.join(placeholders)
        )
        self._db._connection.execute(insert_one_query, values)

    @transactional
    def insert_many(self, documents: list) -> None:
        if not self._registered:
            self._register()

        fields = ['_data']
        placeholders = ['?']
        if not self._indexed_fields:
            rows = [[json.dumps(document)] for document in documents]
        else:
            fields += self._formated_indexed_fields
            placeholders += (len(self._formated_indexed_fields) * ['?'])
            rows = []
            for document in documents:
                row = [json.dumps(document)]
                row += (
                    _nested_get(document, field)
                    for field in self._indexed_fields
                )
                rows.append(row)

        insert_query = 'INSERT INTO {}({}) VALUES ({})'.format(
            self._name,
            ', '.join(fields),
            ', '.join(placeholders)
        )

        self._db._connection.executemany(insert_query, rows)


class Database:
    __slots__ = ('_collections', '_connection', '_name',)

    def __init__(self, name: str) -> None:
        self._name = name
        self._collections = {}
        self._connection = sqlite3.connect(self._name, isolation_level=None)

        with Transaction(self._connection):
            self._connection.execute(
                'CREATE TABLE IF NOT EXISTS plume_master ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,'
                'collection_name TEXT UNIQUE NOT NULL,'
                'indexes TEXT DEFAULT "{}")'
            )

        collections = self._connection.execute(
            'SELECT collection_name, indexes FROM plume_master;'
        ).fetchall()

        for collection_name, indexes in collections:
            collection = Collection(
                self,
                collection_name,
                indexes=indexes,
                registered=True,
            )
            self._collections[collection_name] = collection

    def __getattr__(self, collection_name: str) -> Collection:
        try:
            return self._collections[collection_name]
        except KeyError:
            return Collection(self, collection_name)

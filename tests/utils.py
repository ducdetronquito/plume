def collection_is_registered(db, collection_name):
    query = (
        "SELECT name FROM sqlite_master WHERE "
        "type='table' AND name='{}';"
    ).format(collection_name)

    result = db._connection.execute(query).fetchone()
    return result

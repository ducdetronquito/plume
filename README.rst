Plume
=====

SQLite3 as a document database with a MongoDB API 🚀


⚠️ Plume is currently under development: use it with care ⚠️


Done:
-----

- Database creation with **PlumeDB('test.db')**
- Lazy collection creation
- Document insertion with **Collection.insert**, **Collection.insert_many**
- Document lookup with **Collection.find**
    - Follows the MongoDB query API
    - Support comparison operator: **$eq**, **$gt**, **$gte**, **$lt**, **$lte**, **$ne**
    - Support logical operator: **$and**, **$or** 
- Index creation with **Collection.create_index**
- Query and index support for nested fields


To do:
------

- Add **Collection.find_one**
- Add **Collection.replace_one**
- Add **Collection.update_one**
- Add **Collection.update_many**
- Add **Collection.delete_one**
- Add **Collection.delete_many**
- Add **Collection.drop_index**
- Add **Collection.drop**
- Support query projection
- Support transaction

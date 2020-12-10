from os.path import join
import pymongo
from bson.json_util import dumps


def backup_db(backup_db_dir):
    client = pymongo.MongoClient('127.0.0.1',authSource='lab13db',authMechanism='SCRAM-SHA-1')
    database = client['lab13db']
    collections = database.collection_names()

    for i, collection_name in enumerate(collections):
        col = getattr(database, collections[i])
        collection = col.find()
        jsonpath = collection_name + ".json"
        jsonpath = join(backup_db_dir, jsonpath)
        with open(jsonpath, 'wb') as jsonfile:
            jsonfile.write(dumps(collection).encode())
    print("BackUp Finish")


backup_db('.')

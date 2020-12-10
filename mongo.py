from typing import List
from datetime import datetime
import paho.mqtt.client as mqtt
import pymongo
import pymongo.database
import pymongo.collection
import pymongo.errors
import threading
import os


MONGO_URI = "mongodb://admin:12345678@127.0.0.1/?authSource=lab13db&authMechanism=SCRAM-SHA-256"
MONGO_DB = "lab13db"
MONGO_COLLECTION = "mqtt1"
MONGO_PORT = 2277
MONGO_TIMEOUT = 1  # Time in seconds
MONGO_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"


class Mongo(object):
    def __init__(self):
        self.client = None
        self.database = None
        self.collection = None
        self.queue = list()

    def connect(self):
        print("Connecting Mongo")
        self.client = pymongo.MongoClient('127.0.0.1',authSource='lab13db',authMechanism='SCRAM-SHA-1' )
        #self.client = pymongo.MongoClient(
        #    MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT*1000.0)
        self.database = self.client.get_database(MONGO_DB)
        self.collection = self.database.get_collection(MONGO_COLLECTION)

    def disconnect(self):
        print("Disconnecting Mongo")
        if self.client:
            self.client.close()
            self.client = None

    def connected(self):
        if not self.client:
            print("11111")
            return False
        try:
            self.client.admin.command("ismaster")
            print("2222")
        # except pymongo.errors.PyMongoError:
        except pymongo.errors.ConnectionFailure:
            print("33333")
            return False
        else:
            print("4444")
            return True

    def _enqueue(self, msg):
        print("Enqueuing")
        self.queue.append(msg)
        # TODO process queue

    def __store_thread_f(self, msg):
        print("Storing")
        now = datetime.now()
        try:
            document = {
                "topic": msg.topic,
                "payload": msg.payload.decode(),
                # "retained": msg.retain,
                "qos": msg.qos,
                "timestamp": int(now.timestamp()),
                "datetime": now.strftime(MONGO_DATETIME_FORMAT),
                # TODO datetime must be fetched right when the message is received
                # It will be wrong when a queued message is stored
            }
            result = self.collection.insert_one(document)
            print("Saved in Mongo document ID", result.inserted_id)
            if not result.acknowledged:
                # Enqueue message if it was not saved properly
                self._enqueue(msg)
        except Exception as ex:
            print(ex)

    def _store(self, msg):
        self.__store_thread_f(msg)
        # th = threading.Thread(target=self.__store_thread_f, args=(msg,))
        # th.daemon = True
        # th.start()

    def save(self, msg):
        print("Saving")
        if msg.retain:
            print("Skipping retained message")
            return
        if self.connected():
            self._store(msg)
        else:
            self._enqueue(msg)

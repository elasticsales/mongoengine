import sys

sys.path[0:0] = [""]
import datetime
import unittest

import pymongo
from bson.tz_util import utc

import mongoengine.connection
from mongoengine import *
from mongoengine.connection import ConnectionError, get_connection, get_db


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_connect(self):
        """Ensure that the connect() method works properly.
        """
        connect("mongoenginetest")

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, "mongoenginetest")

        connect("mongoenginetest2", alias="testdb")
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb', 'mongoenginetest2')

        self.assertRaises(ConnectionError, get_connection)
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db('testdb')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest2')

    def test_connection_kwargs(self):
        """Ensure that connection kwargs get passed to pymongo.
        """
        connect('mongoenginetest', alias='t1', tz_aware=True)
        conn = get_connection('t1')
        self.assertTrue(conn.codec_options.tz_aware)

        connect('mongoenginetest2', alias='t2')
        conn = get_connection('t2')
        self.assertFalse(conn.codec_options.tz_aware)

    def test_datetime(self):
        connect('mongoenginetest', tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        DateDoc.drop_collection()
        DateDoc(the_date=d).save()

        date_doc = DateDoc.objects.first()
        self.assertEqual(d, date_doc.the_date)


if __name__ == '__main__':
    unittest.main()

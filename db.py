
import pymongo, json
from pymongo import MongoClient


cluster = MongoClient("mongodb+srv://ahmedyass:webmining@cluster0-elggs.mongodb.net/test?retryWrites=true&w=majority")
database = cluster["music"]
collection = database["users"]

with open('users.json') as json_file:
    data = json.load(json_file)
    collection.insert_many(data['users'])


'''
mongo = pymongo.MongoClient("mongodb+srv://ahmedyass:webminig@cluster0-elggs.mongodb.net/test")
db = client.test

db = pymongo.database.Database(mongo, 'music')
col = pymongo.collection.Collection(db, 'music')
'''
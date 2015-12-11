from pymongo import MongoClient
import pymongo

client = MongoClient()
db = client['assig-test']

# 3

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.ASCENDING).limit(1):
        print res.get("timestamp")

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.DESCENDING).limit(1):
        print res.get("timestamp")


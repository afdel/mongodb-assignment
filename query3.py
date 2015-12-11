from pymongo import MongoClient
import pymongo

client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']

# 3

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.ASCENDING).limit(1):
        print "Earliest message was published at : ",res.get("timestamp")

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.DESCENDING).limit(1):
        print "Latest message was published at : ",res.get("timestamp")


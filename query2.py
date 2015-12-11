from __future__ import division
import csv
import datetime
from pymongo import MongoClient
import pymongo
from bson.son import SON
from bson.code import Code


client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']

# 2

pipeline = [
	{"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
	{"$sort": SON([("count", -1)])}
	]

i = 0
messages_count = 0
messages10_count = 0
for count in list(db.microblog.aggregate(pipeline)):
	if count.get("_id") > 0 and i<10:
		messages10_count = messages10_count + count.get("count")
		i += 1
	messages_count = messages_count + count.get("count")

percentage10mes = 100*messages10_count/messages_count
print "Percentage of TOP 10 users number of messages is : ", percentage10mes



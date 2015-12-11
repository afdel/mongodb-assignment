from __future__ import division
import datetime
from pymongo import MongoClient
import pymongo

client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']

#header = ["id","id_member","timestamp","text","geo_lat","geo_lng"]

#4
totalDiff = datetime.timedelta(minutes=0)


for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.ASCENDING).limit(1):
	previous = res.get("timestamp")

count = 1 

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.ASCENDING).skip(1):
	current = res.get("timestamp")
	diff = current - previous
	totalDiff = totalDiff + diff
	previous = current
	count += 1

print "The Total Time Delta Between All Messages ",totalDiff
diffSec = totalDiff.total_seconds()	
print "In Seconds",diffSec
mean = diffSec/count
print "Count Of Messages: ",count
print "The mean time delta between all messages : ",mean

from __future__ import division
from pymongo import MongoClient
import pymongo
from bson.code import Code


client = MongoClient()
db = client['assig-test']

# 8

#pipeline8 = [
#        {"$group": {"_id": { "lat": "$geo_lat", "long": "$geo_lng" }, "count": {"$sum": 1 } } },
#        {"$sort": SON([("count", -1)])}
#        ]

#{ "$match":     {"$and" : [ { "geo_lat" : {'$gte': 50, '$lte': 55 } } , { "geo_lng" : {'$gte': 0 , '$lte': 4 } } ] } },

pipeline8 = [
		{ "$group": { "_id": { "lat": "$geo_lat", "long": "$geo_lng" }, "count": { "$sum": 1 } } }, 
		{ "$sort": { "count": -1 } }, { "$limit": 1 } 
        ]

command = db.command('aggregate', 'microblog', pipeline=pipeline8, allowDiskUse=True)

resultArray = command.get("result")

result = resultArray[0]

coord = result.get("_id")

latitude = coord.get("lat")

longitude = coord.get("long")

coordMsgCount = result.get("count")

print " The Region in the UK with the highest number of messages is : Latitude : ",latitude," Longitude : ",longitude
print " Number of messages is : ",coordMsgCount

#for count8 in list(db.microblog.aggregate(pipeline8)):
#	print count8

from __future__ import division
import csv
import datetime
from pymongo import MongoClient
import pymongo
from bson.son import SON
from bson.code import Code


#csvfile = open('../microblogDataset_COMP6235_CW2.csv','r')
#reader = csv.DictReader( csvfile )


client = MongoClient()
db = client['assignment-test']

#header = ["id","id_member","timestamp","text","geo_lat","geo_lng"]

#1

users_count = 0
unique_users = db.microblog.find().distinct("id_member")

for user in unique_users :
	users_count += 1

print " Users Count : ",users_count

# 2

pipeline = [
	{"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
	{"$sort": SON([("count", -1)])}
	]

i = 0
messages_count = 0
messages10_count = 0
for count in list(db.microblog.aggregate(pipeline)):
	if i < 10:
		messages10_count = messages10_count + count.get("count")
	messages_count = messages_count + count.get("count")
	i += 1

percentage10mes = 100*messages10_count/messages_count
print "Percentage of TOP 10 users number of messages is : ", percentage10mes

# 3

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.ASCENDING).limit(1):
        print res.get("timestamp")

for res in db.microblog.find({},{"timestamp":1}).sort('timestamp', pymongo.DESCENDING).limit(1):
        print res.get("timestamp")

# 5

#pipe = [{'$group': {'_id': None, 'avgLength': {'$avg': '$text.length'}, 'count': { '$sum': 1 } }}]
#for average in db.microblog.aggregate(pipeline=pipe):
#	print average


map5 = Code("function () {"
	"			emit('length', this.text.length );	"
	"	}						"
)



reduce5 = Code("function (key, values) {"
               "  var total = 0;"
	       "  var count = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
	       "   count += 1;        "
               "  }"
               "  return total/count;"
               "}")

averageLength = db.microblog.map_reduce(map5, reduce5, "myresults")

for average in averageLength.find():
	print " Mean of messages length",average.get("value")


# 6

map6_1 = Code("function () {"
	"	var text = String(this.text);			"
	"	text.split(/[\s]+/).forEach(function(word) {	"
	"		if (word.length == 1){			"
	"			emit(word, 1);			"
	"		}					"
	"	});						"
	"	}						"
)

reduce6 = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total;"
               "}")



countUnigram = db.microblog.map_reduce(map6_1, reduce6, "myresults6_1")

for unigram in countUnigram.find().sort('value', pymongo.DESCENDING).limit(10):
        print "Unigram : ",unigram.get("_id")
	print "Count : ",unigram.get("value")

map6_2 = Code("function () {"
        "       var text = String(this.text);                   "
        "       text.split(/[\s]+/).forEach(function(word) {   "
        "               if (word.length == 2){                  "
        "                       emit(word, 1);                  "
        "               }                                       "
        "       });                                             "
        "       }                                               "
)


countBigram = db.microblog.map_reduce(map6_2, reduce6, "myresults6_2")

for bigram in countBigram.find().sort('value', pymongo.DESCENDING).limit(10):
        print "Bigram : ",bigram.get("_id")
	print "Count : ",bigram.get("value")


# 8

#pipeline8 = [
#        {"$group": {"_id": { "lat": "$geo_lat", "long": "$geo_lng" }, "count": {"$sum": 1 } } },
#        {"$sort": SON([("count", -1)])}
#        ]

pipeline8 = [
		{ "$match": 	{"$and" : [ { "geo_lat" : {'$gte': 50, '$lte': 55 } } , { "geo_lng" : {'$gte': 0 , '$lte': 4 } } ] } },
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

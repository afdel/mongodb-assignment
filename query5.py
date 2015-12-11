from __future__ import division
from pymongo import MongoClient
import pymongo
from bson.code import Code


client = MongoClient()
db = client['assig-test']


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



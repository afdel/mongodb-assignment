from __future__ import division
from pymongo import MongoClient
import pymongo
from bson.code import Code


client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']


# 5


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



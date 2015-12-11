from __future__ import division
from pymongo import MongoClient
import pymongo
from bson.son import SON
from bson.code import Code

client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']

# 6

map6_1 = Code("function () {"
        "       var text = String(this.text);                   "
        "       var parts = text.split(/[\s\t]+/);"
        "       for( var i = 0; i<parts.length; i++) {		"
	"		var unigram = parts[i];			"
	"		emit(unigram, 1);			"
	"	}						"
        "       }                                               "
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
        "       var parts = text.split(/[\s\t]+/);"
        "       for( var i = 1; i<parts.length; i++) {		"
	"		var bigram = parts[i-1].concat(" ").concat(parts[i]);	"
	"		emit(bigram, 1);			"
	"	}						"
        "       }                                               "
)


countBigram = db.microblog.map_reduce(map6_2, reduce6, "myresults6_2")

for bigram in countBigram.find().sort('value', pymongo.DESCENDING).limit(10):
        print "Bigram : ",bigram.get("_id")
	print "Count : ",bigram.get("value")

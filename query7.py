from __future__ import division
from pymongo import MongoClient
import pymongo
from bson.code import Code


client = MongoClient()
db = client['assig-test']

# 7

map7 = Code("function () {"
        "       	var text = String(this.text);                  "
        "       	emit('hashtags', ( text.match(/\#/g) || []).length );"
        "       }"
)

reduce7 = Code("function (key, values) {"
               "  var total = 0;"
               "  var count = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "   count += 1;        "
               "  }"
               "  return total/count;"
               "}")

averageHashTag = db.microblog.map_reduce(map7, reduce7, "myresults")


for average in averageHashTag.find():
        print " Mean of Hash Tags within a message",average.get("value")


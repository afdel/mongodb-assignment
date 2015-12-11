from pymongo import MongoClient
import pymongo

client = MongoClient('mongodb://localhost:27017/')
db = client['assig-test']

#1

users_count = 0
real_users_count = 0
unique_users = db.microblog.find().distinct("id_member")

for user in unique_users :
	users_count += 1
	if user > 0:
		real_users_count += 1

print " Users Count : ",users_count
print " Users Count Ignoring Users with Negatice IDs : ",real_users_count

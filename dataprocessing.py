import csv
from pymongo import MongoClient


csvfile = open('../microblogDataset_COMP6235_CW2.csv','r')
reader = csv.DictReader( csvfile )


client = MongoClient()
db = client.assigmentpython

header = ["id","id_member","timestamp","text","geo_lat","geo_lng"]

for each in reader:
	row={}
	for field in header:
		row[field]=each[field]
	print row
	db.microblogs.insert(row)

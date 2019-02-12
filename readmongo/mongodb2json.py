######################################################
# Read data from mongoDB and save it as an json file
######################################################
# Connect to mongoDB:
from pymongo import MongoClient
import json

conn = MongoClient('127.0.0.1', 27017)
db = conn.mydb
my_set = db.test_set

jsonobj = json.loads(str(list(my_set.find({},{"_id":0}).limit(1000))).replace("'",'"'))

with open ("../data/data.json","w") as fout:
    json.dump(jsonobj,fout)



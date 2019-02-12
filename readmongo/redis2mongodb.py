####################################
# Read data from redis
# Format it and save into mongoDB
####################################

import sys

class formatredis:
    dictstr = dict();

    def __init__(self,indictstr):
        self.dictstr = indictstr;

    def depfunc(self,indictstr):
        return [];

    def titfunc(self,indictstr):
        return "";

    def sumfunc(self,indictstr): 
        return "";

    def idfunc (self,indictstr): 
        return "";

    def formatstring(self):
        outdict = dict();
        outdict["id"]               = self.idfunc (self.dictstr);
        outdict["summary"]          = self.sumfunc(self.dictstr);
        outdict["title"]            = self.titfunc(self.dictstr);
        outdict["dependencies"]     = self.depfunc(self.dictstr);
        return outdict

class formatlifadian(formatredis):
    def depfunc(self,dicstr):
        try:
            strout = "lat:"+dicstr["location,lat".encode()].decode().split(".")[0] + ",lng:" + dicstr["location,lng".encode()].decode().split(".")[0]
            return [{"source":strout.replace(":","--").replace(",","-").replace("(","-").replace(")","-")}]
        except Exception as err:
            return [];

    def idfunc(self,dicstr):
        try:
            outstr =  "lat:"+dicstr["location,lat".encode()].decode() + ",lng:" + dicstr["location,lng".encode()].decode()
            return outstr.replace(":","--").replace(",","-").replace("(","-").replace(")","-").replace(".","--")
        except Exception as err:
            try:
                return dicstr["key"].decode()
            except Exception as err:
                print ("Warning, id is none ...")
                return "";

    def sumfunc(self,dicstr):
        try:
            return dicstr["category".encode()].decode() + "\n,address:" + dicstr["address".encode()].decode().replace(":","--").replace(",","-")
        except Exception as err:
            return ""

    def titfunc(self,dicstr):
        try:
            namelist =  dicstr["key"].decode().split(":") 
            return namelist[len(namelist)-1].replace(":","--").replace(",","-")
        except Exception as err:
            return ""

sys.path.append("/home/cuichao/GitRepos/JZXN-C-DATA")
from RedisLib import redis_cli 
# Build an connection to redis
connectpar = {
        "host":'127.0.0.1',
        "port":6379,
        "db":0
        }
red = redis_cli.hashRedis(connectpar)
pathbase = "SmallCompany:Lifadian:basic:20190117:"

# Connect to mongoDB:
from pymongo import MongoClient

conn = MongoClient('127.0.0.1', 27017)
db = conn.mydb
my_set = db.test_set

allkeys = red.r.keys(pathbase+"*")

for key in allkeys:
    print (key.decode())
    dictstr = red.r.hgetall(key)
    dictstr["key"] = key
    
    formatstr = formatlifadian(dictstr) 
    result = formatstr.formatstring()
    if len( (list( my_set.find({"id":result["id"]})))) == 0:
        my_set.insert(result)
    else:
        print ("Data exists ...")
    
    for i in range(len(result["dependencies"])):
        if len( (list( my_set.find({"id":result["dependencies"][i]["source"]})))) == 0:
            print ("Adding Parents ...")
            dictin = {};
            dictin["key"] = result["dependencies"][i]["source"].encode()
            formatstrtmp = formatlifadian(dictin) 
            result1 = formatstrtmp.formatstring()
            my_set.insert(result1)

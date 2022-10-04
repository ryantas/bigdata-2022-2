
import redis
import json
from bson.json_util import dumps
from pymongo import MongoClient
# client = MongoClient('mongodb://localhost:27017/')
client = MongoClient('mongodb://0.0.0.0:27017/')
# redisClient = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
redisClient = redis.StrictRedis(host='0.0.0.0', port=6379, db=0)

database = client['myMongoDb'] #database name in mongodb,  Important: In MongoDB, a database is not created until it gets content!
mycol = database['Users']
# x = database['Users'].insert_one({"name": "rick"})
x = mycol.insert_many([{"name": "rick"}, {"name": "carlos"}])

userList = mycol.find() #collection name in 'DummyDb' database
serializedObj = dumps(userList) #serialize object for the set redis.
result = redisClient.set('users', serializedObj) #set serialized object to redis server.
#you can check the users in redis using redis gui or type 'get users' to redis client or just get it from the redis like below.
parsedUserList = json.loads(redisClient.get('users'))


for user in parsedUserList: #check the names
	# print(user["name"]) #'username' one of the field of the 'Users' collection
	print(user)
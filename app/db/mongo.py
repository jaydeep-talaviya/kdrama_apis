from pymongo import MongoClient

MONGO_DATABASE_URL = "mongodb://localhost:27017"
client = MongoClient(MONGO_DATABASE_URL)
mongo_db = client.kcinemadb  # Replace 'mydatabase' with your database name
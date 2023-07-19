
# import urllib.parse
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import pandas as pd
import json 


uri = "mongodb+srv://ayushsingh403:Ayush@cluster0.bwumrek.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
# parsed_uri = urllib.parse.quote_plus(uri)
client = MongoClient(uri, server_api=ServerApi('1'))

# create database name and collection name
DATABASE_NAME="SensorData"
COLLECTION_NAME="waferfault"



# Read daa as dataframe
df = pd.read_csv(r"C:\\Users\\ACER\\Downloads\\MLproject-main\\MLproject-main\\sensor-fault-detection\\notebooks\\wafer_23012020_041211.csv")
df = df.drop("Unnamed: 0", axis =1)
#df = pd.read_csv("C:\Users\ACER\Downloads\MLproject-main\MLproject-main\sensor-fault-detection\notebooks\wafer_23012020_041211.csv")
json_record= list(json.loads(df.T.to_json()).values())

# dump data into database
client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
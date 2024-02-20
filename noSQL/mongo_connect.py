from pymongo import MongoClient

user = 'root'
password = 'toor'  # Empty string for password
host = 'localhost'

# If no password is set, the connection URL should still include it
connecturl = "mongodb://{}:{}@{}/".format(user, password, host)
print("Connecting to MongoDB server")
connection = MongoClient(connecturl)

# Fetch databases
databases = connection.list_database_names()

# select database
db = connection.training

#create a collection

# create a sample document
doc = {"lab": "Accessing mongodb using python", "Subject": "NoSQL databases"}

print("Inserting document into collection")
db.collection.insert_one(doc)

# Fetch all from training database and python collection
print("Fetching all documents from collection")


doc1 = {"database":"a database contains collections"}
doc2 = {"collection":"a collection stores the documents"}
doc3 = {"document":"a document contains the data in the form or key value pairs."}

db.collection.insert_many([doc1, doc2, doc3])
documents = db.collection.find()

for doc in documents:
    print(doc)

connection.close()



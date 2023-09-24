import pymongo

# Create a MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select a database (replace 'mydatabase' with your database name)
db = client["mongodb"]

# Select a collection (replace 'mycollection' with the collection name)
collection = db["absent"]

# Query the collection to retrieve documents
documents = collection.find()

# Print the documents
for document in documents:
    print(document)

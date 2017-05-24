# 
# Spark to CosmosDB_Sample.py
#


# Import Necessary Libraries
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

# Configuring the connection policy (allowing for endpoint discovery)
# 	Configure your PreferredLocations, example:
# 	connectionPolicy.PreferredLocations = ["East US 2", "West US", "Southeast Asia", "Western Europe","Canada Central"]
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = [Enter Your PreferredLocations Here]

# Set keys to connect to Cosmos DB 
#	Enter your masterKey, endpoint here
masterKey = '[Enter your masterKey here]' 
host = '[Enter your endpoint here]'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

# Configure Database and Collections
#	Enter your database and collection here
databaseId = '[Enter your database here]'
collectionId = '[Enter your collection here]'

# Configurations the Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
#	Enter your SQL query here
querystr = "[Enter your SQL query here]"

# Query documents
query = client.QueryDocuments(collLink, querystr, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# Convert to DataFrame
starttime = datetime.datetime.now()
df = spark.createDataFrame(list(query))
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()


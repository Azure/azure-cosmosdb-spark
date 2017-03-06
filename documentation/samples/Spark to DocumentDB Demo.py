# 
# Spark to DocumentDB Demo (py)
#    |-- Refer to Spark to DocumentDB Demo.scala 
#


# Import Necessary Libraries
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

# Configuring the connection policy (allowing for endpoint discovery)
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["East US 2", "West US", "Southeast Asia", "Western Europe","Canada Central"]

# Set keys to connect to DocumentDB 
masterKey = 'le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==' 
host = 'https://doctorwho.documents.azure.com:443/'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

# Configure Database and Collections
databaseId = 'DepartureDelays'
collectionId = 'flights_pcoll'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId



# Set query parameter
#querystr = "SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c"
querystr = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"

# Query documents
query = client.QueryDocuments(collLink, querystr, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# Convert to DataFrame
starttime = datetime.datetime.now()
df = spark.createDataFrame(list(query))
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()


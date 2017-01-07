##
## Spark to DocumentDB 
##
##   Test using pyDocumentDB
##
## Prerequisites:
##  1. !pip install pydocumentdb
##  
## Referecnes
##	- Refer to https://notebooks.azure.com/n/J9dSAARelKo/notebooks/python-documentdb.ipynb
##		- Notebook provides connection to DocumentDB via PyDocumentDB
##  - Refer to https://github.com/dennyglee/databricks/blob/b7c4a03ae1e22a6be68d9fe0f745b5a6b64dc8bd/notebooks/Users/denny%40databricks.com/dogfood/AdTech%20Sample%20Notebook%20(Part%201).py
##		- Databricks notebook: https://community.cloud.databricks.com/?o=57901#notebook/3787876137043744
##		- Use above to figure out how to convert Python query back into DataFrame
##		- Initially `The original answer` section of http://stackoverflow.com/questions/33391840/getting-spark-python-and-mongodb-to-work-together
##	- The output of query_iterable is a python iterator
##		*- Use List(); originally was thinking Loop through to create a list 
##		- Convert list into a DataFrame https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html > createDataFrame
##	
## 	This is a good stop gap so we can extract a bunch of data into RDDs (and convert to DataFrame)
##	So we can focus on building it via FiloDB
##

## ssh Tunneling to cluster
##   Reference: https://azure.microsoft.com/en-us/documentation/articles/hdinsight-linux-ambari-ssh-tunnel/
##   Use Firefox to 
##		- connect to Spark UI: http://10.33.0.5:8080/
##		- connect to Zeppelin UI: http://10.33.0.5:8060 (first need to start Zeppelin)
##	 Note, check the inetadr via ifconfig (per the 10.33.0.5 address abvoe)
## ssh -C2qTnNf -D 9876 dennylee@13.88.14.230


## Start pyspark
cd $SPARK_HOME
##./bin/pyspark --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 
./bin/pyspark --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 
./bin/pyspark --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --executor-memory 4G --total-executor-cores 4
./bin/pyspark --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --executor-memory 8G
./bin/pyspark --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --executor-memory 8G --total-executor-cores 8



# Import Necessary Libraries
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

# Configuring the connection policy (allowing for endpoint discovery)
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["West US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]
#connectionPolicy.PreferredLocations = ["Southeast Asia", "Western Europe","Canada Central", "West US", "East US 2"]


# Set keys to connect to DocumentDB 
masterKey = 'le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==' 
host = 'https://doctorwho.documents.azure.com:443/'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)




##
## Airport Codes
##	0:00:00.172908

# Configure Database and Collections
databaseId = 'airports'
collectionId = 'codes'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.City FROM c WHERE c.State='WA'"


# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

starttime = datetime.datetime.now()
for row in query:
  print row

endtime = datetime.datetime.now()
print endtime - starttime



##
## Airport Codes
##	0:00:00.225645, 0:00:00.006784

# Configure Database and Collections
databaseId = 'airports'
collectionId = 'codes'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.City FROM c WHERE c.State='WA'"


# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

# Push into list `elements`
starttime = datetime.datetime.now()
elements = list(query)
endtime = datetime.datetime.now()
print endtime - starttime



#
# Convert to DataFrame
#	https://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html
# 0:00:00.025026
starttime = datetime.datetime.now()
df = spark.createDataFrame(elements)
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()





##
## Flight Data (Top 100)
##	0:00:00.062662
##	0:00:00.120453

# Configure Database and Collections
databaseId = 'DepartureDelays'
collectionId = 'flights'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c"


# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

starttime = datetime.datetime.now()

# create elements list
elements = []
for row in query:
  elements.append(row)
  #print row

endtime = datetime.datetime.now()
print endtime - starttime




##
## Flight Data (Top 100)
##	0:00:00.214985, 0:00:00.009669
## 0:00:00.774820, 0:00:00.508290

# Configure Database and Collections
databaseId = 'DepartureDelays'
#collectionId = 'flights'
collectionId = 'flights_pcoll'


# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c"


# Query documents
#query = client.QueryDocuments(collLink, query, options=None, partition_key=None)
# Query documents
query = client.QueryDocuments(collLink, query, options= { 'enableCrossPartitionQuery': True }, partition_key=None)


# create elements list
starttime = datetime.datetime.now()
elements = list(query)
endtime = datetime.datetime.now()
print endtime - starttime

#
# Convert to DataFrame
#	https://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html
# 0:00:00.045732,  0:00:00.045043
starttime = datetime.datetime.now()
df = spark.createDataFrame(elements)
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()




##
## Flight Data (Top 100)
##	Merged together DataFrame and list
##	0:00:00.010310

# Configure Database and Collections
databaseId = 'DepartureDelays'
collectionId = 'flights'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c"


# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

#
# Convert to DataFrame
#	https://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html
#
starttime = datetime.datetime.now()
df = spark.createDataFrame(list(query))
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()





##
## Flight Data (origin == SEA, 14808 rows**)
##	0:00:02.186529
##	0:00:01.752655
##


# Configure Database and Collections
databaseId = 'DepartureDelays'
collectionId = 'flights'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
#query = "SELECT c.date FROM c WHERE c.origin = 'SEA'"

# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

starttime = datetime.datetime.now()

# create elements list
elements = []
for row in query:
  elements.append(row)
  #print row

endtime = datetime.datetime.now()
print endtime - starttime



#
## Flight Data (origin == SEA, 14808 rows)
##	0:00:01.498699, 0:00:01.323917 (14,808 of 1.05M)
##  0:00:05.146107, 0:00:03.234670 (23,078 of 1.39M)
#

# Configure Database and Collections
databaseId = 'DepartureDelays'
#collectionId = 'flights'
collectionId = 'flights_pcoll'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
#query = "SELECT c.date FROM c WHERE c.origin = 'SEA'"

# Query documents
#query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

# Query documents
query = client.QueryDocuments(collLink, query, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# create elements list
starttime = datetime.datetime.now()
elements = list(query)
endtime = datetime.datetime.now()
print endtime - starttime


#
# Convert to DataFrame
#	https://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html
# 0:00:00.740898, 0:00:00.772207
starttime = datetime.datetime.now()
df = spark.createDataFrame(elements)
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()




#
## Flight Data (origin == SEA, 14808 rows)
##	0:00:01.389337
#

# Configure Database and Collections
databaseId = 'DepartureDelays'
collectionId = 'flights'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"

#
# Convert to DataFrame
#	https://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html
#
starttime = datetime.datetime.now()
df = spark.createDataFrame(list(query))
endtime = datetime.datetime.now()
print endtime - starttime

# Show data
df.show()




#
## Flight Data (all data)
##	pColl: 0:02:36.335267; 1,391,578
##  sColl: 0:01:37.518344; 1,048,575 
#
# Actions:
#	Need to figure out how to enable cross-partiiton query in python SDK
#	Reference: https://github.com/Azure/azure-documentdb-python/blob/master/test/crud_tests.py
#



# Configure Database and Collections
databaseId = 'DepartureDelays'
#collectionId = 'flights_pcoll'
collectionId = 'flights'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c"
#query = "SELECT c.date FROM c WHERE c.origin = 'SEA'"

# Query documents
query = client.QueryDocuments(collLink, query, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

starttime = datetime.datetime.now()

# create elements list
elements = list(query)

endtime = datetime.datetime.now()
print endtime - starttime
print len(elements)






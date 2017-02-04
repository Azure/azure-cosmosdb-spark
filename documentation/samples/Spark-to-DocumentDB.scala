//
// Spark to DocumentDB 
//
//   Test using pyDocumentDB
//
// Prerequisites:
//  1. !pip install pydocumentdb
//  
// Referecnes
//	- Refer to https://notebooks.azure.com/n/J9dSAARelKo/notebooks/python-documentdb.ipynb
//		- Notebook provides connection to DocumentDB via PyDocumentDB
//  - Refer to https://github.com/dennyglee/databricks/blob/b7c4a03ae1e22a6be68d9fe0f745b5a6b64dc8bd/notebooks/Users/denny%40databricks.com/dogfood/AdTech%20Sample%20Notebook%20(Part%201).py
//		- Databricks notebook: https://community.cloud.databricks.com/?o=57901#notebook/3787876137043744
//		- Use above to figure out how to convert Python query back into DataFrame
//		- Initially `The original answer` section of http://stackoverflow.com/questions/33391840/getting-spark-python-and-mongodb-to-work-together
//	- The output of query_iterable is a python iterator
//		*- Use List(); originally was thinking Loop through to create a list 
//		- Convert list into a DataFrame https://spark.apache.org/docs/1.6.2/api/python/spark-shell.sql.html > createDataFrame
//	
// 	This is a good stop gap so we can extract a bunch of data into RDDs (and convert to DataFrame)
//	So we can focus on building it via FiloDB
//

// ssh Tunneling to cluster
//   Reference: https://azure.microsoft.com/en-us/documentation/articles/hdinsight-linux-ambari-ssh-tunnel/
//   Use Firefox to 
//		- connect to Spark UI: http://10.33.0.5:8080/
//		- connect to Zeppelin UI: http://10.33.0.5:8060 (first need to start Zeppelin)
//	 Note, check the inetadr via ifconfig (per the 10.33.0.5 address abvoe)
// ssh -C2qTnNf -D 9876 dennylee@13.88.14.230

// Vivace
ssh -C2qTnNf -D 9876 dennylee@13.88.14.230

// Kintaro
ssh -C2qTnNf -D 9876 dennylee@13.64.77.252 




// Start spark-shell
cd $SPARK_HOME
//./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 

// Vivace
./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 
./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --jars /home/dennylee/spark/jars/azure-documentdb-1.9.4-jar-with-dependencies.jar --executor-memory 4G --total-executor-cores 4

// Kintaro
./bin/spark-shell --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --executor-memory 8G
./bin/spark-shell --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --jars /home/dennylee/spark/jars/azure-documentdb-1.9.4-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8


// Import Necessary Libraries
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.microsoft.azure.documentdb._
import org.joda.time._
import org.joda.time.format._


// Make connection to DocumentDB
val docdbClient = new DocumentClient("https://doctorwho.documents.azure.com:443/", "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==", ConnectionPolicy.GetDefault(), ConsistencyLevel.Session)
//var pcollLink = "dbs/DepartureDelays/colls/flights"
var pcollLink = "dbs/DepartureDelays/colls/flights_pcoll"


// Queries
//var query = "SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
//var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c "


// Enable Cross Partition Queries
var feedOptions = new FeedOptions()
feedOptions.setEnableCrossPartitionQuery(true)
feedOptions.setMaxDegreeOfParallelsim(Integer.MAX_VALUE)
//feedOptions.setMaxDegreeOfParallelsim(-1)


// Java / Scala Iterator
import java.util.{Iterator => JIterator}
def scalaIterator[T](it: JIterator[T]) = new Iterator[T] {  override def hasNext = it.hasNext 
  override def next() = it.next()
}

// Define schema, obtain query iterable(), convert to sequence create RDD 
val schema = StructType(Array(StructField("date", IntegerType), StructField("delay", IntegerType), StructField("distance", IntegerType),StructField("origin", StringType),StructField("destination", StringType)))
var feeds = docdbClient.queryDocuments(pcollLink, query, feedOptions)
var iter = feeds.getQueryIterable()
var seq = scalaIterator(iter.iterator()).map(d => org.apache.spark.sql.RowFactory.create(d.getInt("date"), d.getInt("delay"), d.getInt("distance"),d.getString("origin"),d.getString("destination"))).toSeq
var rdd =  sc.parallelize(seq)


// Execute RDD.count()
//	Forces extraction of data from DocumentDB
val start = new DateTime()
rdd.count()
val end = new DateTime()
val duration = new Duration(start, end)
PeriodFormat.getDefault().print(duration.toPeriod())




// Convert to DataFrame (from RDD)
val start = new DateTime()
var df = spark.createDataFrame(rdd, schema)
df.show()
val end = new DateTime()
val duration = new Duration(start, end)
PeriodFormat.getDefault().print(duration.toPeriod())



//
// Results
//
// Query tests flow A
//	1. Connect to Spark
//	2. Run RDD query - 1st time
//	3. Run RDD query - 2nd time
//	4. Run DF query - 1st time
// 	5. Run DF query - 2nd time
//	6. Disconnect from Spark, repeat for next Queries
//
//
// Queries
//	Q1:  100 rows		SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'
//	Q2:  14808 rows		SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'
// 	Q3:  1391578 rows	SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c 
//
//	Q#a: Q3 with feedOptions.setMaxDegreeOfParallelsim(Integer.MAX_VALUE)
//	Q#b: Q3 with feedOptions.setMaxDegreeOfParallelsim(-1)
//
//
// ** Single Collection **
//		RDD (1st, 2nd)					DF (1st, 2nd)
//	Q1	00:00:01.122	00:00:00.294	00:00:05.879	00:00:00.512
//	Q2	00:00:02.756	00:00:01.583	00:00:05.791	00:00:00.479	
//	Q3	00:01:19.491	00:01:15.981	00:00:06.064	00:00:00.840
//		00:00:05		00:00:04
//
// ** Partitioned Collection **
//	Q1	00:00:01.172	00:00:00.293	00:00:06.093	00:00:00.490
//	Q2	00:00:06.708	00:00:05.008	00:00:05.981	00:00:00.481
//	Q3	00:02:26.632	00:02:52.487	00:00:18.996	00:00:04.788
//		00:00:06		00:00:18		00:00:05		00:00:03
//  Q3a 00:02:23.478 	00:03:08.981
//		00:00:06		00:00:30
//  Q3b 00:02:22.717	
//
// Query tests flow B
//	1. Connect to Spark
//	2. Run RDD query - 1st time
//	3. Run RDD query - 2nd time
//	4. Run DF query - 1st time
// 	5. Run DF query - 2nd time
//	6. Repeat 1-5 for next queries; do NOT disconnect
//
//
// Queries
//	Q1: 100 rows		SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'
//	Q2: 14808 rows		SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'
// 	Q3: 1,fdf,dfd rows	SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c 
//
// ** Single Collection **
//		RDD (1st, 2nd)					DF (1st, 2nd)
//	Q1	00:00:01.185	00:00:00.285	00:00:05.931	00:00:00.527
//	Q2	00:00:01.835	00:00:01.465	00:00:00.413	00:00:00.380	
//	Q3	00:01:19.451	00:01:18.820	00:00:00.771	00:00:00.763
//		00:00:04		00:00:04
//
// ** Partitioned Collection **





/* 
** Spark to DocumentDB Demo
** 
** SparkItUp HDI Cluster connecting to DoctorWho Store
**	HDI Dashboard: https://sparkitup.azurehdinsight.net/
**	YARN cluster: https://sparkitup.azurehdinsight.net/yarnui/hn/cluster
** 
** Reference: https://github.com/Azure/azure-documentdb-spark
**
*/



/*
** 0. Connect to "SparkItUp" Head Node
**   - Use terminal and connect via "SparkItUp"
**	 - Ensure "DoctorWho" [Write region] is [East US 2] (normally its West US)
*/



/* 
** 1. Using pyDocumentDB to connect Spark master node to DocumentDB gateway
**  [NOTE]: We're using Python here!
*/

/* Connect to spark-shell */
pyspark --master yarn

/* Execute script */
// Ensure `pyDocumentDB` is installed
// Refer to 'Spark to DocumentDB Demo.py'



/* 
** 2. Using Spark to DocumentDB Connector
*/


/* Connect to spark-shell */
spark-shell --master yarn --jars /home/sshuser/spark/jars/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar
//spark-shell --master yarn --jars /home/sshuser/spark/jars/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8



// Import Necessary Libraries
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.documentdb.spark.schema._
import com.microsoft.azure.documentdb.spark._
import com.microsoft.azure.documentdb.spark.config.Config
val readConfig2 = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==",
"Database" -> "DepartureDelays",
"preferredRegions" -> "East US 2;West US",
"Collection" -> "flights_pcoll", 
"SamplingRatio" -> "1.0"))
 
// Create collection connection 
val coll = spark.sqlContext.read.DocumentDB(readConfig2)
coll.createTempView("c")
 

// Queries
//var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA' LIMIT 100"
//var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c "

// Run DF query (count)
val start = new DateTime()
val df = spark.sql(query)
df.count()
val end = new DateTime()
val duration = new Duration(start, end)
PeriodFormat.getDefault().print(duration.toPeriod())


// Run DF query (show)
val start = new DateTime()
df.show()
val end = new DateTime()
val duration = new Duration(start, end)
PeriodFormat.getDefault().print(duration.toPeriod())



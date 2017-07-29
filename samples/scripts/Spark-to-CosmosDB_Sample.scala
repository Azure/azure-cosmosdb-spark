/* 
** Spark-to-CosmosDB_Sample.scala
** 
*/

//
// Executing spark-shell example
//
// spark-shell --master yarn --jars /home/sshuser/jars/azure-cosmosdb-spark-0.0.3-SNAPSHOT.jar,/home/sshuser/jars/azure-documentdb-1.12.0-SNAPSHOT.jar,
// /home/sshuser/jars/rxjava-1.3.0.jar, /home/sshuser/jars/azure-documentdb-rx-0.9.0-rc1.jar
//



// Import Necessary Libraries
//	Fill in your end point, masterkey, database, preferredRegions, and collection
//	Preferred Regions example: "preferredRegions" -> "Central US;East US 2",
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
val readConfig2 = Config(Map("Endpoint" -> "[Enter Endpoint Here]",
"Masterkey" -> "[Enter Masterkey Here]",
"Database" -> "[Enter Database Here]",
"preferredRegions" -> "[Enter preferredRegions Here]",
"Collection" -> "[Enter Collection Here]", 
"SamplingRatio" -> "1.0"))

 
// Create collection connection 
val coll = spark.sqlContext.read.cosmosDB(readConfig2)
coll.createOrReplaceTempView("c")
 

// Queries
// 	Enter your SQL query here
var query = "[Enter your SQL query here]"

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



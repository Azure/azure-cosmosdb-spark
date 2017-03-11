/* 
** Spark-to-DocumentDB_Sample.scala
** 
*/



// Import Necessary Libraries
//	Fill in your end point, masterkey, database, preferredRegions, and collection
//	Preferred Regions example: "preferredRegions" -> "Central US;East US 2",
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.documentdb.spark.schema._
import com.microsoft.azure.documentdb.spark._
import com.microsoft.azure.documentdb.spark.config.Config
val readConfig2 = Config(Map("Endpoint" -> "[Enter Endpoint Here]",
"Masterkey" -> "[Enter Masterkey Here]",
"Database" -> "[Enter Database Here]",
"preferredRegions" -> "[Enter preferredRegions Here]",
"Collection" -> "[Enter Collection Here]", 
"SamplingRatio" -> "1.0"))

 
// Create collection connection 
val coll = spark.sqlContext.read.DocumentDB(readConfig2)
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



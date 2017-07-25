/* 
** Spark-to-CosmosDB-Graph_Sample.scala
** 
*/


//
// Executing spark-shell example
//
// spark-shell --master yarn --jars /home/sshuser/jars/azure-cosmosdb-spark-0.0.3-SNAPSHOT.jar,/home/sshuser/jars/azure-documentdb-1.12.0-SNAPSHOT.jar
//


// Import Necessary Libraries
//	Fill in your end point, masterkey, database, preferredRegions, and collection
//	Preferred Regions example: "preferredRegions" -> "Central US;East US 2",
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config


// Maps
//  Note: Endpoint is `documents.azure.com` even for a graph container, e.g.
//    it is "https://tychostation.documents.azure.com:443/", not "https://tychostation.graphs.azure.com:443/"
//
val baseConfigMap = Map(
"Endpoint" -> "[Enter Endpoint Here]",
"Masterkey" -> "[Enter Masterkey Here]",
"Database" -> "[Etnter Database Here]",
"preferredRegions" -> "Central US",
"Collection" -> "flights", 
"SamplingRatio" -> "1.0",
"schema_samplesize" -> "1000"
)

val airportConfigMap = baseConfigMap ++ Map("query_custom" -> "select * from c where c.label='airport'") 
val delayConfigMap = baseConfigMap ++ Map("query_custom" -> "select * from c where c.label='flight'") 


// Configs
// get airport data (vertices)
val airportConfig = Config(airportConfigMap)
val airportColl = spark.sqlContext.read.cosmosDB(airportConfig)
airportColl.createOrReplaceTempView("airportColl") 

// get flight delay data (edges)
val delayConfig = Config(delayConfigMap)
val delayColl = spark.sqlContext.read.cosmosDB(delayConfig)
delayColl.createOrReplaceTempView("delayColl") 


//
// DataFrames
//

// airport information
// 	convert vertices from array<struct[string]> to string
val apdf1 = spark.sql("select id, iata, city, state, country from airportColl")
val apdf2 = apdf1.select($"id", $"iata._value", $"city._value", $"state._value", $"country._value")
val apnn = Seq("id", "iata", "city", "state", "country")
val apdf3 = apdf2.toDF(apnn: _*)
apdf3.createOrReplaceTempView("apdf3")
val airports = spark.sql("select id, cast(concat_ws(',', iata) as string) as iata, cast(concat_ws(',', city) as string) as city, concat_ws(',', state) as state, concat_ws(',', country) as country from apdf3")
airports.createOrReplaceTempView("airports")


// delay (edge) information
val delaysBase = spark.sql("select id, tripid, _vertexid as vx_src, _sink as vx_dst, delay, distance from delayColl")
delaysBase.createOrReplaceTempView("delaysBase")
val delays = spark.sql("select f.tripid, s.iata as src, d.iata as dst, f.delay, f.distance, cast(substring(f.tripid, 0, 8) as int) as jsdate from delaysBase f join airports s on s.id = f.vx_src join airports d on d.id = f.vx_dst")
delays.createOrReplaceTempView("delays")

//
// Queries

// Top 5 destination cities departing from Seattle
spark.sql("select a.city, sum(f.delay) as TotalDelay from delays f join airports a on a.iata = f.dst where f.src = 'SEA' and f.delay < 0 group by a.city order by sum(f.delay) limit 5").show()

// Calculate median delays by destination cities departing from Seattle
spark.sql("select a.city, percentile_approx(f.delay, 0.5) as median_delay from delays f join airports a on a.iata = f.dst where f.src = 'SEA' and f.delay < 0 group by a.city order by median_delay").show()




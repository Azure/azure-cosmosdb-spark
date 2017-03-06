//
// Spark to DocumentDB v3
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

// sparkitup
ssh -i ~/.ssh/id_rsa_azure2 -C2qTnNf -D 9876 sshuser@sparkitup-ssh.azurehdinsight.net
// http://10.0.0.17:4040
// http://10.0.0.17:8050



// Start spark-shell
cd $SPARK_HOME
//./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 

// Vivace
./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 
./bin/spark-shell --master spark://vivace.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --jars /home/dennylee/spark/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 4G --total-executor-cores 4

// Kintaro
./bin/spark-shell --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --executor-memory 8G
./bin/spark-shell --master spark://kintaro.f1xwivc2xm4uporkjsjoixul2c.dx.internal.cloudapp.net:7077 --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 --jars /home/dennylee/spark/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8

// sparkitup
//spark-shell --master yarn --jars /home/sparkuser/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8
spark-shell --master yarn --jars /home/sshuser/j3/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8
spark-shell --master yarn --jars wasb://data@doctorwhostore.blob.core.windows.net/azure-documentdb-spark-0.0.1-jar-with-dependencies.jar --executor-memory 8G --total-executor-cores 8

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
//"Collection" -> "flights", 
// Note, there appears to be a bug concerning connecting to preferredRegions
//"preferredRegions" -> "West US;East US",

 
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


 

//			DF1.count		DF1.show		DF2.count		DF2.show	
// ** Kintaro **
// Single collection				
// Q1:		00:00:01.183	00:00:00.876	00:00:00.958	00:00:00.767
// Q2: 		00:00:01.802	00:00:00.843	00:00:01.558	00:00:00.840
// Q3: 		00:00:56.642	00:00:00.844	00:00:54.931	00:00:00.718
//
// Partitioned Collections
// Q1:		00:00:02.011	00:00:00.981	00:00:01.658	00:00:00.832
// Q2: 		00:00:01.928	00:00:00.615	00:00:01.813	00:00:00.678
// Q3: 		00:00:19.385	00:00:00.667	00:01:41.818	00:00:00.939
//			00:00:22.056	00:00:00.796	00:02:22.850	00:00:00.788
//																			
// ** SparkItUp (HDInsight: 2 masters, 4 workers)
// Partitioned Collections
// Q1:		00:00:01.286	00:00:00.655	00:00:00.868	00:00:00.631
// Q2:		00:00:01.582	00:00:00.831	00:00:01.339	00:00:00.494
// Q3: 		00:00:16.955	00:00:01.125	00:00:12.982	00:00:00.691
//
//   4w		00:00:11.129					00:00:09.958
//	 6w		00:00:10.028					00:00:10.495
//	 8w		00:00:10.323					00:00:09.723
//   12w	00:00:08.899					00:00:09.153
//	 20w	00:00:10.210					00:00:10.398


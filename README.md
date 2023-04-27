# NOTE: There is a new Cosmos DB Spark Connector for Spark 3 available
# --------------------------------------------------------------------
The new Cosmos DB Spark connector has been released. The Maven coordinates (which can be used to install the connector in Databricks) are "**com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.0.0**"

The **source code** for the new connector is located here: https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3_2-12

A **migration guide** to change applications which used the Spark 2.4 connector is located here: https://aka.ms/azure-cosmos-spark-3-migration

**The quick start introduction:** https://aka.ms/azure-cosmos-spark-3-quickstart
**Config Reference:** https://aka.ms/azure-cosmos-spark-3-config
**End-to-end samples:** https://aka.ms/azure-cosmos-spark-3-sample-nyc-taxi-data/01_Batch.ipynb
# ---------------------------------------------------------------------


<img src="https://raw.githubusercontent.com/dennyglee/azure-cosmosdb-spark/main/docs/images/azure-cosmos-db-icon.png" width="75">  &nbsp; Azure Cosmos DB Connector for Apache Spark
==========================================

[![Build Status](https://travis-ci.org/Azure/azure-cosmosdb-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-cosmosdb-spark)


`azure-cosmosdb-spark` is the official connector for [Azure Cosmos DB](http://cosmosdb.com) and [Apache Spark](http://spark.apache.org). The connector allows you to easily read to and write from Azure Cosmos DB via Apache Spark DataFrames in `python` and `scala`.  It also allows you to easily create a lambda architecture for batch-processing, stream-processing, and a serving layer while being globally replicated and minimizing the latency involved in working with big data.


<details>
<summary><strong><em>Table of Contents</em></strong></summary>

* [Jump Start](#jump-start)
  * [Reading from Cosmos DB](#reading-from-Cosmos-DB)
  * [Writing to Cosmos DB](#writing-to-Cosmos-DB)
* [Requirements](#requirements)
* [Working with the connector](#working-with-the-connector)
  * [Using Databricks notebooks](#using-databricks-notebooks)
  * [Using spark-cli](#using-spark-cli)
  * [Using Jupyter notebooks](#using-jupyter-notebooks)
* [Build the connector](#build-the-connector)
* [Working with our samples](#working-with-our-samples)
* [More Information](#more-information)
* [Contributing & Feedback](#contributing--feedback)

</details>

## Jump Start

### Reading from Cosmos DB
Below are excerpts in `Python` and `Scala` on how to create a Spark DataFrame to read from Cosmos DB

```python
# Read Configuration
readConfig = {
  "Endpoint" : "https://doctorwho.documents.azure.com:443/",
  "Masterkey" : "<YourMasterKey>",
  "Database" : "DepartureDelays",
  "preferredRegions" : "Central US;East US2",
  "Collection" : "flights_pcoll",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
}

# Connect via azure-cosmosdb-spark to create Spark DataFrame
flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()
flights.count()
```


<details>
<summary><em>Click for <strong>Scala</strong> Excerpt</em></summary>
<p>

```scala
// Import Necessary Libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val readConfig = Config(Map(
  "Endpoint" -> "https://doctorwho.documents.azure.com:443/",
  "Masterkey" -> "<YourMasterKey>",
  "Database" -> "DepartureDelays",
  "PreferredRegions" -> "Central US;East US2;",
  "Collection" -> "flights_pcoll",
  "SamplingRatio" -> "1.0",
  "query_custom" -> "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
))

// Connect via azure-cosmosdb-spark to create Spark DataFrame
val flights = spark.read.cosmosDB(readConfig)
flights.count()
```

</p>
</details>

### Writing to Cosmos DB
Below are excerpts in `Python` and `Scala` on how to write a Spark DataFrame to Cosmos DB

```python
# Write configuration
writeConfig = {
 "Endpoint" : "https://doctorwho.documents.azure.com:443/",
 "Masterkey" : "<YourMasterKey>",
 "Database" : "DepartureDelays",
 "Collection" : "flights_fromsea",
 "Upsert" : "true"
}

# Write to Cosmos DB from the flights DataFrame
flights.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()
```

<details>
<summary><em>Click for <strong>Scala</strong> Excerpt</em></summary>
<p>

```scala
// Configure connection to the sink collection
val writeConfig = Config(Map(
  "Endpoint" -> "https://doctorwho.documents.azure.com:443/",
  "Masterkey" -> "<YourMasterKey>",
  "Database" -> "DepartureDelays",
  "PreferredRegions" -> "Central US;East US2;",
  "Collection" -> "flights_fromsea",
  "WritingBatchSize" -> "100"
))

// Upsert the dataframe to Cosmos DB
import org.apache.spark.sql.SaveMode
flights.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
```

</p>
</details>

&nbsp;

## Requirements

<em>Review <strong>supported</strong> component versions</em>

| Component | Versions Supported |
| --------- | ------------------ |
| Apache Spark | 2.2.1, 2.3.X, 2.4.X |
| Scala | 2.11 |
| Python | 2.7, 3.6 |

&nbsp;

## Working with the connector
You can build and/or use the maven coordinates to work with `azure-cosmosdb-spark`.

<em>Review the connector's <strong>maven versions</strong></em>

| Spark | Scala | Latest version |
|---|---|---|
| 2.4.0 | 2.11 | [azure-cosmosdb-spark_lkg_version](https://aka.ms/CosmosDB_OLTP_Spark_2.4_LKG)
| 2.3.0 | 2.11 | [azure-cosmosdb-spark_2.3.0_2.11_1.3.3](https://search.maven.org/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.3.0_2.11/1.3.3/jar)
| 2.2.0 | 2.11 | [azure-cosmosdb-spark_2.2.0_2.11_1.1.1](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-cosmosdb-spark_2.2.0_2.11%7C1.1.1%7Cjar)
| 2.1.0 | 2.11 | [azure-cosmosdb-spark_2.1.0_2.11_1.2.2](https://search.maven.org/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.1.0_2.11/1.2.2)

### Using Databricks notebooks
Please create a library using within your Databricks workspace by following the guidance within the Azure Databricks Guide > [Use the Azure Cosmos DB Spark connector](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html)

> Note, the Databricks documentation at docs.azuredatabricks.net is not up to date.  Instead of downloading the six separate jars into six different libraries, you can download the uber jar from maven at https://search.maven.org/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.4.0_2.11/1.3.5/jar) and install this one jar/library.


### Using spark-cli
To work with the connector using the spark-cli (i.e. `spark-shell`, `pyspark`, `spark-submit`), you can use the `--packages` parameter with the connector's [maven coordinates](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.4.0_2.11).

```sh
spark-shell --master yarn --packages "com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5"

```

### Using Jupyter notebooks
If you're using Jupyter notebooks within HDInsight, you can use spark-magic `%%configure` cell to specify the connector's maven coordinates.

```python
{ "name":"Spark-to-Cosmos_DB_Connector",
  "conf": {
    "spark.jars.packages": "com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5",
    "spark.jars.excludes": "org.scala-lang:scala-reflect"
   }
   ...
}
```

> Note, the inclusion of the `spark.jars.excludes` is specific to remove potential conflicts between the connector, Apache Spark, and Livy.

### Build the connector
Currently, this connector project uses `maven` so to build without dependencies, you can run:

```sh
mvn clean package
```

&nbsp;

## Working with our samples

Included in this GitHub repository are a number of sample notebooks and scripts that you can utilize:
* **On-Time Flight Performance with Spark and Cosmos DB (Seattle)** [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/On-Time%20Flight%20Performance%20with%20Spark%20and%20Cosmos%20DB%20-%20Seattle.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/On-Time%20Flight%20Performance%20with%20Spark%20and%20Cosmos%20DB%20-%20Seattle.html): This notebook utilizing `azure-cosmosdb-spark` to connect Spark to Cosmos DB using HDInsight Jupyter notebook service to showcase Spark SQL, GraphFrames, and predicting flight delays using ML pipelines.
* **[Connecting Spark with Cosmos DB Change feed](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Spark%2Band%2BCosmos%2BDB%2BChange%2BFeed.ipynb)**: A quick showcase on how to connect Spark to Cosmos DB Change Feed.
* **Twitter Source with Apache Spark and Azure Cosmos DB Change Feed**: [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Twitter%20with%20Spark%20and%20Azure%20Cosmos%20DB%20Change%20Feed.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Twitter%20with%20Spark%20and%20Azure%20Cosmos%20DB%20Change%20Feed.html)
* **Using Apache Spark to query Cosmos DB Graphs**: [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Using%20Apache%20Spark%20to%20query%20Cosmos%20DB%20Graphs.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Using%20Apache%20Spark%20to%20query%20Cosmos%20DB%20Graphs.html)
* **[Connecting Azure Databricks to Azure Cosmos DB](https://docs.databricks.com/spark/latest/data-sources/azure/cosmosdb-connector.html)** using `azure-cosmosdb-spark`.  Linked here is also an Azure Databricks version of the [On-Time Flight Performance notebook](https://github.com/dennyglee/databricks/tree/master/notebooks/Users/denny%40databricks.com/azure-databricks).
* **[Lambda Architecture with Azure Cosmos DB and HDInsight (Apache Spark)](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/readme.md)**: Combining the Azure Cosmos DB, , and HDInsight not only allows you to accelerate real-time big data analytics, but also allows you to benefit from a Lambda Architecture while simplifying its operations.

&nbsp;

## More Information

We have more information in the `azure-cosmosdb-spark` [wiki](https://github.com/Azure/azure-cosmosdb-spark/wiki) including:
* [Azure Cosmos DB Spark Connector User Guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide)
* [Aggregations Examples](https://github.com/Azure/azure-documentdb-spark/wiki/Aggregations-Examples)

Configuration and Setup
* [Spark Connector Configuration](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references)
* [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-documentdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup) (In progress)
* [Configuring Power BI Direct Query to Azure Cosmos DB via Apache Spark (HDI)](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuring-Power-BI-Direct-Query-to-Azure-Cosmos-DB-via-Apache-Spark-(HDI))

Troubleshooting
* [Using Cosmos DB Aggregates](https://github.com/Azure/azure-documentdb-spark/wiki/Troubleshooting:-Using-Cosmos-DB-Aggregates)
* [Known Issues](https://github.com/Azure/azure-cosmosdb-spark/wiki/Known-Issues)

Performance
* [Performance Tips](https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips)
* [Query Test Runs](https://github.com/Azure/azure-documentdb-spark/wiki/Query-Test-Runs)
* [Writing Test Runs](https://github.com/Azure/azure-cosmosdb-spark/wiki/Writing-Test-Runs)

Change Feed
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)
* [Change Feed Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Change-Feed-demos)
* [Structured Stream Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)

Monitoring
* [Monitoring Spark jobs with application insights](https://github.com/Azure/azure-cosmosdb-spark/tree/2.3/samples/monitoring)

&nbsp;

## Contributing & Feedback

This project has adopted the [Microsoft Open Source Code of
Conduct](https://opensource.microsoft.com/codeofconduct/).  For more information
see the [Code of Conduct
FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

To give feedback and/or report an issue, open a [GitHub
Issue](https://help.github.com/articles/creating-an-issue/).


*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*


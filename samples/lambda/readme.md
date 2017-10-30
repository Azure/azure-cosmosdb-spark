# Lambda Architecture
This samples folder the Lambda Architecture: Re-architected with Apache Spark and Azure Cosmos DB per the diagram below.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/130adaf0c7e45fff4033cddf13adee166b0cf058/docs/images/scenarios/Lambda-architecture-rearchitected.png)

The samples included are
 1. We will use the [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed) as our mechanism to push **new data** into Cosmos DB.
 2. The **batch layer** is comprised of the *master dataset* (an immutable, append-only set of raw data) and ethe ability to pre-compute batch views of the data that will be pushed into the **serving layer**
    * The [Lambda Architecture Re-architected - Batch Layer]() notebook [ipynb]() | [html]() queries the *master dataset* set of batch views.
    * The [Lambda Architecture Re-architected - Batch to Serving Layer]() notebook [ipynb]() | [html]() pushes the *batch* data to the *serving layer*; i.e. Spark will query a batch collection of tweets, process it, and store it into another collection (i.e. *computed batch*).
  

## Requirements
* Azure Cosmos DB Collection
* HDI (Apache Spark 2.1) cluster
* Spark Connector: We are currently using the [0.0.5-SNAPSHOT](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-0.0.5-SNAPSHOT) version of the connector.

## References
* [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed)
* [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup)
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)
* [Structured Stream demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)


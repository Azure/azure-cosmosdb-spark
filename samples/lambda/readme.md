# Lambda Architecture
This samples folder the Lambda Architecture: Re-architected with Apache Spark and Azure Cosmos DB per the diagram below.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/130adaf0c7e45fff4033cddf13adee166b0cf058/docs/images/scenarios/Lambda-architecture-rearchitected.png)

The samples included are
 1. We will use the [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed) as our mechanism to push **new data** into Cosmos DB.


## Requirements
* Azure Cosmos DB Collection
* HDI (Apache Spark 2.1) cluster
* Spark Connector: We are currently using the [0.0.5-SNAPSHOT](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-0.0.5-SNAPSHOT) version of the connector.

## References
* [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed)
* [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup)
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)
* [Structured Stream demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)


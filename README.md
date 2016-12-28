# azure-documentdb-spark
This project provides a client library that allows Azure DocumentDB to act as an input source or output sink for Spark jobs.

There will be two approaches for the **Spark-to-DocumentDB** connector:

* Using `pyDocumentDB`
* Create a Java-based Spark-DocumentDB connector based utilizing the [DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java) and based off of the [`FiloDB` Project](https://github.com/filodb/FiloDB)


## pyDocumentDB
The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `DocumentDB` 
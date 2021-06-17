### 3.6.14
- Moves to version 2.12.4 of the Cosmos Bulk Executor SDK and version 2.6.4 of the Cosmos Java SDK to...
    - add retries on `SocketTimeoutException` errors for read-only requests in more scenarios
    
### 3.6.13
- Fixes another issue sometimes causing "Invalid partition key range" errors when processing queries while partition splits happened.

### 3.6.12
- Fixes an issue sometimes causing "Invalid partition key range" errors when processing queries while partition splits happened.

### 3.6.11
- Moves to version 2.12.2 of the Cosmos Bulk Executor SDK to...
    - fix an issue where on some transient errors documents sometimes were not ingested but skipped without throwing an error
    - move to the latest version of the Guava dependency (30.1.1-jre) to address a security issue in previous Guava versions
- Moves to version 2.9.10.8 of com.fasterxml.jackson.core/jackson-databind to address a known security issue in previous versions

### 3.6.10
- Moving to version 2.6.2 of the Cosmos SDK to also add retries on read timeouts there
- Fixes an issue when trying to save rdd with 0 partitions

### 3.6.9
- Adds retries on ReadTimeouts

### 3.6.8
- Reduces the performance overhead when a Spark DataFrame as many partitions - especially when using  Cosmos DB as a sink in Spark Streaming scenarios

### 3.6.7
- Fixes a bug introduced in 3.6.6 in the retry policy on collection recreation
- Adds the logic to handle EOF exception in Streaming checkpoint reads caused by the transient flush exception during checkpoint writes

### 3.6.6
- Adds a check to purge the Connection Cache when a Container is not available anymore (due to being dropped and recreated for example) during read operations

### 3.6.5
- Fixes WriteStream retry policy to always treat unique key constraint violation as non-transient error

### 3.6.4
- Fixes a regression introduced in 3.4.0 causing NullPointerException during writeStream 
- Adds a check to purge the Connection Cache when a Container is not available anymore (due to being dropped and recreated for example) during write operations

### 3.6.3
- Adds a boolean config to enable converting nested docs that are derived as string into the native json format  

### 3.6.2
- Fixes an issue sometimes resulting in unnecessary duplicates when using readStream in combination with maxPagesPerBacth setting and updating documents  

### 3.6.1
- Fixes an issue preventing docs with MapType and of type NonInternalRow to be ingested in Batch mode to Cosmos DB 

### 3.4.0
- Added support for preserveNullInWrite option to preserve null values in write.
- Improves Write Throughput Budget accuracy that can be used to limit the RU consumption during bulk operations

### 3.3.4
- Fixes an issue in Streaming preventing docs with MapType to be ingested into Cosmos DB

### 3.3.3
- Fixes an issue preventing apps from gracefully shutting down by moving Timer task in CosmosDBConnectionCache to a daemon thread

### 3.3.2
- Fixes an issue preventing updates to existing documents being captured in bookmarks when maxPagesPerBatch is used with streaming. 

### 3.3.1
- Fixes null pointer exception in streaming schema inference. 

### 3.3.0
- Adds a new config option "changefeedstartfromdatetime" that can be used to specify from when the changefeed should be processed. See [Config options](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references) for details.

### 3.2.0
- Fixes a regression that caused excessive memory consumption on the executors for large result sets (for example with millions of rows) ultimately resulting in an error "java.lang.OutOfMemoryError: GC overhead limit exceeded"

### 3.1.1
- Fixes a streaming checkpoint edge case where in the "id" contains "|" character with the "ChangeFeedMaxPagesPerBatch" config applied

### 3.1.0
- Adds support for bulk updates when using nested partition keys
- Adds support for Decimal and Float data types during writes to Cosmos DB.
- Adds support for Timestamp types when they are using Long (unix Epoch) as a value

### 3.0.8
- Fixes type cast exception when using "WriteThroughputBudget" config.

### 3.0.7
- Adds error information for bulk failures to exception and log.

### 3.0.6
- Fixes streaming checkpoint issues.

### 3.0.5
- Fixes log level of a message left unintentionally with level ERROR to reduce noise

### 3.0.4
- Fixes a bug in structured streaming during partition splits - possibly resulting in missing some change feed records or seeing Null exceptions for checkpoint writes

### 3.0.3
- Fixes a bug where a custom schema provided for readStream is ignored

### 3.0.2
- Fixes regression (unshaded JAR includes all shaded dependencies) which increased build time by 50%

### 3.0.1
- Fixes a dependency issue causing Direct Transport over TCP to fail with RequestTimeoutException

### 3.0.0
- Improves connection management and connection pooling to reduce number of metadata calls

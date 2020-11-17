### 3.3.5-SNAPSHOT
- Added support for preserveNullInWrite option to preserve null values in write.

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

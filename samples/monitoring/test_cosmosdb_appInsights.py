# Databricks notebook source
# Import Necessary Libraries
import datetime
from applicationinsights import TelemetryClient
import logging
from applicationinsights.logging import enable
import time
from applicationinsights.exceptions import enable
from applicationinsights import channel
from applicationinsights.logging import LoggingHandler

instrumentation_key = '<Instrumentation key>'

#Enable unhandled exception logging
enable(instrumentation_key)

#setup other needed variables
tc = TelemetryClient(instrumentation_key)
tc.context.application.ver = '0.0.1'
tc.context.device.id = 'Sample notebook'

telemetry_channel = channel.TelemetryChannel()
telemetry_channel.context.application.ver = '1.0.0'
telemetry_channel.context.properties['application_name'] = 'sample_notebook'
telemetry_channel.context.properties['application_id'] = sc.applicationId

handler = LoggingHandler(instrumentation_key, telemetry_channel=telemetry_channel)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger = logging.getLogger('simple_logger')
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('Starting sample app ... ')

def getReadConfig():
  readConfig = {
    "Endpoint" : "https://nomier-test-sql.documents.azure.com:443/",
    "Masterkey" : "<MK>",
    "Database" : "partitionIdTestDB",
    "Collection" : "sourcecollection", 
    "SamplingRatio" : "1.0"
  }
  
  logger.info('read config from cosmos db: ' + str(readConfig))
  return readConfig

def getWriteConfig():
  writeConfig = {
    "Endpoint" : "https://nomier-test-sql.documents.azure.com:443/",
    "Masterkey" : "<MK>",
    "Database" : "partitionIdTestDB",
    "Collection" : "outputcollection",
    "Upsert" : "true"
  }
  
  logger.info('write config from cosmos db: ' + str(writeConfig))
  return writeConfig
  
  
def readFromCosmosDB(config): 
  readstart = time.time()
  tc.track_event('Reading from Cosmos DB collection')
  readDF = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**config).load()
  readend = time.time()
  tc.track_metric('read_latency', readend-readstart)
  return readDF

def writeToCosmosDB(df, config):
  writestart = time.time()  
  tc.track_event('Writing to a new Cosmos DB collection')
  df.write.mode("overwrite").format("com.microsoft.azure.cosmosdb.spark").options(**config).save()
  writeend = time.time()
  tc.track_metric('write_latency', writeend-writestart)
  logger.info('finished writing to cosmos db')

def count_records(df):
  count = df.count()
  logger.info('finished reading from cosmos db with ' + str(count) + ' records')
  tc.track_metric('read_count', count)
  print('records count: ' + str(count))
  
def run():
  try:
    readConfig = getReadConfig()
    df = readFromCosmosDB(readConfig)
    count_records(df)
    writeConfig = getWriteConfig()
    writeToCosmosDB(df, writeConfig)
  except Exception, e:
    logger.error(e)
    print(e)
    tc.track_exception()
  finally:  
    tc.flush()
    logging.shutdown()
  
# start running the sample app  
run()


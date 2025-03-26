__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Ingestion Module
   Purpose             :   This module is used for Ingestion file into data lake Glue Catalog Table

   Input               :   JSON File
   Output              :   Bool
   How to Invoke       :   call function ingestData(<S3 Prefix of JSON File for Dataset Parameters>)
   Last changed on     :   2025-02-19
   Last changed by     :   Mrinal Paul
   Reason for change   :   Bug Fixes
"""
import sys
import re
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import base64
import json
import environmentParams as environmentParams
try:
    if environmentParams.PLATFORM_NAME.lower() == "glue":
        from awsglue.context import GlueContext
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        logger = glueContext.get_logger()
    elif environmentParams.PLATFORM_NAME.lower() == "databricks":
        from databricks.sdk.runtime import *
        from pyspark.sql import *
        import logging
        logging.basicConfig()
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        hndlr = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hndlr.setFormatter(formatter)
        spark = SparkSession.builder.appName("DataFlexor").getOrCreate()
except:
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

from pyspark.sql.functions import explode,explode_outer,regexp_replace,udf,lit,col,when,unix_timestamp,to_timestamp,date_format
from pyspark.sql.types import *
from DataFlexor import s3Functions,archive,reProcessIngestedFile,GlueTable,Utils,versionUpgread
from DataFlexorCommonEtl import commonTransformation,schemaBuilder
import time

class ingestion:
    def __init__(self,env=None,notify=True,cycleId=None):
        self.notify = notify
        if environmentParams.PLATFORM_NAME.lower() == "glue":
            if environmentParams.MWAA_INTEGRATION != False:
                self.args = getResolvedOptions(sys.argv, ["dataset_master_key","env","run_id"])
            else:
                self.args = getResolvedOptions(sys.argv, ["dataset_master_key","env","run_id","process_name","step_name"])
                self.processName = self.args["process_name"]
                self.stepName = self.args["step_name"]
        else:
            self.args = dbutils.widgets.getAll()
            self.processName = self.args["process_name"]
            self.stepName = self.args["step_name"]
            env = environmentParams.ENV_NAME.lower()
        self.runId = self.args['run_id']
        if env == None:
            self.env = self.args['env']
        else:
            self.env = env
        # create S3 Client
        self.s3Obj = s3Functions()
        self.executionDate = datetime.utcnow().strftime("%Y%m%d")

        if environmentParams.PLATFORM_NAME.lower() == "glue":
            try:
                excCycle = getResolvedOptions(sys.argv, ["execution_cycle_id"])
                cycleId = excCycle["execution_cycle_id"]
            except:
                cycleId = None
        else:
            try:
                excCycle = dbutils.widgets.getAll()
                cycleId = excCycle["execution_cycle_id"]
            except:
                cycleId = None
        if cycleId != None:
            self.cycleId = cycleId
        else:
            self.cycleId = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        self.processedTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.commonTransformationObj = commonTransformation()
        self.logTableSchema = StructType([
                StructField("table_name", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("application", StringType(), True),
                StructField("source_files", StringType(), True),
                StructField("static_file_pattern", StringType(), True),
                StructField("source_file_pattern", StringType(), True),
                StructField("file_type", StringType(), True),
                StructField("encoding", StringType(), True),
                StructField("escape", StringType(), True),
                StructField("multiline", StringType(), True),
                StructField("delimiter", StringType(), True),
                StructField("table_location", StringType(), True),
                StructField("target_schema", StringType(), True),
                StructField("target_table", StringType(), True),
                StructField("record_count", IntegerType(), True),
                StructField("execution_start_datetime", StringType(), True),
                StructField("execution_end_time", StringType(), True)
            ])
        self.logFileTableSchema = StructType([
                StructField("table_name", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("input_location_prefix", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("file_type", StringType(), True),
                StructField("record_count", IntegerType(), True),
                StructField("execution_start_time", StringType(), True),
                StructField("execution_end_time", StringType(), True),
                StructField("ingestion_status", StringType(), True)
            ])
        self.totalSkippedFileCount = 0
        # Upgread Version Checks
        versionUpgreadObj = versionUpgread(self.env)
        versionUpgreadObj.processVersionUpgread()
    def updateGlueTable(self,sourceDataDf,targetSchema,targetTable,tableDescription,finalTableLocation,partitionKeys,partitionValues,archiveFlag=False):
        try:
            msg = "Partition Key: " + str(partitionKeys)
            logger.info(msg)
            msg = "Partition Value: " + str(partitionValues)
            logger.info(msg)
            # Create/Update Table
            columnList = self.commonTransformationObj.getColumnDataType(sourceDataDf)
            msg = "Loading Glue table {} in schema {}".format(targetTable,targetSchema)
            logger.info(msg)
            GlueTableObj = GlueTable(targetSchema,targetTable,tableDescription,columnList,finalTableLocation,partitionKeys,partitionValues)
            # if archiveFlag == True:
            #     GlueTableObj.getAndDeletePartitions()
            # Load Partition
            status = GlueTableObj.loadGlueTable()
            if status == True:
                logger.info("Table "+targetTable+" loaded successfully")
                return True
            else:
                return False
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def updateIngestionLog(self,fileNameLog,rowCount):
        try:
            # Prepare Ingestion log Datafrate amd table
            ingestionLogSchema = environmentParams.LOG_SCHEMA.replace("`","")
            logTable = "ingestion_log_"+self.applicationName
            logTable = logTable.replace("-","_")

            ingestionEndTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            ingestionLogList = [(self.targetTable,self.runId,self.applicationName,fileNameLog,self.staticFilePattern,self.filePattern,self.fileType,self.encoding,self.escape,self.multiline,self.delimiter,self.tablePath,self.targetSchema,self.targetTable, rowCount, self.processedTime, ingestionEndTime)]
            ingestionLogDf = spark.createDataFrame(ingestionLogList, schema=self.logTableSchema)
            # Write Log to S3 location
            logTablePath = "s3://" + environmentParams.CURATED_S3_BUCKET.format(self.env) + "/DataFlexor/" + self.applicationName + "/" + logTable + "/"
            logTablePathFinal = logTablePath + "dataset_name=" + self.datasetName + "/execution_date=" + self.executionDate + "/execution_cycle_id=" + str(self.cycleId) + "/"
            msg = "Ingestion Log Table Path: {}".format(logTablePath)
            logger.info(msg)
            msg = "Ingestion Log Table Partition Path: {}".format(logTablePathFinal)
            logger.info(msg)
            ingestionLogDf.replace("null","").write.mode("overwrite").option("multiline",True).option("escape","\"").option("encoding", "utf-8").parquet(logTablePathFinal)
            partitionKeys = [
                {
                    'name': "dataset_name",
                    'data_type': "string"
                },
                {
                    'name': "execution_date",
                    'data_type': "string"
                },
                {
                    'name': "execution_cycle_id",
                    'data_type': "string"
                }
            ]
            partitionValues = {
                "dataset_name": str(self.datasetName),
                "execution_date": str(self.executionDate),
                "execution_cycle_id": str(self.cycleId)
            }
            msg = "Original Partition Key: " + str(partitionKeys)
            logger.info(msg)
            msg = "Original Partition Value: " + str(partitionValues)
            logger.info(msg)
            self.updateGlueTable(ingestionLogDf,ingestionLogSchema,logTable,'',logTablePath,partitionKeys,partitionValues)
            msg = "Updated Log Table: " + logTable
            logger.info(msg)
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def updateFileLog(self,sourcePrefix,fileName,rowCount,ingestionStatus,fileLoadStartTime):
        try:
            # Prepare Ingestion log Datafrate amd table
            ingestionLogSchema = environmentParams.LOG_SCHEMA.replace("`","")
            fileLogTable = "file_log_"+self.applicationName
            fileLogTable = fileLogTable.replace("-","_")

            fileLoadEndTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            fileLogList = [(self.targetTable,self.runId,sourcePrefix,fileName,self.fileType, rowCount, fileLoadStartTime, fileLoadEndTime, ingestionStatus)]
            ingestionLogDf = spark.createDataFrame(fileLogList, schema=self.logFileTableSchema)
            # Write Log to S3 location
            fileLogTablePath = "s3://" + environmentParams.CURATED_S3_BUCKET.format(self.env) + "/DataFlexor/" + self.applicationName + "/" + fileLogTable + "/"
            fileLogTablePathFinal = fileLogTablePath + "dataset_name=" + self.datasetName + "/execution_date=" + self.executionDate + "/execution_cycle_id=" + str(self.cycleId) + "/" + "source_file_name=" + fileName + "/"
            msg = "Ingestion File Log Table Path: {}".format(fileLogTablePath)
            logger.info(msg)
            msg = "Ingestion File Log Table Partition Path: {}".format(fileLogTablePathFinal)
            logger.info(msg)
            ingestionLogDf.replace("null","").write.mode("overwrite").option("multiline",True).option("escape","\"").option("encoding", "utf-8").parquet(fileLogTablePathFinal)
            partitionKeys = [
                {
                    'name': "dataset_name",
                    'data_type': "string"
                },
                {
                    'name': "execution_date",
                    'data_type': "string"
                },
                {
                    'name': "execution_cycle_id",
                    'data_type': "string"
                },
                {
                    'name': "source_file_name",
                    'data_type': "string"
                }
            ]
            partitionValues = {
                "dataset_name": str(self.datasetName),
                "execution_date": str(self.executionDate),
                "execution_cycle_id": str(self.cycleId),
                "source_file_name": str(fileName)
            }
            msg = "Updating File Log Table: {} in Schema: {}".format(fileLogTable,ingestionLogSchema)
            logger.info(msg)
            self.updateGlueTable(ingestionLogDf,ingestionLogSchema,fileLogTable,'',fileLogTablePath,partitionKeys,partitionValues)
            msg = "Updated Log Table: " + fileLogTable
            logger.info(msg)
            return True
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def createStagingtablePath(self,sourceFileLocation):
        try:
            msg = "Starting creation of stagging table path"
            logger.info(msg)
            sourceFileName = sourceFileLocation.split("/")[-1]
            partitionKeys = [
                {
                    'name': "execution_date",
                    'data_type': "string"
                },
                {
                    'name': "execution_cycle_id",
                    'data_type': "string"
                },
                {
                    'name': "source_file_name",
                    'data_type': "string"
                }
            ]
            partitionValues = {
                "execution_date": str(self.executionDate),
                "execution_cycle_id": str(self.cycleId),
                "source_file_name": str(sourceFileName)
            }
            tableSubPath = ""
            for k in partitionKeys:
                if tableSubPath == "":
                    tableSubPath = k["name"] + "=" + partitionValues[k["name"]] + "/"
                else:
                    tableSubPath = tableSubPath + k["name"] + "=" + partitionValues[k["name"]] + "/"
            msg = "Table Path: {} \nPartition Keys: {} \nPartition Value: {}".format(tableSubPath,str(partitionKeys),str(partitionValues))
            logger.info(msg)
            return tableSubPath,partitionKeys,partitionValues
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def getSchema(self,fileType):
        try:
            msg = "Creating Schema Metadata for File Type: {}".format(fileType)
            logger.info(msg)
            schema = None
            schemaBuilderObj = schemaBuilder()
            if self.schemaJson == None:
                msg = "Schema JSON File is Empty!"
                logger.error(msg)
                raise Exception(msg)
            if fileType.lower() in ["txt","csv","excel"]:
                schema = schemaBuilderObj.buildCsvTxtExcelShema(self.schemaJson)
            elif fileType.lower() == "json":
                schema = schemaBuilderObj.buildJsonShema(self.schemaJson)
            msg = "Schema for reading file: {}".format(str(schema))
            logger.info(msg)
            return schema
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def loadStagingTable(self,fileType,sourceFileLocation,option,header='true'):
        try:
            fileLoadStartTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            # Prepare Final S3 Location
            ingestionStatus = "Ingested"
            tableSubPath,partitionKeys,partitionValues = self.createStagingtablePath(sourceFileLocation)
            finalTableLocation = self.tablePath + tableSubPath
            sourceFileLocation = "s3://" + self.sourceS3Bucket + "/" + sourceFileLocation
            msg = "Started Loading File: " + sourceFileLocation
            logger.info(msg)
            sourceFileName = sourceFileLocation.split("/")[-1]
            sourcePrefix = sourceFileLocation.replace(sourceFileName,"").replace("s3://" + self.sourceS3Bucket + "/", "")
            # check if Source File Name contains special Character
            regex = re.compile('[@!%^&*<>?/\|}{~:]')
            if regex.search(sourceFileName) != None:
                msg = "Source File Name: {} is having special character which is not allowed!!! Only Alphabets, Numbers, Underscore and Hyphen are allowed".format(sourceFileName)
                logger.error(msg)
                raise Exception(msg)
            match fileType:
                case "csv":
                    if header == 'false':
                        # Get Schema
                        schema = self.getSchema(fileType)
                        msg = "Schema Details: " + str(schema)
                        logger.info(msg)
                        sourceDataDf = spark.read.format("csv").options(**option).schema(schema).load(sourceFileLocation)
                        sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                    else:
                        if self.schemaJson == None:
                            sourceDataDf = spark.read.format("csv").options(**option).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.hederNameStandardization(sourceDataDf)
                        else:
                            schema = self.getSchema(fileType)
                            msg = "Schema Details: " + str(schema)
                            logger.info(msg)
                            sourceDataDf = spark.read.format("csv").options(**option).schema(schema).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                case "excel":
                    if header == 'false':
                        # Get Schema
                        schema = self.getSchema(fileType)
                        msg = "Schema Details: " + str(schema)
                        logger.info(msg)
                        sourceDataDf = spark.read.format("com.crealytics.spark.excel").options(**option).schema(schema).load(sourceFileLocation)
                        sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                    else:
                        if self.schemaJson == None:
                            sourceDataDf = spark.read.format("com.crealytics.spark.excel").options(**option).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.hederNameStandardization(sourceDataDf)
                        else:
                            schema = self.getSchema(fileType)
                            msg = "Schema Details: " + str(schema)
                            logger.info(msg)
                            sourceDataDf = spark.read.format("com.crealytics.spark.excel").options(**option).schema(schema).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                case "txt":
                    if header == 'false':
                        # Get Schema
                        schema = self.getSchema(fileType)
                        msg = "Schema Details: " + str(schema)
                        logger.info(msg)
                        sourceDataDf = spark.read.format("csv").options(**option).schema(schema).load(sourceFileLocation)
                        sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                    else:
                        if self.schemaJson == None:
                            sourceDataDf = spark.read.format("csv").options(**option).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.hederNameStandardization(sourceDataDf)
                        else:
                            schema = self.getSchema(fileType)
                            msg = "Schema Details: " + str(schema)
                            logger.info(msg)
                            sourceDataDf = spark.read.format("csv").options(**option).schema(schema).load(sourceFileLocation)
                            sourceDataDf = self.commonTransformationObj.columnFormat(sourceDataDf,self.schemaJson)
                case "json":
                    if header == 'false':
                        # Get Schema
                        schema = self.getSchema(fileType)
                        msg = "Schema Details: " + str(schema)
                        logger.info(msg)
                        sourceDataDf = spark.read.options(**option).schema(schema).json(sourceFileLocation)
                    else:
                        sourceDataDf = spark.read.options(**option).json(sourceFileLocation)
                        sourceDataDf = self.commonTransformationObj.hederNameStandardization(sourceDataDf)
                    # Flatten data
                    sourceDataDf = self.commonTransformationObj.flattenJsonData(sourceDataDf)
                    if header == 'true':
                        for col in sourceDataDf.columns:
                            sourceDataDf = sourceDataDf.withColumn(col, sourceDataDf[col].cast(StringType()))
                case "parquet":
                    sourceDataDf = spark.read.format("parquet").options(**option).load(sourceFileLocation)
            sourceDataDf = self.commonTransformationObj.hederNameStandardization(sourceDataDf)
            if self.dropDuplicateRecords.upper() == 'Y':
                sourceDataDf = sourceDataDf.dropDuplicates()
            # Check if Source File is empty
            if sourceDataDf.rdd.isEmpty():
                logger.info("Input File is Empty: {}".format(sourceFileLocation))
                fileEmpty = True
            else:
                fileEmpty = False
            # Pre-DQM Check
            totalCriticalFiledQcCount = 0
            if self.preIngestionDQMConfig != None and self.preIngestionDQMConfig != "":
                try:
                    if fileEmpty == False:
                        msg = "Pre Ingestion DQM Config File Location: " + str(self.preIngestionDQMConfig)
                        logger.info(msg)
                        from DataGuardian import executeDqm
                        executeDqmObj = executeDqm(self.preIngestionDQMConfig,self.env,self.cycleId)
                        # Call Execute DQM Function
                        msg = "Starting DQM Check for Table: " + self.datasetName
                        logger.info(msg)
                        validRecordsDf, erroredRecordsDf, totalFiledQcCount, totalCriticalFiledQcCount, totalValidRecords, totalErrorRecords = executeDqmObj.preIngestionDQMChecks(self.datasetName,sourceDataDf,sourceFileName)
                        msg = "DQM Check Completed"
                        logger.info(msg)
                        msg = "Total QC Failed: " + str(totalFiledQcCount)
                        logger.info(msg)
                        msg = "Total Critical QC Failed: " + str(totalCriticalFiledQcCount)
                        logger.info(msg)
                        msg = "Total Valid Records: " + str(totalValidRecords)
                        logger.info(msg)
                    else:
                        msg = "Error: The Input File: {} has 0 records".format(sourceFileName)
                        logger.error(msg)
                except Exception as e:
                    msg = "Error: " + str(e.__str__())
                    logger.error(msg)
                    raise Exception(e.__str__())
                if fileEmpty == True:
                    self.totalSkippedFileCount = self.totalSkippedFileCount + 1
                    count = 0
                    ingestionStatus = "File Skipped"
                elif (totalCriticalFiledQcCount > 0 and self.skipFile == "Y") or (self.skipFile == "Y" and self.skipFileOnNonCriticalFailure == "Y" and totalFiledQcCount > 0) or totalValidRecords == 0:
                    self.totalSkippedFileCount = self.totalSkippedFileCount + 1
                    count = 0
                    ingestionStatus = "File Skipped"
                elif totalCriticalFiledQcCount > 0 and self.writeBackValidData.upper() == "Y":
                    ingestionStatus = "Ingested Valid Data"
                    sourceDataDf = validRecordsDf
                    sourceDataDf = sourceDataDf.withColumn("source_file_location",lit(sourceFileLocation)).withColumn("processed_time",lit(self.processedTime))
                    # Write Source DF to S3 Target Location
                    sourceDataDf.replace("null","").write.mode("overwrite").option("multiline",True).option("escape","\"").option("encoding", "utf-8").parquet(finalTableLocation)
                    self.updateGlueTable(sourceDataDf,self.targetSchema,self.targetTable,'',self.tablePath,partitionKeys,partitionValues)
                    msg = "Loaded File: " + sourceFileLocation
                    logger.info(msg)
                    count = totalValidRecords
                else:
                    ingestionStatus = "Ingested"
                    # Write Source DF to S3 Target Location
                    sourceDataDf = sourceDataDf.withColumn("source_file_location",lit(sourceFileLocation)).withColumn("processed_time",lit(self.processedTime))
                    sourceDataDf.replace("null","").write.mode("overwrite").option("multiline",True).option("escape","\"").option("encoding", "utf-8").parquet(finalTableLocation)
                    self.updateGlueTable(sourceDataDf,self.targetSchema,self.targetTable,'',self.tablePath,partitionKeys,partitionValues)
                    msg = "Loaded File: " + sourceFileLocation
                    logger.info(msg)
                    count = totalValidRecords
            else:
                # Write Source DF to S3 Target Location
                if fileEmpty == False:
                    sourceDataDf = sourceDataDf.withColumn("source_file_location",lit(sourceFileLocation)).withColumn("processed_time",lit(self.processedTime))
                    sourceDataDf.replace("null","").write.mode("overwrite").option("multiline",True).option("escape","\"").option("encoding", "utf-8").parquet(finalTableLocation)
                    self.updateGlueTable(sourceDataDf,self.targetSchema,self.targetTable,'',self.tablePath,partitionKeys,partitionValues)
                    msg = "Loaded File: " + sourceFileLocation
                    logger.info(msg)
                    count = sourceDataDf.count()
                else:
                    self.totalSkippedFileCount = self.totalSkippedFileCount + 1
                    count = 0
                    ingestionStatus = "File Skipped"
            sourceDataDf.unpersist(True)
            self.updateFileLog(sourcePrefix,sourceFileName,count,ingestionStatus,fileLoadStartTime)
            msg = "Total Record Loaded from file " + sourceFileName + " is: " + str(count)
            logger.info(msg)
            spark.catalog.clearCache()
            return count
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def getSchemaJson(self,schemaLocationKey):
        try:
            if schemaLocationKey.endswith("/"):
                msg = "Schema Location Key is not valid"
                logger.error(msg)
                raise Exception(msg)
            schemaLocationBucket = environmentParams.PLATFORM_ARTIFACTS_S3_BUCKET.format(self.env)
            # Check Schema Location
            msg = "schema_location_key: " + schemaLocationKey
            logger.info(msg)
            if not schemaLocationKey.endswith(".json"):
                msg = "schema_location_key is not a JSON file"
                logger.error(msg)
                raise Exception(msg)
            if schemaLocationKey.startswith("s3://"):
                schemaLocationKey = schemaLocationKey.replace("s3://","").lstrip("/").split("/")[1:]
            else:
                schemaLocationKey = schemaLocationKey.split("/")
                if schemaLocationKey[0] != environmentParams.APPLICATION_NAME:
                    schemaLocationKey.insert(0,environmentParams.APPLICATION_NAME)
                try:
                    if schemaLocationKey[2] != environmentParams.WORKSPACE:
                        if environmentParams.WORKSPACE != None and environmentParams.WORKSPACE != "":
                            schemaLocationKey.insert(2,environmentParams.WORKSPACE)
                except:
                    msg = "Workspace is not Set"
                    logger.warn(msg)
            schemaLocationKey = "/".join(schemaLocationKey)
            # Read the JSON
            try:
                schemaJson = self.s3Obj.getFileContent(schemaLocationBucket,schemaLocationKey).read().decode('utf-8')
                schemaJson = json.loads(schemaJson)
            except Exception as e:
                msg = "Error: " + str(e.__str__()) + "s3://" + schemaLocationBucket + "/" + schemaLocationKey
                logger.error(msg)
                raise Exception(e.__str__())
            return schemaJson
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def readLogTable(self, retry=0, multipleInputLocation=False):
        try:
            while retry <= 10:
                import random
                fileLogTable = "`" + environmentParams.LOG_SCHEMA.replace("`","") + "`.file_log_" + self.applicationName
                if retry > 0:
                    randomSleepTime = random.randint(60,180)
                    msg = "Waiting for {} seconds before retry".format(str(randomSleepTime))
                    logger.info(msg)
                    time.sleep(randomSleepTime)
                    msg = "Error: Unable to read logtable: {}".format(fileLogTable)
                    logger.warn(msg)
                    msg = "Reading {}, retry: {}".format(fileLogTable,retry)
                    logger.info(msg)
                if multipleInputLocation == False:
                    processFilesQuery = "SELECT DISTINCT file_name from {} WHERE dataset_name = '{}'".format(fileLogTable, self.datasetName)
                else:
                    processFilesQuery = "SELECT DISTINCT concat(input_location_prefix,file_name) as file_name from {} WHERE dataset_name = '{}'".format(fileLogTable, self.datasetName)
                msg = "Query for getting processed files: {}".format(processFilesQuery)
                logger.info(msg)
                processedFilesDf = spark.sql(processFilesQuery)
                processedFiles = [str(row.file_name) for row in processedFilesDf.select("file_name").collect()]
                return processedFiles
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            descQuery = "DESC " + fileLogTable
            msg = "DESC Query: " + descQuery
            logger.info(msg)
            try:
                spark.sql(descQuery)
            except Exception as e:
                processedFiles = []
                return processedFiles
            retry = retry + 1
            if retry > 10:
                msg = "Error: Reading log table retry limit exceeded"
                logger.error(msg)
                raise Exception(e.__str__())
            return self.readLogTable(retry)
    def ingestData(self,datasetMasterKey=None):
        try:
            if datasetMasterKey == None:
                datasetMasterKey = self.args['dataset_master_key']
            # Read Ingestion Object Json Config
            if datasetMasterKey.endswith("/"):
                msg = "datasetMasterKey should not be directory"
                logger.error(msg)
                raise Exception(msg)
            if datasetMasterKey.startswith('s3://'):
                datasetMasterKey = datasetMasterKey.lstrip("s3://").split("/")
                datasetMasterBucket = datasetMasterKey[0]
                datasetMasterKey = "/".join(datasetMasterKey[1:])
            else:
                datasetMasterBucket = environmentParams.PLATFORM_ARTIFACTS_S3_BUCKET.format(self.env)
                datasetMasterKey = datasetMasterKey.lstrip("/").split("/")
                if datasetMasterKey[0] != environmentParams.APPLICATION_NAME:
                    datasetMasterKey.insert(0,environmentParams.APPLICATION_NAME)
                try:
                    if datasetMasterKey[2] != environmentParams.WORKSPACE:
                        if environmentParams.WORKSPACE != None and environmentParams.WORKSPACE != "":
                            datasetMasterKey.insert(2,environmentParams.WORKSPACE)
                except:
                    msg = "Workspace is not Set"
                    logger.warn(msg)
                datasetMasterKey = "/".join(datasetMasterKey)
            try:
                datasetMaster = self.s3Obj.getFileContent(datasetMasterBucket,datasetMasterKey).read().decode('utf-8')
                datasetMaster = json.loads(datasetMaster)
            except Exception as e:
                msg = "Error: " + str(e.__str__()) + "s3://" + datasetMasterBucket + "/" + datasetMasterKey
                logger.error(msg)
                raise Exception(e.__str__())

            option = {}
            self.datasetName = datasetMaster.get("dataset_name")
            active = datasetMaster.get("active")
            if active.upper() == "N":
                msg = "Skipping the Process as The Dataset is not Active for Ingestion"
                logger.warn(msg)
                return True

            self.datasetName = datasetMaster.get("dataset_name")
            self.applicationName = environmentParams.APPLICATION_NAME.lower()
            try:
                subApplicationName = environmentParams.SUB_APPLICATION_NAME.lower()
                if subApplicationName != None and subApplicationName != "":
                    self.applicationName = subApplicationName
            except:
                logger.info("Not a Sub Application")
            # get source bucket
            match datasetMaster.get("source_bucket").upper():
                case "RAW_S3_BUCKET":
                    self.sourceS3Bucket = environmentParams.RAW_S3_BUCKET.format(self.env)
                case "SANDBOX_S3_BUCKET":
                    self.sourceS3Bucket = environmentParams.SANDBOX_S3_BUCKET.format(self.env)
                case "PROCESSED_S3_BUCKET":
                    self.sourceS3Bucket = environmentParams.PROCESSED_S3_BUCKET.format(self.env)
                case "CURATED_S3_BUCKET":
                    self.sourceS3Bucket = environmentParams.CURATED_S3_BUCKET.format(self.env)
                case "ARCHIVE_S3_BUCKET":
                    self.sourceS3Bucket = environmentParams.ARCHIVE_S3_BUCKET.format(self.env)
                case _:
                    self.sourceS3Bucket = datasetMaster.get("source_bucket").lstrip("s3://").rstrip("/")
            # get source prefix
            multipleInputLocation = False
            sourcePrefix = datasetMaster.get("source_prefix")
            if type(sourcePrefix) == list:
                multipleInputLocation = True
                if len(sourcePrefix) == 0:
                    msg = "Source Prefix is Empty!!!It should be String or a list of String"
                    logger.error(msg)
            else:
                sourcePrefix = sourcePrefix.strip("/") + "/"
            # get pettern
            self.staticFilePattern = datasetMaster.get("static_file_pattern")
            self.filePattern = datasetMaster.get("source_file_pattern")

            sourceFileList = []
            if self.filePattern == None or self.filePattern == "":
                # If FileName Pattern is Blank or NUll process All thh files
                sourceFileList = self.s3Obj.getValidFileList(self.sourceS3Bucket, sourcePrefix, "", "N")
            elif self.staticFilePattern.upper() == "Y":
                sourceFileList.append(self.filePattern)
            else:
                # match patter and get valid file list
                try:
                    customPattern = datasetMaster.get("custom_pattern")
                    if customPattern == None or customPattern == "":
                        customPattern = "N"
                except Exception as e:
                    customPattern = "N"
                    msg = "Using Default Pattern"
                    logger.info(msg)
                sourceFileList = self.s3Obj.getValidFileList(self.sourceS3Bucket, sourcePrefix, self.filePattern, customPattern)
            # Get File Details
            self.fileType = datasetMaster.get("file_type")

            try:
                self.encoding = datasetMaster.get("encoding")
                if self.encoding != None and self.encoding != "":
                    option.update({"encoding": self.encoding})
            except Exception as e:
                msg = "Using Default Encoding"
                logger.info(msg)

            try:
                self.escape = datasetMaster.get("escape")
                if self.escape != None and self.escape != "":
                    option.update({"escape": self.escape})
            except Exception as e:
                msg = "By Default Escape Character is not Applied"
                logger.info(msg)

            try:
                self.multiline = datasetMaster.get("multiline").lower()
                if self.multiline == None or self.multiline == "":
                    self.multiline = "true"
                if self.multiline == "true":
                    option.update({"multiline": self.multiline})
            except Exception as e:
                msg = "By Default Multiline Option is Set to False"
                logger.info(msg)

            # try:
            #     self.dropMalformedRecords = datasetMaster.get("drop_malformed_records").upper()
            #     if self.dropMalformedRecords == "Y" and self.fileType in ["csv","txt"]:
            #         option.update({"mode": self.dropMalformedRecords})
            # except Exception as e:
            #     msg = "Drop Malformed Records is Set to N"
            #     logger.info(msg)

            match datasetMaster.get("target_bucket").upper():
                case "RAW_S3_BUCKET":
                    self.targetBucket = environmentParams.RAW_S3_BUCKET.format(self.env)
                case "SANDBOX_S3_BUCKET":
                    self.targetBucket = environmentParams.SANDBOX_S3_BUCKET.format(self.env)
                case "PROCESSED_S3_BUCKET":
                    self.targetBucket = environmentParams.PROCESSED_S3_BUCKET.format(self.env)
                case "CURATED_S3_BUCKET":
                    self.targetBucket = environmentParams.CURATED_S3_BUCKET.format(self.env)
                case "ARCHIVE_S3_BUCKET":
                    self.targetBucket = environmentParams.ARCHIVE_S3_BUCKET.format(self.env)
                case _:
                    self.targetBucket = datasetMaster.get("target_bucket").lstrip("s3://").rstrip("/")
            self.targetPrefix = datasetMaster.get("target_prefix").strip("/") + "/"
            self.tablePath = "s3://" + self.targetBucket + "/" + self.targetPrefix
            msg = "Table Path: " + self.tablePath
            logger.info(msg)
            self.targetTable = datasetMaster.get("target_table").upper()
            if len(self.targetTable) > 80:
                msg = "Maximum Length of Table Name allowed is 80 Character"
                logger.info(msg)
                raise Exception(msg)
            match datasetMaster.get("target_schema").upper():
                case "STAGING_SCHEMA":
                    self.targetSchema = environmentParams.STAGING_SCHEMA
                case "PROCESSED_SCHEMA":
                    self.targetSchema = environmentParams.PROCESSED_SCHEMA
                case "REPORTING_SCHEMA":
                    self.targetSchema = environmentParams.REPORTING_SCHEMA
                case "PUBLISH_SCHEMA":
                    self.targetSchema = environmentParams.PUBLISH_SCHEMA
                case "DQ_SCHEMA":
                    self.targetSchema = environmentParams.DQ_SCHEMA
                case "LOG_SCHEMA":
                    self.targetSchema = environmentParams.LOG_SCHEMA
                case "DP_SCHEMA":
                    self.targetSchema = environmentParams.DP_SCHEMA
                case _:
                    self.targetSchema = datasetMaster.get("target_schema")
            self.loadType = datasetMaster.get("load_type").upper()
            try:
                self.archiveFlag = datasetMaster.get("archive").upper()
                if self.archiveFlag == None or self.archiveFlag == "":
                    self.archiveFlag = "N"
            except Exception as e:
                self.archiveFlag = "N"

            if self.loadType == "FULL" and self.archiveFlag == "Y":
                GlueTableObj = GlueTable(self.targetSchema,self.targetTable)
                GlueTableObj.getAndDeletePartitions()

            if self.staticFilePattern.upper() == "N":
                try:
                    msg = "Checking for Files to be Reprocessed for Execution Date" + str(self.executionDate)
                    logger.info(msg)
                    # Lest List of Files Needs to be reprocessed
                    self.reProcessMasterFile = datasetMaster.get("reprocess_master_file_s3_key")
                    if self.reProcessMasterFile == None or self.reProcessMasterFile == "":
                        logger.error("Re-Process Master File S3 Key is Blank")
                    if self.reProcessMasterFile.endswith('/'):
                        msg = "Error: Re-Process Master File S3 Key Should be File not Directory"
                        logger.error(msg)
                        raise Exception(msg)
                    if self.reProcessMasterFile.startswith('s3://'):
                        self.reProcessMasterFile = "/".join(self.reProcessMasterFile.lstrip("s3://").split("/")[1:])
                    else:
                        self.reProcessMasterFile = self.reProcessMasterFile.lstrip("/")

                except Exception as e:
                    logger.error("Re-Process Master File S3 Key not Available")
                    self.reProcessMasterFile = ""
                # Clean Up for Reprocessing
                if self.reProcessMasterFile != "":
                    reprocessObj = reProcessIngestedFile(self.env)
                    listOfFileToBeReProcessed = reprocessObj.prepareForReProcessing(self.reProcessMasterFile,self.datasetName,self.targetSchema,self.targetTable,self.targetBucket,self.targetPrefix,self.executionDate,multipleInputLocation)
                else:
                    listOfFileToBeReProcessed = []
                msg = "Files to be reprocessed: " + str(listOfFileToBeReProcessed)
                logger.info(msg)
                # Get List of Already Processed File List
                try:
                    if type(listOfFileToBeReProcessed) == list:
                        processedFiles = self.readLogTable(0,multipleInputLocation)
                    else:
                        processedFiles = []
                except Exception as e:
                    msg = e.__str__()
                    logger.error(msg)
                    # processedFiles = []
                    # msg = "Log Table Not Available"
                    # logger.info(msg)
                    raise Exception(e)

                msg = "Files available in Source Location: " + str(sourceFileList)
                logger.info(msg)
                msg = "Files Already Processed: " + str(processedFiles)
                logger.info(msg)
                # Prepare List of Files needs to processed in the run
                if len(listOfFileToBeReProcessed) > 0:
                    msg = "Files to be Re-Processed: " + ", ".join(listOfFileToBeReProcessed)
                    logger.info(msg)
                    processedFiles = list(set(processedFiles) - set(listOfFileToBeReProcessed))
                    msg = "Files Already Processed: " + str(processedFiles)
                    logger.info(msg)
                sourceFileList = list(set(sourceFileList) - set(processedFiles))
                msg = "Files Not Available for Processing: " + str(sourceFileList)
                logger.info(msg)
            self.totalFileCount = len(sourceFileList)
            if self.totalFileCount == 0:
                # msg = "Skipping the Process as No File Available for Processing"
                # logger.warn(msg)
                # return True
                msg = "No Files Available for Processing"
                logger.error(msg)
                raise Exception(msg)
            # Get Pre-Ingestion DQM Config
            try:
                self.preIngestionDQMConfig = datasetMaster.get("pre_ingestion_dqm_config")
                self.skipFile = datasetMaster.get("skip_file")
                self.writeBackValidData = datasetMaster.get("write_valid_data")
                if self.skipFile == None or self.skipFile == "":
                    self.skipFile = "N"
                if self.writeBackValidData == None or self.writeBackValidData == "":
                    self.writeBackValidData = "N"
            except:
                msg = "No Pre-Ingestion DQM Config File Available"
                logger.info(msg)
                self.preIngestionDQMConfig = None
                self.skipFile  = "N"
                self.writeBackValidData = "N"

            if self.skipFile == "Y":
                try:
                    self.skipFileOnNonCriticalFailure = datasetMaster.get("skip_file_on_non_critical_failure")
                    if self.skipFileOnNonCriticalFailure == None or self.skipFileOnNonCriticalFailure == "":
                        self.skipFileOnNonCriticalFailure = "N"
                except:
                    self.skipFileOnNonCriticalFailure = "N"
            try:
                self.dropDuplicateRecords = datasetMaster.get("drop_duplicate_records")
                if self.dropDuplicateRecords == None or self.dropDuplicateRecords == "":
                    self.dropDuplicateRecords = "N"
            except:
                self.dropDuplicateRecords = "N"
            # Ingest File
            fileNameLog = []
            rowCount = 0
            match self.fileType.lower():
                case "csv":
                    try:
                        self.delimiter = datasetMaster.get("delimiter")
                        if self.delimiter != None and self.delimiter != "":
                            option.update({"delimiter": self.delimiter})
                        else:
                            option.update({"delimiter": ","})
                    except Exception as e:
                        option.update({"delimiter": ","})
                        msg = "Using Default Delimiter for CSV file (,)"
                        logger.info(msg)

                    try:
                        self.header = datasetMaster.get("header").upper()
                    except Exception as e:
                        msg = "Header is mendatory for file type: " + self.fileType
                        logger.info(msg)
                        raise Exception(msg)
                    if self.header == "TRUE":
                        self.header = "true"
                        option.update({"header": self.header})
                    else:
                        self.header = "false"
                    self.schemaJson = None
                    try:
                        self.inferSchema = datasetMaster.get("infer_schema").upper()
                        msg = "Infer schema Value: " + self.inferSchema
                        logger.info(msg)
                        if self.inferSchema == None or self.inferSchema == "":
                            if self.header == "true":
                                self.inferSchema = "Y"
                            else:
                                msg = "Infer Schema is not set , using default value N"
                                logger.warn(msg)
                                self.inferSchema = "N"
                    except Exception as e:
                        logger.error("Error: " + e.__str__())
                        logger.error("Error occurred while validating Infer Schema")
                        if self.header == "true":
                            self.inferSchema = "Y"
                        else:
                            msg = "Infer Schema is not set , using default value N"
                            logger.warn(msg)
                            self.inferSchema = "N"
                    msg = "Final Infer schema Value: " + self.inferSchema
                    logger.info(msg)
                    if self.inferSchema == "N":
                        try:
                            schemaLocationKey = datasetMaster.get("schema_location_key")
                            if schemaLocationKey == "" or schemaLocationKey == None:
                                msg = "Schema Location Key is Required!!!"
                                logger.error(msg)
                                raise Exception(msg)
                            else:
                                self.schemaJson = self.getSchemaJson(schemaLocationKey)
                                msg = "Reading Dataset using custom schema: {}".format(str(self.schemaJson))
                                logger.info(msg)
                        except:
                            msg = "Error: Schema Location Key is missing!!!"
                            logger.error(msg)
                            raise Exception(msg)
                    for sourcefile in sourceFileList:
                        if type(sourcePrefix) != list:
                            sourceFileLocation = sourcePrefix + sourcefile
                        else:
                            sourceFileLocation = sourcefile
                        count = self.loadStagingTable(self.fileType,sourceFileLocation,option,self.header)
                        fileNameLog.append(sourcefile)
                        rowCount = rowCount + count
                case "excel":
                    self.delimiter = None
                    try:
                        self.header = datasetMaster.get("header").upper()
                    except Exception as e:
                        msg = "Header is mendatory for file type: " + self.fileType
                        logger.info(msg)
                        raise Exception(msg)
                    if self.header == "TRUE":
                        self.header = "true"
                    else:
                        self.header = "false"
                    option.update({"header": self.header})
                    self.schemaJson = None
                    try:
                        self.inferSchema = datasetMaster.get("infer_schema").upper()
                        msg = "Infer schema Value: " + self.inferSchema
                        logger.info(msg)
                        if self.inferSchema == None or self.inferSchema == "":
                            if self.header == "true":
                                self.inferSchema = "Y"
                            else:
                                msg = "Infer Schema is not set , using default value N"
                                logger.warn(msg)
                                self.inferSchema = "N"
                    except Exception as e:
                        logger.error("Error: " + e.__str__())
                        logger.error("Error occurred while validating Infer Schema")
                        if self.header == "true":
                            self.inferSchema = "Y"
                        else:
                            msg = "Infer Schema is not set , using default value N"
                            logger.warn(msg)
                            self.inferSchema = "N"
                    msg = "Final Infer schema Value: " + self.inferSchema
                    logger.info(msg)
                    if self.inferSchema == "N":
                        try:
                            schemaLocationKey = datasetMaster.get("schema_location_key")
                            if schemaLocationKey == "" or schemaLocationKey == None:
                                msg = "Schema Location Key is Required!!!"
                                logger.error(msg)
                                raise Exception(msg)
                            else:
                                self.schemaJson = self.getSchemaJson(schemaLocationKey)
                                msg = "Reading Dataset using custom schema: {}".format(str(self.schemaJson))
                                logger.info(msg)
                        except:
                            msg = "Error: Schema Location Key is missing"
                            logger.error(msg)
                            raise Exception(msg)
                    if self.schemaJson == None:
                        option.update({"inferSchema": "true"})
                    else:
                        option.update({"inferSchema": "false"})

                    # Get data Address
                    self.excelDataAddress = datasetMaster.get("excel_data_address")
                    sheetName = self.excelDataAddress["sheet_name"]
                    startingCellId = self.excelDataAddress["starting_cell_id"]
                    dataAddress = "'{}'!{}".format(sheetName,startingCellId)
                    option.update({"treatEmptyValuesAsNulls": "false"})
                    option.update({"dataAddress": dataAddress})
                    option.update({"es.field.read.empty.as.null": "no"})
                    for sourcefile in sourceFileList:
                        if type(sourcePrefix) != list:
                            sourceFileLocation = sourcePrefix + sourcefile
                        else:
                            sourceFileLocation = sourcefile
                        count = self.loadStagingTable(self.fileType,sourceFileLocation,option,self.header)
                        fileNameLog.append(sourcefile)
                        rowCount = rowCount + count
                case "txt":
                    try:
                        self.delimiter = datasetMaster.get("delimiter")
                        if self.delimiter != None and self.delimiter != "":
                            option.update({"delimiter": self.delimiter})
                        else:
                            msg = "Delimiter is Mandatory for file type: " + self.fileType
                            logger.error(msg)
                            raise Exception(msg)
                    except Exception as e:
                        msg = "Delimiter is Mandatory for file type: " + self.fileType
                        logger.error(msg)
                        raise Exception(msg)
                    try:
                        self.header = datasetMaster.get("header").upper()
                    except Exception as e:
                        msg = "Header is mendatory for file type: " + self.fileType
                        logger.info(msg)
                        raise Exception(msg)
                    if self.header == "TRUE":
                        self.header = "true"
                        option.update({"header": self.header})
                    else:
                        self.header = "false"
                    self.schemaJson = None
                    try:
                        self.inferSchema = datasetMaster.get("infer_schema").upper()
                        msg = "Infer schema Value: " + self.inferSchema
                        logger.info(msg)
                        if self.inferSchema == None or self.inferSchema == "":
                            if self.header == "true":
                                self.inferSchema = "Y"
                            else:
                                msg = "Infer Schema is not set , using default value N"
                                logger.warn(msg)
                                self.inferSchema = "N"
                    except Exception as e:
                        logger.error("Error: " + e.__str__())
                        logger.error("Error occurred while validating Infer Schema")
                        if self.header == "true":
                            self.inferSchema = "Y"
                        else:
                            msg = "Infer Schema is not set , using default value N"
                            logger.warn(msg)
                            self.inferSchema = "N"
                    msg = "Final Infer schema Value: " + self.inferSchema
                    logger.info(msg)
                    if self.inferSchema == "N":
                        try:
                            schemaLocationKey = datasetMaster.get("schema_location_key")
                            if schemaLocationKey == "" or schemaLocationKey == None:
                                msg = "Schema Location Key is Required!!!"
                                logger.error(msg)
                                raise Exception(msg)
                            else:
                                self.schemaJson = self.getSchemaJson(schemaLocationKey)
                                msg = "Reading Dataset using custom schema: {}".format(str(self.schemaJson))
                                logger.info(msg)
                        except:
                            msg = "Error: Schema Location Key is missing"
                            logger.error(msg)
                            raise Exception(msg)
                    # option.update({"header": self.header})
                    for sourcefile in sourceFileList:
                        if type(sourcePrefix) != list:
                            sourceFileLocation = sourcePrefix + sourcefile
                        else:
                            sourceFileLocation = sourcefile
                        count = self.loadStagingTable(self.fileType,sourceFileLocation,option,self.header)
                        fileNameLog.append(sourcefile)
                        rowCount = rowCount + count
                case "json":
                    self.header = None
                    self.delimiter = None
                    try:
                        self.inferSchema = datasetMaster.get("infer_schema").upper()
                    except Exception as e:
                        logger.error("Error: " + e.__str__())
                        logger.error("Error occurred while validating Infer Schema")
                        self.inferSchema = "Y"
                    msg = "Final Infer schema Value: " + self.inferSchema
                    logger.info(msg)
                    if self.inferSchema == "N":
                        try:
                            schemaLocationKey = datasetMaster.get("schema_location_key")
                            if schemaLocationKey == "" or schemaLocationKey == None:
                                msg = "Schema Location Key is Empty!!!"
                                logger.error(msg)
                                raise Exception(msg)
                            else:
                                self.schemaJson = self.getSchemaJson(schemaLocationKey)
                                msg = "Reading Dataset using custom schema: {}".format(str(self.schemaJson))
                                logger.info(msg)
                        except Exception as e:
                            msg = "Error: Schema Location Key is missing"
                            logger.error(msg)
                            raise Exception(msg)
                    # option.update({"header": self.header})
                    for sourcefile in sourceFileList:
                        if type(sourcePrefix) != list:
                            sourceFileLocation = sourcePrefix + sourcefile
                        else:
                            sourceFileLocation = sourcefile
                        count = self.loadStagingTable(self.fileType,sourceFileLocation,option,self.header)
                        fileNameLog.append(sourcefile)
                        rowCount = rowCount + count
                case "parquet":
                    self.delimiter = None
                    for sourcefile in sourceFileList:
                        if type(sourcePrefix) != list:
                            sourceFileLocation = sourcePrefix + sourcefile
                        else:
                            sourceFileLocation = sourcefile
                        count = self.loadStagingTable(self.fileType,sourceFileLocation,option)
                        fileNameLog.append(sourcefile)
                        rowCount = rowCount + count
                case _:
                    msg = "File Type "+self.fileType+" not Supported for Ingestion"
                    logger.error(msg)
                    raise Exception(msg)
            msg = "Updated Staging Table: " + self.targetTable
            logger.info(msg)
            self.updateIngestionLog(fileNameLog,rowCount)
            # Start Archival Process
            if self.loadType in ["FULL", "ROLLING"] and self.archiveFlag == "Y":
                archiveBucket = datasetMaster.get("archive_location_bucket")
                match archiveBucket.upper():
                    case "RAW_S3_BUCKET":
                        archiveBucket = environmentParams.RAW_S3_BUCKET.format(self.env)
                    case "SANDBOX_S3_BUCKET":
                        archiveBucket = environmentParams.SANDBOX_S3_BUCKET.format(self.env)
                    case "PROCESSED_S3_BUCKET":
                        archiveBucket = environmentParams.PROCESSED_S3_BUCKET.format(self.env)
                    case "CURATED_S3_BUCKET":
                        archiveBucket = environmentParams.CURATED_S3_BUCKET.format(self.env)
                    case "ARCHIVE_S3_BUCKET":
                        archiveBucket = environmentParams.ARCHIVE_S3_BUCKET.format(self.env)
                    case _:
                        archiveBucket = datasetMaster.get("source_bucket").lstrip("s3://").rstrip("/")
                archivePrefix = datasetMaster.get("archive_location_prefix")
                archiveObj = archive()
                archiveObj.archiveStaging(self.targetBucket,self.targetPrefix+"execution_date="+self.executionDate+"/",self.cycleId,archiveBucket,archivePrefix)
                msg = "Archived Previous Run Data"
                logger.info(msg)
            else:
                msg = "Skipping Archival Process for " + self.targetTable + " as the load type is not FULL"
                logger.info(msg)

            try:
                from DataFlexor import sendNotification
                if self.preIngestionDQMConfig != None and self.preIngestionDQMConfig != "":
                    msg = "Starting Pre-Ingestion DQM E-Mail Notification Process"
                    logger.info(msg)
                    sendNotificationObj = sendNotification()
                    try:
                        workSpaceName = environmentParams.WORKSPACE
                    except Exception as e:
                        workSpaceName = ""
                    sendNotificationObj.sendNotification(self.datasetName, self.cycleId,"",workSpaceName,True)
                    bucketName = str(environmentParams.SANDBOX_S3_BUCKET).format(self.env)
                    prefix = "temp/"+self.targetTable.lower()+"/"
                    self.s3Obj.cleanUpS3(bucketName,prefix)
            except Exception as e:
                msg = "Error: Unable to send Email Notification"
                logger.error(msg)
                logger.error("Error: " + e.__str__())

            # Future Enhencement if a Check is done in preDQM do not process in post ingestion DQM
            # Integration of POST Ingestion DQMs
            try:
                dqmConfigFiles = datasetMaster.get("dqm_config_s3_key")
            except Exception as e:
                logger.error("Error: " + e.__str__())
                msg = "DQM JSON Config file missing, hence skipping the process"
                logger.info(msg)
                dqmConfigFiles = None

            msg = "DQM JSON Config file location: {}".format(str(dqmConfigFiles))
            logger.info(msg)

            if dqmConfigFiles != None and dqmConfigFiles != "":
                from DataGuardian import executeDqm
                if (self.totalFileCount - self.totalSkippedFileCount) == 0:
                    msg = "All Input Files are skipped by Pre-ingestion DQM! Skipping Post-Ingestrion DQM"
                    logger.info(msg)
                    # zero records ingested then mark the job failed
                    if rowCount == 0:
                        msg = "No Records available for ingestion"
                        logger.error(msg)
                        raise Exception(msg)
                executeDqmObj = executeDqm(dqmConfigFiles,self.env,self.cycleId)
                sourcePath = "s3://{}/{}/{}/{}/".format(self.targetBucket,self.targetPrefix,"execution_date="+str(self.executionDate),"execution_cycle_id="+self.cycleId)
                executeDqmObj.processDQM(self.datasetName,sourcePath)
            else:
                msg = "DQM JSON Config file missing, hence skipping the process"
                logger.info(msg)
                return True
            # Cleanup Temp Files
            try:
                self.s3Obj.cleanUpS3(environmentParams.SANDBOX_S3_BUCKET.format(self.env),'temp/{}/'.format(self.targetTable))
            except:
                msg = "Unable to Clean Temp Files"
                logger.error(msg)
            # zero records ingested then mark the job failed
            if rowCount == 0:
                msg = "No Records available for ingestion"
                logger.error(msg)
                raise Exception(msg)
        except Exception as e:
            logger.error("Error: " + e.__str__())
            # Spark Integration
            try:
                utilsObj = Utils()
                if environmentParams.MWAA_INTEGRATION == False:
                    applicationName = environmentParams.APPLICATION_NAME
                    try:
                        subApplicationName = environmentParams.SUB_APPLICATION_NAME.lower()
                        if subApplicationName != None and subApplicationName != "":
                            applicationName = subApplicationName
                    except:
                        logger.info("Not a Sub Application")
                    utilsObj.triggerSparcUtility(self.processName, self.runId, self.stepName, self.env, applicationName,e.__str__(),self.notify)
            except:
                msg = "Application is MWAA Integrated!!!"
            raise Exception(e.__str__())
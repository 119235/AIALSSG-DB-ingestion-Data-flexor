__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Processed Table Module
   Purpose             :   This module is used for Creating/Updating Processed Layer Tale (E.g. HIST/FACT/AGG/RPT)

      How to Invoke       :   call function processedTable(<S3 Prefix of JSON File for Dataset Parameters>,<Final Pyspark Dataframe That need to written to S3 and Refresh Table>)
   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   Module Created
"""
import sys
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
from DataFlexor import s3Functions,archive,publishCleanup,GlueTable,Utils
from DataFlexorCommonEtl import commonTransformation

class processed:
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
        self.partitionKeys = [
            {
                'name': "execution_date",
                'data_type': "string"
            },
            {
                'name': "execution_cycle_id",
                'data_type': "string"
            }
        ]
        self.partitionValues = {
            "execution_date": str(self.executionDate),
            "execution_cycle_id": str(self.cycleId)
        }
        self.commonTransformationObj = commonTransformation()
        self.logTableSchema = StructType([
                StructField("dataset_name", StringType(), True),
                StructField("application", StringType(), True),
                StructField("table_location", StringType(), True),
                StructField("table_schema", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("processed_table_record_count", IntegerType(), True),
                StructField("publish_table_schema", StringType(), True),
                StructField("publish_table_name", StringType(), True),
                StructField("publish_table_record_count", IntegerType(), True),
                StructField("execution_start_time", StringType(), True),
                StructField("execution_end_time", StringType(), True)
            ])

    def updateGlueTable(self,sourceDataDf,targetSchema,targetTable,tableDescription,finalTableLocation,partitionKeys,partitionValues,archiveFlag=False):
        try:
            # Create/Update Table
            columnList = self.commonTransformationObj.getColumnDataType(sourceDataDf)
            GlueTableObj = GlueTable(targetSchema,targetTable,tableDescription,columnList,finalTableLocation,partitionKeys,partitionValues)
            if archiveFlag == True:
                GlueTableObj.getAndDeletePartitions()
            # Load Partition
            status = GlueTableObj.loadGlueTable()
            if status == True:
                logger.info("Table "+targetTable+" loaded successfully")
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def updateProcessedLog(self):
        try:
            # Prepare Processed log Datafrate amd table
            processedLogSchema = environmentParams.LOG_SCHEMA.replace("`","")
            logTable = "processed_table_log_"+self.applicationName
            logTable = logTable.replace("-","_")

            executionEndTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            processedLogList = [(self.datasetName,self.applicationName,self.processedTablePath,self.processedSchema,self.processedTableName,self.recordCount,self.publishSchema,self.publishTable,self.publishRecordCount, self.processedTime, executionEndTime)]

            processedLogDf = spark.createDataFrame(processedLogList, schema=self.logTableSchema)
            # Write Log to S3 location
            logTablePath = "s3://" + environmentParams.PROCESSED_S3_BUCKET.format(self.env) + "/DataFlexor/" + self.applicationName + "/" + logTable + "/"
            logTablePathFinal = logTablePath + "execution_date=" + self.executionDate + "/execution_cycle_id=" + str(self.cycleId) + "/"
            processedLogDf.write.mode("overwrite").parquet(logTablePathFinal)
            self.updateGlueTable(processedLogDf,processedLogSchema,logTable,'',logTablePath,self.partitionKeys,self.partitionValues)
            msg = "Updated Log Table: " + logTable
            logger.info(msg)
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())

    def createTablePath(self, partitionKeys, partitionValues):
        try:
            tableSubPath = ""
            for k in partitionKeys:
                if tableSubPath == "":
                    tableSubPath = k["name"] + "=" + partitionValues[k["name"]] + "/"
                else:
                    tableSubPath = tableSubPath + k["name"] + "=" + partitionValues[k["name"]] + "/"
            return tableSubPath
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def loadProcessedTable(self,resultDf):
        try:
            # Prepare Final S3 Location
            partitionKeys = [
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
                "execution_date": str(self.executionDate),
                "execution_cycle_id": str(self.cycleId)
            }
            if len(self.userPartitionKeyValue) > 0:
                for ky,vl in self.userPartitionKeyValue.items():
                    partitionKeys.append({
                        'name': ky,
                        'data_type': "string"
                    })
                    partitionValues[ky] = str(vl)
            tableSubPath = self.createTablePath(partitionKeys,partitionValues)
            finalTableLocation = self.processedTablePath + tableSubPath

            msg = "Started Loading Processed Table"
            logger.info(msg)
            # Write Source DF to S3 Target Location
            resultDf.write.mode("overwrite").parquet(finalTableLocation)
            if self.archiveFlag == "Y":
                archiveFlag = True
            else:
                archiveFlag = False
            self.updateGlueTable(resultDf,self.processedSchema,self.processedTableName,'',self.processedTablePath,partitionKeys,partitionValues,archiveFlag)
            msg = self.processedTableName + " Table Loaded Successfully"
            logger.info(msg)
            return True
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def loadPublishTable(self):
        try:
            # Prepare Final S3 Location
            partitionKeys = [
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
                "execution_date": str(self.executionDate),
                "execution_cycle_id": str(self.cycleId)
            }
            additionalWhere = ''
            if len(self.userPartitionKeyValue) > 0:
                for ky,vl in self.userPartitionKeyValue.items():
                    partitionKeys.append({
                        'name': ky,
                        'data_type': "string"
                    })
                    partitionValues[ky] = str(vl)
                    additionalWhere = " AND {} = '{}'".format(ky,vl)
            tableSubPath = self.createTablePath(partitionKeys,partitionValues)
            msg = "Started Loading Processed Table"
            logger.info(msg)

            if additionalWhere != '':
                publishQuery = "SELECT * FROM {}.{} WHERE execution_cycle_id = '{}' {}".format("`"+self.processedSchema.replace("`","")+"`",self.processedTableName, self.cycleId,additionalWhere)
            else:
                publishQuery = "SELECT * FROM {}.{} WHERE execution_cycle_id = '{}'".format("`"+self.processedSchema.replace("`","")+"`",self.processedTableName, self.cycleId)
            publishDf = spark.sql(publishQuery)
            dropColumns = ["execution_cycle_id","execution_date"]
            if len(self.userPartitionKeyValue) > 0:
                dropColumns = dropColumns + list(self.userPartitionKeyValue.keys())
            publishDf = publishDf.drop(*dropColumns)
            finalTableLocation = self.publishTablePath + tableSubPath
            publishDf.write.mode("overwrite").parquet(finalTableLocation)

            if self.archiveFlag == "Y":
                archiveFlag = True
            else:
                archiveFlag = False
            self.updateGlueTable(publishDf,self.publishSchema,self.publishTable,'',self.publishTablePath,partitionKeys,partitionValues,archiveFlag)
            msg = self.publishTable + " Table Loaded Successfully"
            logger.info(msg)
            return True
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def executeSqlScript(self,sqlScriptPath):
        try:
            # Prepare Final S3 Location
            if sqlScriptPath.startswith("s3://"):
                sqlScriptPath = sqlScriptPath.split("/")
                sqlScriptPathBucket = sqlScriptPath[0]
                sqlScriptPath = "/".join(sqlScriptPath[1:])
            else:
                sqlScriptPathBucket = environmentParams.PLATFORM_ARTIFACTS_S3_BUCKET.format(self.env)
                sqlScriptPath = sqlScriptPath.lstrip("/").split("/")
                if sqlScriptPath[0] != environmentParams.APPLICATION_NAME:
                    sqlScriptPath.insert(0,environmentParams.APPLICATION_NAME)
                try:
                    if sqlScriptPath[2] != environmentParams.WORKSPACE:
                        if environmentParams.WORKSPACE != None and environmentParams.WORKSPACE != "":
                            sqlScriptPath.insert(2,environmentParams.WORKSPACE)
                except:
                    msg = "Workspace is not Set"
                    logger.warn(msg)
                sqlScriptPath = "/".join(sqlScriptPath)
            rawQuery = self.s3Obj.getFileContent(sqlScriptPathBucket,sqlScriptPath)
            if rawQuery == None or rawQuery == "":
                msg = "The SQL Script is Blank: s3://" + sqlScriptPathBucket + sqlScriptPath
                logger.error(msg)
                raise Exception(msg)
            query = ""
            for line in rawQuery.iter_lines():
                line = line.decode('utf-8')
                query = query + line.replace("\r","") + "\n"
            msg = "Query Fetched from SQL Script: " + str(query)
            logger.info(msg)
            msg = "Executin Query: " + str(query)
            logger.info(msg)
            resultDf = spark.sql(query)
            msg = "Query Executed"
            logger.info(msg)
            return resultDf
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())
    def processedTable(self,datasetMasterKey=None,resultDf=None, userPartitionKeyValue={}):
        try:
            if datasetMasterKey == None:
                datasetMasterKey = self.args['dataset_master_key']
            self.userPartitionKeyValue = userPartitionKeyValue
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

            self.datasetName = datasetMaster.get("dataset_name")
            active = datasetMaster.get("active")
            if active.upper() == "N":
                msg = "Skipping the Dataset as The Dataset is not Active for Processed"
                logger.warn(msg)
                return True

            self.applicationName = environmentParams.APPLICATION_NAME.lower()
            try:
                subApplicationName = environmentParams.SUB_APPLICATION_NAME.lower()
                if subApplicationName != None and subApplicationName != "":
                    self.applicationName = subApplicationName
            except:
                logger.info("Not a Sub Application")

            match datasetMaster.get("processed_location_bucket").upper():
                case "RAW_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.RAW_S3_BUCKET.format(self.env)
                case "SANDBOX_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.SANDBOX_S3_BUCKET.format(self.env)
                case "PROCESSED_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.PROCESSED_S3_BUCKET.format(self.env)
                case "CURATED_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.CURATED_S3_BUCKET.format(self.env)
                case "ARCHIVE_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.ARCHIVE_S3_BUCKET.format(self.env)
                case "PUBLISH_S3_BUCKET":
                    self.processedLocationBucket = environmentParams.PUBLISH_S3_BUCKET.format(self.env)
                case _:
                    self.processedLocationBucket = datasetMaster.get("processed_location_bucket").lstrip("s3://").rstrip("/")

            self.processedLocationPrefix = datasetMaster.get("processed_location_prefix").strip("/") + "/"
            self.processedTablePath = "s3://" + self.processedLocationBucket + "/" + self.processedLocationPrefix

            match datasetMaster.get("processed_schema").upper():
                case "STAGING_SCHEMA":
                    self.processedSchema = environmentParams.STAGING_SCHEMA
                case "PROCESSED_SCHEMA":
                    self.processedSchema = environmentParams.PROCESSED_SCHEMA
                case "REPORTING_SCHEMA":
                    self.processedSchema = environmentParams.REPORTING_SCHEMA
                case "PUBLISH_SCHEMA":
                    self.processedSchema = environmentParams.PUBLISH_SCHEMA
                case "DQ_SCHEMA":
                    self.processedSchema = environmentParams.DQ_SCHEMA
                case "LOG_SCHEMA":
                    self.processedSchema = environmentParams.LOG_SCHEMA
                case "DP_SCHEMA":
                    self.processedSchema = environmentParams.DP_SCHEMA
                case _:
                    self.processedSchema = datasetMaster.get("processed_schema")

            self.processedTableName = datasetMaster.get("processed_table")
            if len(self.processedTableName) > 80:
                msg = "Maximum Length of Table Name alowed is 80 Character"
                logger.info(msg)
                raise Exception(msg)
            try:
                self.archiveFlag = datasetMaster.get("archive").upper()
            except Exception as e:
                self.archiveFlag = "N"

            publishFlag = datasetMaster.get("publish_flag").upper()

            if resultDf == None:
                logger.info("Result Dataframe is Empty!!! Starting process for Executing SQL Script")
                try:
                    # If the resultDf is not passed search for SQL file
                    sqlScriptPath = datasetMaster.get("sql_script_path")
                    if sqlScriptPath == None or sqlScriptPath == "":
                        msg = "No SQL Script Found"
                        logger.error(msg)
                        raise Exception(msg)
                    elif not sqlScriptPath.endswith(".sql"):
                        msg = "SQL Script Path Should be a SQL (*.sql) file"
                        logger.error(msg)
                        raise Exception(msg)
                    resultDf = self.executeSqlScript(sqlScriptPath)
                except Exception as e:
                    logger.error("Error: " + e.__str__())
                    raise Exception(e.__str__())
            else:
                # Convert Column Names to Lowercase
                logger.info("Convert Column Names to Lowercase")
                resultDf = self.commonTransformationObj.converColumnNameToLowecase(resultDf)
            self.recordCount = resultDf.count()
            self.loadProcessedTable(resultDf)
            resultDf.unpersist(True)
            msg = "Updated Processed Table: " + self.processedTableName
            logger.info(msg)
            msg = "Total Records Loaded: " + str(self.recordCount)
            logger.info(msg)

            # Check publish table flag
            if publishFlag == "Y":
                match datasetMaster.get("publish_schema").upper():
                    case "STAGING_SCHEMA":
                        self.publishSchema = environmentParams.STAGING_SCHEMA
                    case "PROCESSED_SCHEMA":
                        self.publishSchema = environmentParams.PROCESSED_SCHEMA
                    case "REPORTING_SCHEMA":
                        self.publishSchema = environmentParams.REPORTING_SCHEMA
                    case "PUBLISH_SCHEMA":
                        self.publishSchema = environmentParams.PUBLISH_SCHEMA
                    case "DQ_SCHEMA":
                        self.publishSchema = environmentParams.DQ_SCHEMA
                    case "LOG_SCHEMA":
                        self.publishSchema = environmentParams.LOG_SCHEMA
                    case "DP_SCHEMA":
                        self.publishSchema = environmentParams.DP_SCHEMA
                    case _:
                        self.publishSchema = datasetMaster.get("publish_schema")

                self.publishTable = datasetMaster.get("publish_table")
                if len(self.publishTable) > 80:
                    msg = "Maximum Length of Table Name alowed is 80 Character"
                    logger.info(msg)
                    raise Exception(msg)

                match datasetMaster.get("publish_location_bucket").upper():
                    case "PUBLISH_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.PUBLISH_S3_BUCKET.format(self.env)
                    case "RAW_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.RAW_S3_BUCKET.format(self.env)
                    case "SANDBOX_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.SANDBOX_S3_BUCKET.format(self.env)
                    case "PROCESSED_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.PROCESSED_S3_BUCKET.format(self.env)
                    case "CURATED_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.CURATED_S3_BUCKET.format(self.env)
                    case "ARCHIVE_S3_BUCKET":
                        self.publishLocationBucket = environmentParams.ARCHIVE_S3_BUCKET.format(self.env)
                    case _:
                        self.publishLocationBucket = datasetMaster.get("publish_location_bucket").lstrip("s3://").rstrip("/")
                self.publishLocationPrefix = datasetMaster.get("publish_location_prefix").strip("/") + "/"
                self.publishTablePath = "s3://" + self.publishLocationBucket + "/" + self.publishLocationPrefix

                publishCleanupObj = publishCleanup()
                if len(self.userPartitionKeyValue) > 0:
                    try:
                        publishCycleIdDist = spark.sql("SELECT DISTINCT 1 FROM {}.{} WHERE execution_cycle_id = '{}'".format(self.publishSchema,self.publishTable,self.cycleId)).count()
                        if publishCycleIdDist != 1:
                            publishCleanupObj.publishCleanup(self.publishLocationBucket,self.publishLocationPrefix)
                    except Exception as e:
                        msg = "Publish Table doesn't exists"
                        logger.warn(msg)
                else:
                    publishCleanupObj.publishCleanup(self.publishLocationBucket,self.publishLocationPrefix)
                msg = "Loading Publish Table"
                logger.info(msg)
                self.loadPublishTable()
                self.publishRecordCount = self.recordCount
                msg = "Total Records Loaded in Publish Table: " + str(self.publishRecordCount)
                logger.info(msg)
            else:
                self.publishSchema = None
                self.publishTable = None
                self.publishRecordCount = 0

            msg = "Updating Log Table"
            logger.info(msg)
            self.updateProcessedLog()

            # Start Archival Process
            if self.archiveFlag == "Y":
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
                        archiveBucket = archiveBucket.lstrip("s3://").rstrip("/")
                archivePrefix = datasetMaster.get("archive_location_prefix")
                archiveObj = archive()
                archiveObj.archiveProcessedTableData(self.processedLocationBucket,self.processedLocationPrefix+"execution_date="+self.executionDate+"/",self.cycleId,archiveBucket,archivePrefix)
                msg = "Archived Previous Run Data"
                logger.info(msg)
            else:
                msg = "Skipping Archival Process for " + self.processedTableName + " as the load type is not FULL"
                logger.info(msg)
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
                executeDqmObj = executeDqm(dqmConfigFiles,self.env)
                sourcePath = "s3://{}/{}/{}/{}/".format(self.processedLocationBucket,self.processedLocationPrefix,"execution_date="+str(self.executionDate),"execution_cycle_id="+self.cycleId)
                executeDqmObj.processDQM(self.datasetName,sourcePath)
            else:
                msg = "DQM JSON Config file missing, hence skipping the process"
                logger.info(msg)
            try:
                self.s3Obj.cleanUpS3(environmentParams.SANDBOX_S3_BUCKET.format(self.env),'temp/{}/'.format(self.processedTableName))
            except:
                msg = "Unable to Clean Temp Files"
                logger.error(msg)
            return True
        except Exception as e:
            logger.error("Error: " + e.__str__())
            # Spark Integration
            try:
                utilsObj = Utils()
                if environmentParams.MWAA_INTEGRATION == False:
                    applicationName = environmentParams.APPLICATION_NAME.lower()
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
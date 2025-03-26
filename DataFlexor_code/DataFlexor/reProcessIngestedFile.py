__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Reprocess Already Ingested File Module
   Purpose             :   This Module is used for Clean-Up Activity to Re-Process Already Ingested File

   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   New Function added
"""
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
from DataFlexor import s3Functions

class reProcessIngestedFile:
    def __init__(self,env):
        self.env = env
        self.s3Obj = s3Functions()
        self.applicationName = environmentParams.APPLICATION_NAME.lower()

    def readReProcessMasterFile(self,fileLocationKey):
        try:
            msg= "Reading Re-Process Master File: s3://" + environmentParams.RAW_S3_BUCKET.format(self.env) + "/" + fileLocationKey
            logger.info(msg)
            reProcessMaster = self.s3Obj.getFileContent(environmentParams.RAW_S3_BUCKET.format(self.env),fileLocationKey).read().decode('utf-8')
            reProcessMaster = json.loads(reProcessMaster)
            return reProcessMaster
        except Exception as e:
            logger.error("Error: " + e.__str__())
            return False
    def ingestionTableCleanup(self,targetSchema,targetTableName,targetTableBucket,targetTablePrefix,listOfFileToBeReProcessed):
        try:
            # Prepare String for Where Clause
            listOfFileToBeReProcessedNew = []
            if type(listOfFileToBeReProcessed) == list:
                inCond = ""
                for file in listOfFileToBeReProcessed:
                    if type(file) == dict:
                        fileName = file["name"]
                        if type(fileName) == list:
                            for f in fileName:
                                if inCond == "":
                                    inCond = "'" + f + "'"
                                else:
                                    inCond = inCond + ",'" + f + "'"
                        else:
                            inCond = "'" + fileName + "'"
                    elif type(file) == str:
                        fileName = file
                        inCond = "'" + fileName + "'"
                inCond = "(" + inCond + ")"
                query = "SELECT DISTINCT execution_date, execution_cycle_id, source_file_name FROM `{}`.{} WHERE source_file_name IN {}".format(targetSchema.replace("`",""),targetTableName,inCond)
            elif listOfFileToBeReProcessed.lower() == "all":
                query = "SELECT DISTINCT execution_date, execution_cycle_id, source_file_name FROM `{}`.{}".format(targetSchema.replace("`",""),targetTableName)

            msg = "Re-Processing Query: " + query
            logger.info(msg)
            rePrecessList = spark.sql(query)
            if type(listOfFileToBeReProcessed) == list:
                for file in listOfFileToBeReProcessed:
                    if type(file) == dict:
                        if type(file["name"]) == list:
                            for f in file["name"]:
                                fileKey = file["prefix"].lstrip("/").rstrip("/") + "/" + f
                                listOfFileToBeReProcessedNew.append(fileKey)
                        else:
                            fileKey = file["prefix"].lstrip("/").rstrip("/")  + "/" + file["name"]
                            listOfFileToBeReProcessedNew.append(fileKey)
                    elif type(file) == str:
                        listOfFileToBeReProcessedNew.append(file)
            if rePrecessList.count() > 0:
                for row in rePrecessList.rdd.toLocalIterator():
                    cleanUpPrefix = targetTablePrefix + "execution_date=" + row["execution_date"] + "/execution_cycle_id=" + row["execution_cycle_id"] + "/" + "source_file_name=" + row["source_file_name"] + "/"
                    # listOfFileToBeReProcessedNew.append(row["source_file_name"])
                    # Clean Up
                    self.s3Obj.cleanUpS3(targetTableBucket,cleanUpPrefix)
            msg = "Cleanup Completed for Files: " + str(listOfFileToBeReProcessedNew)
            logger.info(msg)
            return listOfFileToBeReProcessedNew
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def prepareForReProcessing(self,fileLocationKey,datsetName,targetSchema,targetTableName,targetTableBucket,targetTablePrefix,executionDate, multipleInputLocation=False):
        try:
            reProcessMaster = self.readReProcessMasterFile(fileLocationKey)
            if reProcessMaster != False:
                try:
                    listOfFileToBeReProcessed = reProcessMaster.get(executionDate)[datsetName]['file_name']
                    msg = "Reprocess String: " + str(listOfFileToBeReProcessed)
                    logger.info(msg)
                    if len(listOfFileToBeReProcessed) == 0:
                        logger.error("No Files Available for Re-Processing")
                    else:
                        listOfFileToBeReProcessed = self.ingestionTableCleanup(targetSchema,targetTableName,targetTableBucket,targetTablePrefix,listOfFileToBeReProcessed)
                        msg = "File Ready to be Reprocessed: " + str(listOfFileToBeReProcessed)
                        logger.info(msg)
                        # if type(reProcessMaster.get(executionDate)[datsetName]['file_name']) != list and len(listOfFileToBeReProcessed) == 0:
                        if type(reProcessMaster.get(executionDate)[datsetName]['file_name']) == str:
                            if reProcessMaster.get(executionDate)[datsetName]['file_name'].lower() == "all":
                                listOfFileToBeReProcessed = "all"
                            else:
                                listOfFileToBeReProcessed = []
                        else:
                            # Check if skipped files needs to be reprocessed
                            try:
                                includeSkippedFiles = reProcessMaster.get(executionDate)[datsetName]['includes_skipped_files'].upper()
                            except Exception as e:
                                includeSkippedFiles = 'N'
                            if includeSkippedFiles == 'Y':
                                fileLogTable = "file_log_"+self.applicationName
                                fileLogTable = fileLogTable.replace("-","_")
                                fileLogTable = "`" + environmentParams.LOG_SCHEMA.replace("`","") + "`." + fileLogTable
                                if multipleInputLocation == False:
                                    skippedFilesDf = spark.sql("SELECT DISTINCT file_name from {} WHERE dataset_name = '{}' AND ingestion_status = 'File Skipped'".format(fileLogTable, datsetName))
                                else:
                                    skippedFilesDf = spark.sql("SELECT DISTINCT concat(input_location_prefix,file_name) as file_name from {} WHERE dataset_name = '{}' AND ingestion_status = 'File Skipped'".format(fileLogTable, datsetName))
                                skippedFiles = [str(row.file_name) for row in skippedFilesDf.select("file_name").collect()]
                                if len(skippedFiles) > 0:
                                    listOfFileToBeReProcessed = listOfFileToBeReProcessed + skippedFiles
                except Exception as e:
                    logger.error("No Files Available for Re-Processing")
                    listOfFileToBeReProcessed = []
            else:
                logger.error("The specified key does not exist")
                listOfFileToBeReProcessed = []
            return listOfFileToBeReProcessed
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
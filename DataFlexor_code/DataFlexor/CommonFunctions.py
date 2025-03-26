__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Common Function Library
   Purpose             :   This module will provide commol function that can be used accross all the ETL script in Glue

   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   New Function added
"""
import sys
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import base64
import json
import requests
import environmentParams as environmentParams
try:
    if environmentParams.PLATFORM_NAME.lower() == "glue":
        from awsglue.context import GlueContext
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from pyspark.sql.types import *
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        logger = glueContext.get_logger()
    elif environmentParams.PLATFORM_NAME.lower() == "databricks":
        from databricks.sdk.runtime import *
        from pyspark.sql import *
        from pyspark.sql.types import *
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
    from pyspark.sql.types import *
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()

from pyspark.sql.functions import explode,explode_outer,regexp_replace,udf,lit,col,when,unix_timestamp,to_timestamp,date_format,coalesce
from DataFlexor import patternValidator,SparcUtility
from DataFlexor import loadNotification, GlueTable

class s3Functions:
    def __init__(self):
        # create S3 Client
        self.s3_client = boto3.client('s3')
        self.s3_client_Clean = boto3.resource('s3')
        self.s3_client_v2 = self.s3_client_Clean.meta.client

    def cleanUpS3(self,bucketName,prefix):
        # Delete all versions of objects once the file is moved to other S3 location
        logger.info("Clean up started")
        try:
            msg = "Clean Up Bucket Name: " + bucketName
            logger.info(msg)
            msg = "Clean Up Prefix: " + prefix
            logger.info(msg)
            bucket = self.s3_client_Clean.Bucket(bucketName)
            if prefix[-1] == '/':
                if prefix is None:
                    bucket.object_versions.delete()
                else:
                    bucket.object_versions.filter(Prefix=prefix).delete()
            else:
                for version in bucket.object_versions.all():
                    if version.object_key == prefix:
                        version.delete()
            logger.info("Clean up Completed")
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def createEmptyS3File(self,bucketName,key):
        try:
            self.s3_client.put_object(
                Bucket=bucketName,
                Key=key
            )
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def getFileContent(self,bucketName,key):
        logger.info("Strating function to get content of file: s3://"+bucketName+"/"+key)
        try:
            source_response = self.s3_client.get_object(Bucket=bucketName,Key=key)
            return source_response['Body']
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def listS3Files(self,bucketName,prefix):
        # This function will return file name as key in a S3 location
        try:
            continuationToken = ""
            contents = []
            while continuationToken != None:
                if continuationToken == "":
                    result = self.s3_client.list_objects_v2(
                        Bucket=bucketName,
                        Prefix=prefix
                    )
                else:
                    result = self.s3_client.list_objects_v2(
                        Bucket=bucketName,
                        Prefix=prefix,
                        ContinuationToken = continuationToken
                    )
                try:
                    continuationToken = result["NextContinuationToken"]
                except Exception as e:
                    continuationToken = None
                contents = contents + result['Contents']
            # result = self.s3_client.list_objects_v2(Bucket=bucketName, Prefix=prefix)
            return contents
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False

    def listS3Directories(self,bucketName,prefix):
        # This function will return file name as key in a S3 location
        try:
            logger.warn("List files in S3 location s3://" + bucketName + "/" + prefix)
            continuationToken = ""
            prefixList = []
            while continuationToken != None:
                if continuationToken == "":
                    result = self.s3_client.list_objects_v2(
                        Bucket=bucketName,
                        Prefix=prefix,
                        Delimiter='/'
                    )
                else:
                    result = self.s3_client.list_objects_v2(
                        Bucket=bucketName,
                        Prefix=prefix,
                        Delimiter='/',
                        ContinuationToken = continuationToken
                    )
                try:
                    continuationToken = result["NextContinuationToken"]
                except Exception as e:
                    continuationToken = None
                result = result.get('CommonPrefixes')
                prefixs = []
                if result == None:
                    continuationToken = None
                    continue
                for p in result:
                    prefixs.append(p.get('Prefix'))
                    logger.warn("Fetched Prefix " + p.get('Prefix'))
                prefixList = prefixList + prefixs
            # result = self.s3_client.list_objects_v2(Bucket=bucketName, Prefix=prefix)
            return prefixList
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False

    def copyS3File(self,sourceBucket,sourceKey,destinationBucket,destinationKey):
        # Copy single file from one S3 location ot another
        try:
            copy_source = {'Bucket': sourceBucket, 'Key': sourceKey}
            logger.info(str(copy_source))
            self.s3_client.copy_object(Bucket = destinationBucket, CopySource = copy_source, Key = destinationKey)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False
    def deleteS3File(self,bucketName,key):
        try:
            logger.info("Starting Delete File S3://" + bucketName + '/' + key)
            response = self.s3_client.delete_object(Bucket = bucketName, Key = key)
            if response:
                logger.info("Deleteted File S3://" + bucketName + '/' + key)
                # self.cleanUpS3(bucketName,key)
                return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False
    def moves3File(self,sourceBucket,sourceKey,destinationBucket,destinationKey):
        try:
            copy_response = self.copyS3File(sourceBucket,sourceKey,destinationBucket,destinationKey)
            if copy_response == False:
                message = "Unable to Copy file: s3://" + sourceBucket+"/"+sourceKey
                logger.error(message)
                return False
            else:
                response = self.deleteS3File(sourceBucket,sourceKey)
                if response == False:
                    logger.error("Unable to move file: s3://" + sourceBucket+"/"+sourceKey)
                    return False
                else:
                    logger.info("File to moved from s3://" + sourceBucket+"/"+sourceKey + " to s3://" + destinationBucket + "/" + destinationKey)
                    # self.cleanUpS3(sourceBucket,sourceKey)
                    return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            return False
    def copyS3File_v2(self,sourceBucket,sourceKey,destinationBucket,destinationKey):
        # Copy single file from one S3 location ot another
        try:
            logger.info("Starting: Copy File greater then 5 GB")
            copy_source = {'Bucket': sourceBucket, 'Key': sourceKey}
            logger.info(str(copy_source))
            self.s3_client_v2.copy(copy_source,destinationBucket,destinationKey)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False
    def moves3File_v2(self,sourceBucket,sourceKey,destinationBucket,destinationKey):
        try:
            logger.info("Starting: Moving Files greater then 5 GB")
            copy_response = self.copyS3File_v2(sourceBucket,sourceKey,destinationBucket,destinationKey)
            if copy_response == False:
                message = "Unable to Copy file: s3://" + sourceBucket+"/"+sourceKey
                logger.error(message)
                return False
            else:
                response = self.deleteS3File(sourceBucket,sourceKey)
                if response == False:
                    logger.error("Unable to move file: s3://" + sourceBucket+"/"+sourceKey)
                    return False
                else:
                    logger.info("File to moved from s3://" + sourceBucket+"/"+sourceKey + " to s3://" + destinationBucket + "/" + destinationKey)
                    # self.cleanUpS3(sourceBucket,sourceKey)
                    return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            return False
    def deleteS3Directory(self,bucketName,prefix):
        # get list of files
        try:
            logger.info("Starting Delete Directory")
            response = self.listS3Files(bucketName,prefix)
            if (response == False) or (response == None):
                return False
            logger.info(str(response))
            for file in response:
                key = file['Key']
                message = "File to be Deleted: s3://"+bucketName+"/"+key
                logger.info(message)
                response = self.deleteS3File(bucketName,key)
                logger.info(str(response))
                if response == False:
                    logger.error("Unable to delete file: s3://" + bucketName+"/"+key)
                    return False
                else:
                    logger.info("Directory Deleted")
                    # raise Exception("Unable to delete file: s3://" + bucketName+"/"+key)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False
    def copyS3Directory(self,sourceBucket,sourcePrefix,destinationBucket,destinationPrefix):
        # Copy S3 directory from one location to another
        try:
            response = self.listS3Files(sourceBucket,sourcePrefix)
            for file in response:
                sourceKey = file['Key']
                logger.info("Source Key: " + sourceKey)
                destinationKey = destinationPrefix + file['Key'].split("/")[-1]
                logger.info("Destination Key: " + destinationKey)
                copy_response = self.copyS3File(sourceBucket,sourceKey,destinationBucket,destinationKey)
                if copy_response == False:
                    message = "Unable to Copy file: s3://" + sourceBucket+"/"+sourceKey
                    logger.error(message)
                    return False
                    # raise Exception("Unable to Copy file: s3://" + sourceBucket+"/"+sourceKey)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())
    def moveS3Directory(self,sourceBucket,sourcePrefix,destinationBucket,destinationPrefix):
        # Move S3 directory from one location to another
        logger.info("Starting Copying Files")
        logger.info("Source Lication: s3://" + sourceBucket + "/" + sourcePrefix)
        logger.info("Destination Lication: s3://" + destinationBucket + "/" + destinationPrefix)
        try:
            response = self.copyS3Directory(sourceBucket,sourcePrefix,destinationBucket,destinationPrefix)
            if response == True:
                # Delete the Source S3 location
                logger.info("Calling Function to Delete Directory: s3://" + sourceBucket+"/"+sourcePrefix)
                delete_response = self.deleteS3Directory(sourceBucket,sourcePrefix)
                if delete_response == True:
                    self.cleanUpS3(sourceBucket,sourcePrefix)
                    return True
                else:
                    return False
            else:
                message = "Unable to Move file: s3://" + sourceBucket+"/"+sourcePrefix
                logger.error(message)
                # raise Exception("Unable to Move file: s3://" + sourceBucket+"/"+sourcePrefix)
                return False
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            # raise Exception(e.__str__())
            return False
    def getValidFileList(self, bucketName, prefix, filePattern, customPattern):
        try:
            availableFiles = []
            if type(prefix) == list:
                multiPrefix = True
            else:
                multiPrefix = False
            if type(prefix) == str:
                prefix = [prefix]
            for prefixKey in prefix:
                prefixKey = prefixKey.strip("/") + "/"
                msg = "Input Prefix is: " + prefixKey
                logger.info(msg)
                msg = "Source Location: s3://{}/{}".format(bucketName,prefixKey)
                logger.info(msg)
                msg = "File pattern is: " + filePattern
                logger.info(msg)
                s3FileList = self.listS3Files(bucketName,prefixKey)
                if type(s3FileList) != list or len(s3FileList) == 0:
                    msg = "Input Location: s3://{}/{} Doesn't Exists".format(bucketName,prefixKey)
                    logger.error(msg)
                    raise Exception(msg)
                if filePattern != None and filePattern != "":
                    if customPattern.upper() == "Y":
                        customPatternFlag = True
                    else:
                        customPatternFlag = False
                    if (s3FileList == False) or (s3FileList == None):
                        msg = "No Files Available in Source S3 location"
                        logger.error(msg)
                    patternValidatorObj = patternValidator()
                    for file in s3FileList:
                        fileKey = file["Key"]
                        # Get File Name
                        finaleName = fileKey.split("/")[-1]
                        if finaleName != None or finaleName != "":
                            patternMatched = patternValidatorObj.patternValidator(source=finaleName, pattern=filePattern, customPattern = customPatternFlag)
                            if patternMatched == True:
                                logger.info("Pattern is Valid")
                                if multiPrefix == False:
                                    availableFiles.append(finaleName)
                                else:
                                    availableFiles.append(prefixKey + finaleName)
                            else:
                                logger.info("Pattern is not Valid")
                else:
                    for file in s3FileList:
                        fileKey = file["Key"]
                        # Get File Name
                        finaleName = fileKey.split("/")[-1]
                        if finaleName != None and finaleName != "":
                            if multiPrefix == False:
                                availableFiles.append(finaleName)
                            else:
                                availableFiles.append(prefixKey + finaleName)
                if len(availableFiles) == 0:
                    msg = "No Files Available in Source S3 location"
                    logger.error(msg)
                    raise Exception(msg)
            return availableFiles
            # msg = "Input Prefix is: " + prefix
            # logger.info(msg)
            # msg = "Source Location: s3://{}/{}".format(bucketName,prefix)
            # logger.info(msg)
            # msg = "File pattern is: " + filePattern
            # logger.info(msg)
            # s3FileList = self.listS3Files(bucketName,prefix)
            # if type(s3FileList) != list or len(s3FileList) == 0:
            #     msg = "Input Location Doesn't Exists"
            #     logger.error(msg)
            #     raise Exception(msg)
            # availableFiles = []
            # if filePattern != None and filePattern != "":
            #     if customPattern.upper() == "Y":
            #         customPatternFlag = True
            #     else:
            #         customPatternFlag = False
            #     if (s3FileList == False) or (s3FileList == None):
            #         msg = "No Files Available in Source S3 location"
            #         logger.error(msg)
            #         raise Exception(msg)
            #     patternValidatorObj = patternValidator()
            #     for file in s3FileList:
            #         fileKey = file["Key"]
            #         # Get File Name
            #         finaleName = fileKey.split("/")[-1]
            #         if finaleName != None or finaleName != "":
            #             patternMatched = patternValidatorObj.patternValidator(source=finaleName, pattern=filePattern, customPattern = customPatternFlag)
            #             if patternMatched == True:
            #                 logger.info("Pattern is Valid")
            #                 availableFiles.append(finaleName)
            #             else:
            #                 logger.info("Pattern is not Valid")
            # else:
            #     for file in s3FileList:
            #         fileKey = file["Key"]
            #         # Get File Name
            #         finaleName = fileKey.split("/")[-1]
            #         if finaleName != None and finaleName != "":
            #             availableFiles.append(finaleName)
            # return availableFiles
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())
class cleanUp:
    def __init__(self):
        a = 1
    def ingestionCleanUp(self,bucketName,prefix):
        try:
            self.s3FunctionsObj = s3Functions()
            # Check if Directory exists
            dirCheck =  self.s3FunctionsObj.listS3Files(bucketName,prefix)
            if (dirCheck != None) and (dirCheck != False):
                logger.info("Starting Cleanup for Failed process")
                response = self.s3FunctionsObj.deleteS3Directory(bucketName,prefix)
                if response == True:
                    logger.info("Cleanup for Completed")
                    return True
                else:
                    message = "Cleanup for Failed Process Failed"
                    logger.info(message)
                    raise message
            else:
                return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            return False

class Utils:
    def __init__(self,env=None):
        msg = "Initilizing Utils"
        logger.info(msg)
        if environmentParams.PLATFORM_NAME.lower() == "glue":
            self.args = getResolvedOptions(sys.argv, ["dataset_master_key","env","run_id"])
        else:
            self.args = dbutils.widgets.getAll()
            env = environmentParams.ENV_NAME.lower()
        if env != None:
            self.env = env
        else:
            self.env = self.args["env"]
        self.s3Obj = s3Functions()

    def getPartitionValue(self,datasetName):
        # Get Partition Value
        try:
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
                msg = "Error: " + str(e.__str__()) + "s3://" + datasetMasterKey + "/" + datasetMasterKey
                logger.error(msg)
                raise Exception(e.__str__())
            try:
                partitionValuesKey = datasetMaster.get("reprocess_master_file_s3_key")
            except:
                partitionValuesKey = None
            if partitionValuesKey == None or partitionValuesKey == "":
                msg = "Partition Value Key is Empty"
                logger.warn(msg)
                return []
            if partitionValuesKey.endswith("/"):
                msg = "No Partition Value Key JSON file available!!! Skipping the Custom Partition Value Step"
                logger.error(msg)
                return []
            rawS3Bucket = environmentParams.RAW_S3_BUCKET.format(self.env)
            msg = "Input Bucket Name: {}".format(rawS3Bucket)
            logger.info(msg)
            msg = "Partition Value Key: {}".format(partitionValuesKey)
            logger.info(msg)
            if partitionValuesKey.startswith('s3://'):
                partitionValuesKey = partitionValuesKey.lstrip("s3://").split("/")
                rawS3Bucket = partitionValuesKey[0]
                partitionValuesKey = "/".join(partitionValuesKey[1:])
            try:
                partitionValues = self.s3Obj.getFileContent(rawS3Bucket,partitionValuesKey).read().decode('utf-8')
                partitionValues = json.loads(partitionValues)
                msg = "Content of {}: {}".format(partitionValuesKey,str(partitionValues))
                logger.info(msg)
                dt = str(datetime.utcnow().strftime("%Y%m%d"))
                msg = "Fetching Data for Date: {}".format(dt)
                logger.info(msg)
                partitionValues = partitionValues.get(dt)
                partitionValues = partitionValues.get(datasetName)
                msg = "Partition Values: {}".format(str(partitionValues))
                logger.info(msg)
                if partitionValues == None:
                    partitionValues = []
                return partitionValues
            except Exception as e:
                msg = "Error: " + str(e.__str__()) + " while reading file s3://" + rawS3Bucket + "/" + partitionValuesKey
                logger.error(msg)
                return []
        except Exception as e:
            msg = str(e.__str__())
            logger.error(msg)
            raise Exception(e.__str__())


    def getPreviousHist(self, histTableName, histTablePartitionKey, stagingTableName='', stagingTablePartitionKey=[], columnsToBeDropped=[], histDatasetName=None):
        try:
            if histTableName == "" or histTableName == None:
                msg = "Hist Table Name is Mandatory"
                logger.error(msg)
                raise Exception(msg)
            if type(histTablePartitionKey) is not list:
                msg = "histTablePartitionKey should be a list"
                logger.error(msg)
                raise Exception(msg)
            if len(histTablePartitionKey) == 0:
                msg = "histTablePartitionKey is Empty"
                logger.error(msg)
                raise Exception(msg)
            # Prepare Hist Query
            histQuery = "SELECT * FROM " + histTableName + " WHERE "
            subQuery = ""
            if histDatasetName != None:
                histPartitionValue = self.getPartitionValue(histDatasetName)
            else:
                histPartitionValue = []
            if len(histPartitionValue) == 0:
                for key in histTablePartitionKey:
                    if subQuery == "":
                        subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,histTableName)
                    else:
                        subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,histTableName)
            elif (type(histPartitionValue) != dict and histPartitionValue.lower() == 'full load'):
                histQuery = "SELECT * FROM " + histTableName + " LIMIT 0"
            else:
                try:
                    for key in histTablePartitionKey:
                        if subQuery == "":
                            subQuery = "{} = '{}'".format(key,histPartitionValue[key])
                        else:
                            subQuery = subQuery + " AND {} = '{}'".format(key,histPartitionValue[key])
                except:
                    msg = "Value for Partition Key {} is not available".format(key)
                    logger.warn(msg)
            histQuery = histQuery + subQuery
            try:
                msg = "Running Hist Query: " + histQuery
                logger.info(msg)
                histDf = spark.sql(histQuery)
            except:
                if stagingTableName == "" or stagingTableName == None:
                    msg = "Staging Table Name is Mandatory"
                    logger.error(msg)
                    raise Exception(msg)
                if type(stagingTablePartitionKey) is not list:
                    msg = "stagingTablePartitionKey should be a list"
                    logger.error(msg)
                    raise Exception(msg)
                if len(stagingTablePartitionKey) == 0:
                    msg = "stagingTablePartitionKey is Empty"
                    logger.error(msg)
                    raise Exception(msg)
                histQuery = "SELECT * FROM {} LIMIT 0".format(stagingTableName)
                msg = "Hist Table Doesn't Exists, getting table structure from Stagging Table! Running Query: " + histQuery
                logger.info(msg)
                histDf = spark.sql(histQuery)

            listAuditColumns = ["source_file_location","processed_time","execution_date","execution_cycle_id","source_file_name"]
            listAuditColumns = list(set(histTablePartitionKey + stagingTablePartitionKey + listAuditColumns + columnsToBeDropped))
            histDf = histDf.drop(*listAuditColumns)
            return histDf
        except Exception as e:
            message = "Error: an error occurred while getting Previous Hidt Data"
            logger.error(message)
            message = "Error: " + e.__str__()
            logger.error(message)
            raise e

    def getPreviousHistForDW(self, histTableName, histTablePartitionKey,columnsToBeDropped = []):
        try:
            if histTableName == "" or histTableName == None:
                msg = "Hist Table Name is Mandatory"
                logger.error(msg)
                raise Exception(msg)
            if type(histTablePartitionKey) is not list:
                msg = "histTablePartitionKey should be a list"
                logger.error(msg)
                raise Exception(msg)
            if len(histTablePartitionKey) == 0:
                msg = "histTablePartitionKey is Empty"
                logger.error(msg)
                raise Exception(msg)
            # Prepare Hist Query
            histQuery = "SELECT * FROM " + histTableName + " WHERE "
            subQuery = ""
            histPartitionValue = self.getPartitionValue(histTableName)
            if len(histPartitionValue) == 0:
                for key in histTablePartitionKey:
                    if subQuery == "":
                        subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,histTableName)
                    else:
                        subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,histTableName)
            elif (type(histPartitionValue) != dict and histPartitionValue.lower() == 'full load'):
                histQuery = "SELECT * FROM " + histTableName + " LIMIT 0"
            else:
                try:
                    for key in histTablePartitionKey:
                        if subQuery == "":
                            subQuery = "{} = '{}'".format(key,histPartitionValue[key])
                        else:
                            subQuery = subQuery + " AND {} = '{}'".format(key,histPartitionValue[key])
                except:
                    msg = "Value for Partition Key {} is not available".format(key)
                    logger.warn(msg)
            histQuery = histQuery + subQuery
            try:
                msg = "Running Hist Query: " + histQuery
                logger.info(msg)
                histDf = spark.sql(histQuery)
                histDf = histDf.drop(*histTablePartitionKey)
            except Exception as e:
                message = "Error: " + e.__str__()
                logger.error(message)
                raise e
            listAuditColumns = ["source_file_location","processed_time","execution_date","execution_cycle_id","source_file_name"]
            listAuditColumns = list(set(histTablePartitionKey + listAuditColumns + columnsToBeDropped))
            histDf = histDf.drop(*listAuditColumns)
            return histDf
        except Exception as e:
            message = "Error: an error occurred while getting Previous Hidt Data"
            logger.error(message)
            message = "Error: " + e.__str__()
            logger.error(message)
            raise e

    def getLatestStgData(self, stagingTableName, loadType='', stagingTablePartitionKey=[],columnsToBeDropped = [],histDatasetName=None):
        try:
            if type(stagingTablePartitionKey) is not list:
                msg = "stagingTablePartitionKey should be a list"
                logger.error(msg)
                raise Exception(msg)
            if type(stagingTablePartitionKey) is not list:
                msg = "stagingTablePartitionKey should be a list"
                logger.error(msg)
                raise Exception(msg)
            if stagingTableName == "" or stagingTableName == None:
                msg = "Staginf Table Name is Mandatory"
                logger.error(msg)
                raise Exception(msg)
            if histDatasetName != None:
                histPartitionValue = self.getPartitionValue(histDatasetName)
                if len(histPartitionValue) != 0:
                    if (type(histPartitionValue) != dict and histPartitionValue.lower() == 'full load'):
                        loadType = 'full'
            # Prepare Staging Query
            # Custom list of cycles also need to be done in future
            match loadType.lower():
                case None:
                    try:
                        stgQuery = "SELECT * FROM {} WHERE ".format(stagingTableName)
                        subQuery = ""
                        stagingTablePartitionValue = self.getPartitionValue(stagingTableName)
                        if len(stagingTablePartitionValue) == 0:
                            for key in stagingTablePartitionKey:
                                if subQuery == "":
                                    subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                                else:
                                    subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                        else:
                            try:
                                for key in stagingTablePartitionKey:
                                    if subQuery == "":
                                        subQuery = "{} = '{}'".format(key,stagingTablePartitionValue[key])
                                    else:
                                        subQuery = subQuery + " AND {} = '{}'".format(key,stagingTablePartitionValue[key])
                            except:
                                msg = "Value for Partition Key {} is not available".format(key)
                                logger.warn(msg)
                        stgQuery = stgQuery + subQuery
                        msg = "Running Query: " + stgQuery
                        logger.info(msg)
                        stgDf = spark.sql(stgQuery)
                    except Exception as e:
                        message = "Error: " + e.__str__()
                        logger.error(message)
                        raise e
                case '':
                    try:
                        stgQuery = "SELECT * FROM {} WHERE ".format(stagingTableName)
                        subQuery = ""
                        stagingTablePartitionValue = self.getPartitionValue(stagingTableName)
                        if len(stagingTablePartitionValue) == 0:
                            for key in stagingTablePartitionKey:
                                if subQuery == "":
                                    subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                                else:
                                    subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                        else:
                            try:
                                for key in stagingTablePartitionKey:
                                    if subQuery == "":
                                        subQuery = "{} = '{}'".format(key,stagingTablePartitionValue[key])
                                    else:
                                        subQuery = subQuery + " AND {} = '{}'".format(key,stagingTablePartitionValue[key])
                            except:
                                msg = "Value for Partition Key {} is not available".format(key)
                                logger.warn(msg)
                        stgQuery = stgQuery + subQuery
                        msg = "Running Query: " + stgQuery
                        logger.info(msg)
                        stgDf = spark.sql(stgQuery)
                    except Exception as e:
                        message = "Error: " + e.__str__()
                        logger.error(message)
                        raise e
                case 'full':
                    stgQuery = "SELECT * FROM {}".format(stagingTableName)
                    msg = "Running Query: " + stgQuery
                    logger.info(msg)
                    stgDf = spark.sql(stgQuery)
                case 'latest_cycle':
                    columnNotFound = False
                    stgQueryDF = "SELECT * FROM {} WHERE execution_cycle_id = (select max(execution_cycle_id) from {})".format(stagingTableName,stagingTableName)
                    stgQueryCCF = "SELECT * FROM {} WHERE pt_cycle_id = (select max(pt_cycle_id) from {})".format(stagingTableName,stagingTableName)
                    try:
                        msg = "Running Query: " + stgQueryDF
                        logger.info(msg)
                        stgDf = spark.sql(stgQueryDF)
                    except Exception as e:
                        columnNotFound = True
                        msg = "execution_cycle_id Columns doesn't exists"
                        logger.info(msg)
                    try:
                        if columnNotFound == True:
                            msg = "Running Query: " + stgQueryCCF
                            logger.info(msg)
                            stgDf = spark.sql(stgQueryCCF)
                            columnNotFound = False
                    except Exception as e:
                        columnNotFound = True
                        msg = "pt_cycle_id Columns doesn't exists"
                        logger.info(msg)
                        try:
                            stgQuery = "SELECT * FROM {} WHERE ".format(stagingTableName)
                            subQuery = ""
                            for key in stagingTablePartitionKey:
                                if subQuery == "":
                                    subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                                else:
                                    subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                            stgQuery = stgQuery + subQuery
                            msg = "Running Query: " + stgQuery
                            logger.info(msg)
                            stgDf = spark.sql(stgQuery)
                        except Exception as e:
                            message = "Error: " + e.__str__()
                            logger.error(message)
                            raise e
                case 'latest_date':
                    try:
                        stgQuery = "SELECT * FROM {} WHERE execution_date = (select max(execution_date) from {})".format(stagingTableName,stagingTableName)
                        msg = "Running Query: " + stgQuery
                        logger.info(msg)
                        stgDf = spark.sql(stgQuery)
                    except Exception as e:
                        msg = "execution_date Columns doesn't exists"
                        logger.info(msg)
                        try:
                            stgQuery = "SELECT * FROM {} WHERE ".format(stagingTableName)
                            subQuery = ""
                            for key in stagingTablePartitionKey:
                                if subQuery == "":
                                    subQuery = "{} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                                else:
                                    subQuery = subQuery + " AND {} = (SELECT max({}) FROM {})".format(key,key,stagingTableName)
                            stgQuery = stgQuery + subQuery
                            msg = "Running Query: " + stgQuery
                            logger.info(msg)
                            stgDf = spark.sql(stgQuery)
                        except Exception as e:
                            message = "Error: " + e.__str__()
                            logger.error(message)
                            raise e
                case _:
                    msg = "Load Type not allowed"
                    logger.error(msg)
                    raise Exception(msg)
            # Prepare List of Audit Columns
            listAuditColumns = ["execution_date","execution_cycle_id","pt_batch_id","pt_cycle_id","pt_file_id","pt_data_dt"]
            listAuditColumns = list(set(stagingTablePartitionKey + listAuditColumns + columnsToBeDropped))
            stgDf = stgDf.drop(*listAuditColumns)
            return stgDf
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise e
    def runDataLakeQuery(qyery):
        try:
            msg = "Executin Query in Data Lake: " + qyery
            logger.inof(msg)
            resultDf = spark.sql(qyery)
            msg = "Query Executed in Data Lake: " + qyery
            logger.inof(msg)
            return resultDf
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise e

    def sendNotification(self,processName,processStatus,env,appName,NOTIFY,stepName="",errorShortDesc="",errorDesc=""):
        try:
            if NOTIFY:
                loadNotificationObj = loadNotification()
                status = loadNotificationObj.sendNotification(processName,processStatus,env,appName,stepName,errorShortDesc,errorDesc)
                return True
        except Exception as e:
            logger.error("Error occured during sending notification: ", str(e))
            raise e

    def triggerSparcUtility(self,processName, dagRunId, stepName, env, appName,errorFullDessc,NOTIFY=True):
        """This method triggers SNOW Utility"""
        try:
            incident_data = {}
            logger.info("Staring function to trigger SNOW Utility")

            logger.info("Preparing the information to be added to the Incident")
            incident_data["processName"] = processName
            incident_data["stepName"] = stepName
            incident_data["errorFullDessc"] = errorFullDessc
            incident_data["errorShortDessc"] = env.upper() + " Process " + processName + " failed at Step " + stepName + " ("+dagRunId+")"
            self.sendNotification(processName,'failed',env,appName,NOTIFY,stepName,incident_data["errorShortDessc"],errorFullDessc)
            snow_obj = SparcUtility(appName)
            output = snow_obj.createsparcticket(incident_data)
            logger.info("Output of SNOW Trigger: " + str(output))
            return True
        except Exception as e:
            logger.error("Failed while triggering SNOW Utility. Error: ", str(e))
            raise e
    def triggerBoundaryApp(self,boundaryAppMasterSchema,boundaryAppMasterTable,bondaryAppConfigTable,datasetName):
        try:
            #Boundary App
            tgtTblQuery = """select distinct target_column_name, cast(target_column_sequence as integer) from {}.{} WHERE dataset_name = '{}' AND active_flag = 'Y'ORDER BY cast(target_column_sequence as integer) ASC""".format(boundaryAppMasterSchema,boundaryAppMasterTable,datasetName)
            tgtTblDf = spark.sql(tgtTblQuery).collect()

            tgt_tbl_column_list = []
            for row in tgtTblDf:
                tgt_tbl_column_list.append(row['target_column_name'])

            schema = StructType([StructField(name, StringType(), True) for name in tgt_tbl_column_list])
            empty_df = spark.createDataFrame([], schema)

            datasetListQuery = """select distinct source_schema_name, source_table_name from {}.{} WHERE dataset_name = '{}' AND active_flag = 'Y'""".format(boundaryAppMasterSchema,boundaryAppMasterTable,datasetName)

            datasetListDf = spark.sql(datasetListQuery)
            for row in datasetListDf.rdd.toLocalIterator():
                boundaryAppQuery = "SELECT DISTINCT source_schema_name, source_column_name, source_table_name, target_column_name, load_type, source, cast(target_column_sequence as integer) FROM {}.{} WHERE source_schema_name = '{}' AND source_table_name = '{}' AND dataset_name = '{}' AND active_flag='Y' ORDER BY cast(target_column_sequence as integer) ASC".format(boundaryAppMasterSchema,boundaryAppMasterTable,row["source_schema_name"],row["source_table_name"],datasetName)
                src_col_list=[]
                tgt_col_list=[]
                msg = "Boundary App Master Query: {}".format(boundaryAppQuery)
                logger.info(msg)
                for i in spark.sql(boundaryAppQuery).rdd.toLocalIterator():
                    obj={}
                    obj['target_column_name']=i['target_column_name']
                    obj['source_column_name']=i['source_column_name']
                    src_col_list.append(obj)
                    schema_name=i['source_schema_name']
                    dataset_name=i['source_table_name']
                    LOAD_TYPE=i['load_type']
                    source=i['source']
                configQuery = "SELECT DISTINCT where_clause, join_statement FROM {}.{} WHERE source_schema_name = '{}' AND source_table_name = '{}' AND dataset_name = '{}' AND active_flag='Y'".format(boundaryAppMasterSchema,bondaryAppConfigTable,row["source_schema_name"],row["source_table_name"],datasetName)
                msg = "Boundary App Config Query: {}".format(configQuery)
                logger.info(msg)
                whereClause = None
                try:
                    for i in spark.sql(configQuery).rdd.toLocalIterator():
                        JOIN_STATEMENT=i['join_statement']
                        WHERE_CLAUSE=i['where_clause'].strip()
                        if WHERE_CLAUSE != None and WHERE_CLAUSE != '':
                            if WHERE_CLAUSE.lower().startswith("where"):
                                msg = "Please remove `WHERE` word from the where clause field"
                                logger.error(msg)
                                raise Exception(msg)
                            else:
                                whereClause = "WHERE {}".format(WHERE_CLAUSE)
                except Exception as e:
                    message = "Error: " + e.__str__()
                    logger.error(message)

                selectColumn =""
                counter=0
                for srcCol in src_col_list:
                    if srcCol['source_column_name'] == None or srcCol['source_column_name'] == "":
                        sourceColumnName = "cast(NULL as string)"
                    else:
                        sourceColumnName = "cast(" + srcCol['source_column_name'] + " as string)"
                    if counter!=len(src_col_list)-1:
                        selectColumn=selectColumn+sourceColumnName+" as "+srcCol['target_column_name'].lower()+","
                    else:
                        selectColumn=selectColumn+sourceColumnName+" as "+srcCol['target_column_name'].lower()
                    counter=counter+1
                    query = "SELECT DISTINCT {} FROM {}.{}".format(selectColumn,schema_name,dataset_name)
                if whereClause != None:
                    query = "{} {}".format(query,whereClause)
                msg = "Boundary App Query for {}.{}: {}".format(schema_name,dataset_name,query)
                logger.info(msg)
                result = spark.sql(query)
                empty_df = empty_df.unionAll(result)
            return empty_df
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def createTempTable(self,inputDf,datasetName,uniqueId,tempTableName):
        try:
            bucketName = str(environmentParams.SANDBOX_S3_BUCKET).format(self.env)
            prefix = "temp/{}/{}/{}/".format(datasetName,tempTableName,uniqueId)
            tempLoc = "s3://{}/{}/".format(bucketName,prefix)
            inputDf.write.parquet(tempLoc)
            inputDf.unpersist(True)
            inputDf = spark.read.parquet(tempLoc)
            inputDf.createOrReplaceTempView(tempTableName)
            inputDf.unpersist(True)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def createTempTableDf(self,inputDf,datasetName,uniqueId,tempTableName):
        try:
            bucketName = str(environmentParams.SANDBOX_S3_BUCKET).format(self.env)
            prefix = "temp/{}/{}/{}/".format(datasetName,tempTableName,uniqueId)
            tempLoc = "s3://{}/{}/".format(bucketName,prefix)
            inputDf.write.parquet(tempLoc)
            inputDf.unpersist(True)
            inputDf = spark.read.parquet(tempLoc)
            return inputDf
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

    def cleanUpTempTable(self,datasetName,uniqueId,tempTableNames):
        try:
            bucketName = str(environmentParams.SANDBOX_S3_BUCKET).format(self.env)
            for tempTableName in tempTableNames:
                prefix = "temp/{}/{}/{}/".format(datasetName,tempTableName,uniqueId)
                self.s3Obj.cleanUpS3(bucketName,prefix)
            return True
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(e.__str__())

class versionUpgread:
    def __init__(self,env):
        msg = "Initilizing Version Upgread"
        logger.info(msg)
        try:
            self.dataFlexorVersion = environmentParams.DATAFLEXOR_VERSION
        except Exception as e:
            self.dataFlexorVersion = 'v1.3.1'
        self.s3Obj = s3Functions()
        self.env = env
    # Get column list and data type of DF
    def getColumnDataType(self,inpDF):
        try:
            columnDataType = []
            for columnName,dataType in inpDF.dtypes:
                columnDataType.append(
                    {
                        "name": columnName,
                        "data_type": dataType
                    }
                )
            return columnDataType
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def updateGlueTable(self,columnList,targetSchema,targetTable,tableDescription,finalTableLocation,partitionKeys,partitionValues):
        try:
            msg = "Partition Key: " + str(partitionKeys)
            logger.info(msg)
            msg = "Partition Value: " + str(partitionValues)
            logger.info(msg)
            # Create/Update Table
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
    def migrateFileLog(self):
        try:
            bucketName=environmentParams.CURATED_S3_BUCKET.format(self.env)
            applicationName = environmentParams.APPLICATION_NAME
            try:
                subApplicationName = environmentParams.SUB_APPLICATION_NAME
                if subApplicationName != None and subApplicationName != "":
                    applicationName = subApplicationName
            except Exception as e:
                msg = "This is not a sub application"
                logger.info(msg)

            logger.info("Getting Old File Logs")
            oldPrefix = applicationName + "/file_log/file_log_mdh/"
            keysList = self.s3Obj.listS3Directories(bucketName,oldPrefix)
            if type(keysList) == bool:
                keysList = []
            elif type(keysList) == None:
                keysList = []
            msg = "Number of Prefix: {}".format(str(len(keysList)))
            logger.info(msg)
            if len(keysList) > 0:
                # Migrate logs
                logger.info("Staring Old File Log Migration")
                basePath = "s3://" + bucketName + "/" + applicationName + "/file_log/file_log_mdh/"
                msg = "Base Path: {}".format(basePath)
                logger.info(msg)
                df = spark.read.option("basePath", basePath).parquet(basePath + "*")
                df = df.withColumnRenamed("dataset_name", "table_name")
                df = df.withColumn("dataset_name", df.table_name).withColumn("run_id", coalesce(df['run_id'],lit("NA"))).withColumn("input_location_prefix", lit(''))
                df = df.select("table_name", "run_id", "input_location_prefix", "file_name", "file_type", "record_count", "execution_start_time", "execution_end_time", "ingestion_status", "dataset_name", "execution_date", "execution_cycle_id", "source_file_name")
                logTablePath = "s3://" + bucketName + "/DataFlexor/" + applicationName + "/file_log_mdh/"
                df.write.mode("overwrite").partitionBy("dataset_name", "execution_date", "execution_cycle_id", "source_file_name").parquet(logTablePath)
                logger.info("Old File Log Migration Completed")
                # Create Table
                newDf = df.drop(*["dataset_name", "execution_date", "execution_cycle_id", "source_file_name"])
                columnList = self.getColumnDataType(newDf)
                # Prepare Ingestion log Datafrate amd table
                ingestionLogSchema = environmentParams.LOG_SCHEMA.replace("`","")
                fileLogTable = "file_log_"+applicationName
                fileLogTable = fileLogTable.replace("-","_")
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
                partitionValuesList = df.select("dataset_name", "execution_date", "execution_cycle_id", "source_file_name").distinct().collect()
                dataSetNameList = []
                executionDateList = []
                executionCycleIdList = []
                sourceFileNameList = []

                for row in partitionValuesList:
                    dataSetNameList.append(row.dataset_name)
                    executionDateList.append(row.execution_date)
                    executionCycleIdList.append(row.execution_cycle_id)
                    sourceFileNameList.append(row.source_file_name)

                for i, v in enumerate(dataSetNameList):
                    partitionValues = {
                        "dataset_name": str(v),
                        "execution_date": str(executionDateList[i]),
                        "execution_cycle_id": str(executionCycleIdList[i]),
                        "source_file_name": str(sourceFileNameList[i])
                    }
                    msg = "Partition Key: " + str(partitionKeys)
                    logger.info(msg)
                    msg = "Partition Value: " + str(partitionValues)
                    logger.info(msg)
                    self.updateGlueTable(columnList,ingestionLogSchema,fileLogTable,'',logTablePath,partitionKeys,partitionValues)
                    msg = "Updated Log Table: " + fileLogTable
                    logger.info(msg)
                # Clead the Old File Logs
                logger.info("Clean-up Started")
                self.s3Obj.cleanUpS3(bucketName,oldPrefix)
                logger.info("Clean-up Completed")
                return True
            else:
                return False
        except Exception as e:
            raise Exception(e)
    def migrateIngestionLog(self):
        try:
            bucketName=environmentParams.CURATED_S3_BUCKET.format(self.env)
            applicationName = environmentParams.APPLICATION_NAME
            try:
                subApplicationName = environmentParams.SUB_APPLICATION_NAME
                if subApplicationName != None and subApplicationName != "":
                    applicationName = subApplicationName
            except Exception as e:
                msg = "This is not a sub application"
                logger.info(msg)

            logger.info("Getting Old Ingestion Logs")
            oldPrefix = applicationName + "/ingestion_log/ingestion_log_mdh/"
            keysList = self.s3Obj.listS3Directories(bucketName,oldPrefix)
            if type(keysList) == bool:
                keysList = []
            elif type(keysList) == None:
                keysList = []
            msg = "Number of Prefix: {}".format(str(len(keysList)))
            logger.info(msg)
            if len(keysList) > 0:
                # Migrate logs
                logger.info("Staring Old Ingestion Log Migration")
                basePath = "s3://" + bucketName + "/" + applicationName + "/ingestion_log/ingestion_log_mdh/"
                msg = "Base Path: {}".format(basePath)
                logger.info(msg)
                df = spark.read.option("basePath", basePath).parquet(basePath + "*")
                df = df.withColumnRenamed("dataset_name", "table_name")
                df = df.withColumn("dataset_name", df.table_name)
                # df = df.select("table_name", "run_id", "application", "source_files", "static_file_pattern", "source_file_pattern", "file_type", "encoding", "escape", "multiline", "delimiter", "table_location", "target_schema", "target_table", "record_count", "execution_start_datetime", "execution_end_time", "dataset_name", "execution_date", "execution_cycle_id")
                logTablePath = "s3://" + bucketName + "/DataFlexor/" + applicationName + "/ingestion_log_mdh/"
                df.write.mode("overwrite").partitionBy("dataset_name", "execution_date", "execution_cycle_id").parquet(logTablePath)
                logger.info("Old Ingestion Log Migration Completed")
                # Create Table
                newDf = df.drop(*["dataset_name", "execution_date", "execution_cycle_id"])
                columnList = self.getColumnDataType(newDf)
                # Prepare Ingestion log Datafrate amd table
                ingestionLogSchema = environmentParams.LOG_SCHEMA.replace("`","")
                fileLogTable = "ingestion_log_"+applicationName
                fileLogTable = fileLogTable.replace("-","_")
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

                partitionValuesList = df.select("dataset_name", "execution_date", "execution_cycle_id").distinct().collect()
                dataSetNameList = []
                executionDateList = []
                executionCycleIdList = []

                for row in partitionValuesList:
                    dataSetNameList.append(row.dataset_name)
                    executionDateList.append(row.execution_date)
                    executionCycleIdList.append(row.execution_cycle_id)

                for i, v in enumerate(dataSetNameList):
                    partitionValues = {
                        "dataset_name": str(v),
                        "execution_date": str(executionDateList[i]),
                        "execution_cycle_id": str(executionCycleIdList[i])
                    }
                    msg = "Partition Key: " + str(partitionKeys)
                    logger.info(msg)
                    msg = "Partition Value: " + str(partitionValues)
                    logger.info(msg)
                    self.updateGlueTable(columnList,ingestionLogSchema,fileLogTable,'',logTablePath,partitionKeys,partitionValues)
                    msg = "Updated Log Table: " + fileLogTable
                    logger.info(msg)
                # Clead the Old File Logs
                logger.info("Clean-up Started")
                self.s3Obj.cleanUpS3(bucketName,oldPrefix)
                logger.info("Clean-up Completed")
                return True
            else:
                return False
        except Exception as e:
            raise Exception(e)
    def processVersionUpgread(self):
        match self.dataFlexorVersion:
            case 'v1.3.1':
                self.migrateFileLog()
                self.migrateIngestionLog()
                return True
            case _:
                msg = "Version is less than v1.3.0"
                logger.error(msg)
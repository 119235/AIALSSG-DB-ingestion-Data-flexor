__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Publish Table Clean-Up Module
   Purpose             :   This Module is used for Cleaning Previous Run's files from Publish Table S3 Location

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

class publishCleanup:
    def __init__(self):
        self.s3Obj = s3Functions()

    def publishCleanup(self,bucket,prefix):
        msg = "Starting Publish Table Cleanup"
        logger.info(msg)
        msg = "Publish Bucket: {}".format(bucket)
        logger.info(msg)
        msg = "Publish Prefix: {}".format(prefix)
        logger.info(msg)
        # msg = "Execution Date: {}".format(processDate)
        # logger.info(msg)
        # msg = "Execution Cycle ID: {}".format(cycleId)
        # logger.info(msg)
        try:
            # prefixes = self.s3Obj.listS3Directories(bucket,prefix)
            # for prefix in prefixes:
            #     if processDate not in prefix:
            #         cleanUpResponse = self.s3Obj.cleanUpS3(bucket,prefix)
            #         if cleanUpResponse == False:
            #             msg = "Unable to Clean Publish Location: s3://" + bucket + "/" + prefix
            #             logger.error(msg)
            #     else:
            #         prefixesNew = self.s3Obj.listS3Directories(bucket,prefix)
            #         for prefixNew in prefixesNew:
            #             if cycleId not in prefixNew:
            #                 cleanUpResponse = self.s3Obj.cleanUpS3(bucket,prefixNew)
            #                 if cleanUpResponse == False:
            #                     msg = "Unable to Clean Publish Location: s3://" + bucket + "/" + prefixNew
            #                     logger.error(msg)
            cleanUpResponse = self.s3Obj.cleanUpS3(bucket,prefix)
            if cleanUpResponse == False:
                msg = "Unable to Clean Publish Location: s3://" + bucket + "/" + prefix
                logger.error(msg)
            msg = "Publish Table CleanUp Completed"
            logger.info(msg)
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Archival Module
   Purpose             :   This Module is used for Archiving Staging and Processed Layer Table

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

from DataFlexor import s3Functions

class archive:
    def __init__(self):
        self.s3Obj = s3Functions()

    def archiveStaging(self,bucket,prefix,cycleId,archiveBucket,archivePrefix):
        try:
            prefixes = self.s3Obj.listS3Directories(bucket,prefix)
            for prefix in prefixes:
                if cycleId not in prefix:
                    moveResponse = self.s3Obj.moveS3Directory(bucket,prefix,archiveBucket,archivePrefix)
                    if moveResponse == False:
                        msg = "Unable to migrate files from " + bucket + "/" + prefix
                        logger.error(msg)
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

    def archiveProcessedTableData(self,bucket,prefix,cycleId,archiveBucket,archivePrefix):
        try:
            prefixes = self.s3Obj.listS3Directories(bucket,prefix)
            for prefix in prefixes:
                if cycleId not in prefix:
                    moveResponse = self.s3Obj.moveS3Directory(bucket,prefix,archiveBucket,archivePrefix)
                    if moveResponse == False:
                        msg = "Unable to migrate files from " + bucket + "/" + prefix
                        logger.error(msg)
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
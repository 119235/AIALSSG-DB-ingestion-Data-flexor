import sys
import json
import traceback
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, date_format, unix_timestamp, split,lit,to_date,to_timestamp
from datetime import datetime
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
from DataFlexor import processed,Utils
import environmentParams as environmentParams
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
try:
    processedObj = processed()
    '''
    Your Code
    '''
    processedObj.processedTable(resultDf=None)
except Exception as e:
    raise e

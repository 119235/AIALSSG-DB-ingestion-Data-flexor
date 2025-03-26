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
from pyspark.sql.functions import explode,explode_outer,regexp_replace,udf,lit,col,when,unix_timestamp,to_timestamp,date_format,unix_timestamp,to_timestamp,date_format
from datetime import datetime
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
from DataFlexor import ingestion

try:
    ingObj = ingestion()
    ingObj.ingestData()
except Exception as e:
    raise e
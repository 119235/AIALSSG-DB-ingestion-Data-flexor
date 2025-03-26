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
from pyspark.sql.functions import current_timestamp, date_format, unix_timestamp, split,lit
from datetime import datetime
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
from DataFlexor import processed,Utils
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
try:
    processedObj = processed()
    UtilsObj = Utils()
    # Get Previous HIST Data
    histDf = UtilsObj.getPreviousHist(
        histTableName="comm_pro_mdh_dw.pld_bulletin_healthcare_hist_dw",
        histTablePartitionKey=["execution_cycle_id","execution_date"],
        stagingTableName='comm_cur_mdh_staging.pld_bulletin_healthcare',
        stagingTablePartitionKey=["execution_cycle_id","execution_date","source_file_name"],
        histDatasetName="pld_bulletin_healthcare_hist"
    )
    if "mdh_hist_load_ts" not in histDf.columns:
        histDf = histDf.withColumn("mdh_hist_load_ts",current_timestamp())
    # get Latest Staging data
    stgDf = UtilsObj.getLatestStgData(
        stagingTableName="comm_cur_mdh_staging.pld_bulletin_healthcare",
        stagingTablePartitionKey=["execution_cycle_id","execution_date"],
        columnsToBeDropped=["source_file_location","processed_time","source_file_name"],
        histDatasetName="pld_bulletin_healthcare_hist"
    ).distinct()
    stgDf = stgDf.withColumn("mdh_hist_load_ts",current_timestamp())
    resultDf = histDf.union(stgDf)
    processedObj.processedTable(resultDf=resultDf)
except Exception as e:
    raise e
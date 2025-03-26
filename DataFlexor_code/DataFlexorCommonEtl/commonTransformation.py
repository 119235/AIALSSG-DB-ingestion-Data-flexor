__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Common Transformation Library
   Purpose             :   This module will provide functions that can be used to perform some standard transformation

   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   New Function added
"""
import os,boto3
import sys
import requests
import json
from datetime import date
from dateutil.relativedelta import relativedelta
import re
import sys
import logging
import boto3
import time
from botocore.exceptions import ClientError

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

from pyspark.sql.functions import explode,explode_outer,regexp_replace,udf,lit,col,when,to_date,to_timestamp,date_format,unix_timestamp,to_timestamp,date_format
from pyspark.sql.types import *

class commonTransformation():
    def __init__(self):
        pass
    # Flatten JSON Data
    def flattenJsonData(self,df):
        try:
            logger.info("Starting Flattening Json Data")
            complex_fields = dict([
                (field.name, field.dataType)
                for field in df.schema.fields
                if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
            ])
            qualify = list(complex_fields.keys())[0] + "_"
            while len(complex_fields) != 0:
                col_name = list(complex_fields.keys())[0]

                if isinstance(complex_fields[col_name], StructType):
                    expanded = [col(col_name + '.' + k).alias(col_name + '_' + k)
                    for k in [ n.name for n in complex_fields[col_name]]
                    ]
                    df = df.select("*", *expanded).drop(col_name)

                elif isinstance(complex_fields[col_name], ArrayType):
                    df = df.withColumn(col_name, explode_outer(col_name))

                complex_fields = dict([
                (field.name, field.dataType)
                for field in df.schema.fields
                if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
                ])

            for df_col_name in df.columns:
                df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))
            return df
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
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
    # Get column list and data type of DF
    def hederNameStandardization(self,inpDF):
        try:
            for columnName in inpDF.columns:
                # Remove Space
                inpDF = inpDF.withColumnRenamed(columnName, (re.sub(r'[^a-zA-Z0-9_#]', '_', columnName.lower())).strip('_'))
            return inpDF
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

    def converColumnNameToLowecase(self,inpDF):
        try:
            for columnName in inpDF.columns:
                # Remove Space
                inpDF = inpDF.withColumnRenamed(columnName, columnName.lower())
            return inpDF
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def columnFormat(self,df,schemaJson):
        try:
            for element in schemaJson:
                if element["column_data_type"].lower() == "date":
                    try:
                        dateFormat = element["data_format"]
                    except:
                        dateFormat = None

                    columnName = str(element["column_name"])

                    if dateFormat == None or dateFormat == "":
                        df = df.withColumn(columnName,to_date(df[columnName]))
                    else:
                        df = df.withColumn(columnName,to_date(date_format(unix_timestamp(df[columnName], str(dateFormat)).cast("timestamp"),'yyyy-MM-dd')))
                elif element["column_data_type"].lower() == "timestamp":
                    try:
                        dateFormat = element["data_format"]
                    except:
                        dateFormat = None

                    columnName = str(element["column_name"])

                    if dateFormat == None or dateFormat == "":
                        df = df.withColumn(columnName,to_timestamp(df[columnName]))
                    else:
                        if ".SSS" in dateFormat.upper():
                            outputFormat = 'yyyy-MM-dd HH:mm:ss.SSS'
                        else:
                            outputFormat = 'yyyy-MM-dd HH:mm:ss'
                        df = df.withColumn(columnName,to_timestamp(date_format(unix_timestamp(df[columnName], str(dateFormat)).cast("timestamp"),str(outputFormat))))
            return df
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
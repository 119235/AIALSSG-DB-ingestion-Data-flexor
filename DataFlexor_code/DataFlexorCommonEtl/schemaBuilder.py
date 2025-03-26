__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   JSON Schema Buolder
   Purpose             :   This Module is used for building schemas for JSON. If no Schema structure provided it will build the schema based on the data structure

   Last changed on     :   2024-06-01
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

from pyspark.sql.functions import explode,explode_outer,regexp_replace,udf,lit,col,when,unix_timestamp,to_timestamp,date_format
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,DoubleType,LongType,BooleanType,TimestampType,DecimalType,DateType,FloatType,ArrayType

class schemaBuilder:
    def genSchemaList(self,element):
        subSchema = StructType()
        for elementDict in element:
            if type(elementDict) == dict:
                for k,v in elementDict.items():
                    if type(v) == dict:
                        subSchema = subSchema.add(k,self.genSchemaDict(v),True)
                    elif type(v) == list:
                        subSchemaList = self.genSchemaList(v)
                        if subSchemaList == False:
                            subSchema = subSchema.add(k,StringType(),True)
                        else:
                            subSchema = subSchema.add(k,ArrayType(subSchemaList),True)
                    else:
                        match v.lower():
                            case 'str':
                                subSchema = subSchema.add(k,StringType(),True)
                            case 'string':
                                subSchema = subSchema.add(k,StringType(),True)
                            case 'int':
                                subSchema = subSchema.add(k,IntegerType(),True)
                            case 'integer':
                                subSchema = subSchema.add(k,IntegerType(),True)
                            case 'long':
                                subSchema = subSchema.add(k,LongType(),True)
                            case 'float':
                                subSchema = subSchema.add(k,FloatType(),True)
                            case 'float':
                                subSchema = subSchema.add(k,FloatType(),True)
                            case "decimal":
                                try:
                                    dataFormat = element.get("data_format").split(",")
                                    pos = int(dataFormat[0])
                                    scale = int(dataFormat[1])
                                except:
                                    logger.warn("No Data Format Provided for Decimal data type! Using Default format (18,2)")
                                    pos = 18
                                    scale = 2
                                subSchema = subSchema.add(k,DecimalType(pos,scale),True)
                            # case x if v.lower().startswith("decimal"):
                            #     poscale = v.lower().split("|")[1].split(",")
                            #     pos = poscale[0]
                            #     scale = poscale[1]
                            #     subSchema = subSchema.add(k,DecimalType(pos,scale),True)
                            case 'boolean':
                                subSchema = subSchema.add(k,BooleanType(),True)
                            case 'timestamp':
                                subSchema = subSchema.add(k,StringType(),True)
                            case 'date':
                                subSchema = subSchema.add(k,StringType(),True)
                return subSchema
            else:
                return False

    def genSchemaDict(self,element):
        try:
            subSchema = StructType()
            for k,v in element.items():
                if type(v) == dict:
                    subSchema = subSchema.add(k,self.genSchemaDict(v),True)
                elif type(v) == list:
                    subSchemaList = self.genSchemaList(v)
                    if subSchemaList == False:
                        subSchema = subSchema.add(k,StringType(),True)
                    else:
                        subSchema = subSchema.add(k,ArrayType(subSchemaList),True)
                else:
                    match v.lower():
                        case 'str':
                            subSchema = subSchema.add(k,StringType(),True)
                        case 'string':
                            subSchema = subSchema.add(k,StringType(),True)
                        case 'int':
                            subSchema = subSchema.add(k,IntegerType(),True)
                        case 'integer':
                            subSchema = subSchema.add(k,IntegerType(),True)
                        case 'long':
                            subSchema = subSchema.add(k,LongType(),True)
                        case 'float':
                            subSchema = subSchema.add(k,FloatType(),True)
                        case 'float':
                            subSchema = subSchema.add(k,FloatType(),True)
                        case x if v.lower().startswith("decimal"):
                            poscale = v.lower().split("|")[1].split(",")
                            pos = poscale[0]
                            scale = poscale[1]
                            subSchema = subSchema.add(k,DecimalType(pos,scale),True)
                        case 'boolean':
                            subSchema = subSchema.add(k,BooleanType(),True)
                        case 'timestamp':
                            subSchema = subSchema.add(k,StringType(),True)
                        case 'date':
                            subSchema = subSchema.add(k,StringType(),True)
            return subSchema
        except Exception as e:
            msg = "Error: " + e.__str__()
            logger.error(msg)
            return False

    def buildJsonShema(self,schemaJson):
        try:
            schema = StructType()
            if type(schemaJson) == list:
                schemaJson = schemaJson[0]
            elif type(schemaJson) != dict:
                msg = "The Schema Structure Provided is not a Valid JSON"
                logger.error(msg)
                raise Exception(msg)
            for element,value in schemaJson.items():
                if type(value) == dict:
                    schema = schema.add(element,self.genSchemaDict(value),True)
                elif type(value) == list:
                    subSchemaList = self.genSchemaList(value)
                    if subSchemaList == False:
                        schema = schema.add(element,StringType(),True)
                    else:
                        schema = schema.add(element,ArrayType(subSchemaList),True)
                else:
                    match value.lower():
                        case 'string':
                            schema = schema.add(element,StringType(),True)
                        case 'str':
                            schema = schema.add(element,StringType(),True)
                        case 'integer':
                            schema = schema.add(element,IntegerType(),True)
                        case 'int':
                            schema = schema.add(element,IntegerType(),True)
                        case 'long':
                            schema = schema.add(element,LongType(),True)
                        case 'float':
                            schema = schema.add(element,FloatType(),True)
                        case x if value.lower().startswith("decimal"):
                            poscale = value.lower().split("|")[1].split(",")
                            pos = poscale[0]
                            scale = poscale[1]
                            schema = schema.add(element,DecimalType(pos,scale),True)
                        case 'boolean':
                            schema = schema.add(element,BooleanType(),True)
                        case 'timestamp':
                            schema = schema.add(element,StringType(),True)
                        case 'date':
                            schema = schema.add(element,StringType(),True)
            return schema
        except Exception as e:
            msg = "Error: " + e.__str__()
            logger.error(msg)
            raise Exception(e)

    def buildCsvTxtExcelShema(self,schemaJson):
        try:
            schema = StructType()
            if type(schemaJson) != list:
                msg = "The Schema Structure Provided is not a Valid JSON"
                raise Exception(msg)
            for element in schemaJson:
                columnName = element.get("column_name")
                columnDataType = element.get("column_data_type")
                match columnDataType.lower():
                    case "string":
                        schema = schema.add(columnName,StringType(),True)
                    case "str":
                        schema = schema.add(columnName,StringType(),True)
                    case 'int':
                        schema = schema.add(columnName,IntegerType(),True)
                    case 'integer':
                        schema = schema.add(columnName,IntegerType(),True)
                    case 'long':
                        schema = schema.add(columnName,LongType(),True)
                    case 'float':
                        schema = schema.add(columnName,FloatType(),True)
                    case "decimal":
                        try:
                            dataFormat = element.get("data_format").split(",")
                            pos = int(dataFormat[0])
                            scale = int(dataFormat[1])
                        except:
                            logger.warn("No Data Format Provided for Decimal data type! Using Default format (18,2)")
                            pos = 18
                            scale = 2
                        schema = schema.add(columnName,DecimalType(pos,scale),True)
                    case 'double':
                        schema = schema.add(columnName,DoubleType(),True)
                    case 'boolean':
                        schema = schema.add(columnName,BooleanType(),True)
                    case 'timestamp':
                        schema = schema.add(columnName,StringType(),True)
                    case 'date':
                        schema = schema.add(columnName,StringType(),True)
                    case _:
                        msg = "Data Type: " + columnDataType.lower() + " is not yet supported for Column: " + columnName
                        logger.error(msg)
                        raise Exception(msg)
            return schema
        except Exception as e:
            msg = "Error: " + e.__str__()
            logger.error(msg)
            raise Exception(e)
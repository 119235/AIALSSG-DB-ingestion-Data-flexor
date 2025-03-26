#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = "CTS"

"""
   Module Name         :   Glue Catalog Table Library
   Purpose             :   This module provides function for creating, updating catalog table. Also using function of this module we can handle partitions of Glue Table

   Last changed on     :   2024-02-16
   Last changed by     :   Mrinal Paul
   Reason for change   :   Logic change for updating table metadata
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

class GlueTable():
    def __init__(self,databaseName,tableName,tableDesc="",columnList=[],tableLocation="",partitionKeys={},partitionValues=[],awsRegion='us-west-2'):
        self.databaseName = databaseName.replace("`","")
        self.tableName = tableName.replace("`","")
        self.tableDesc = tableDesc
        self.columnList = columnList
        self.tableLocation = tableLocation
        self.partitionKeys = partitionKeys
        self.partitionValues = partitionValues
        self.awsRegion = awsRegion
        session = boto3.session.Session()
        self.glue_client = session.client('glue', region_name='us-west-2')

    def prepareColumns(self):
        try:
            columnList = []
            for colDet in self.columnList:
                column = {
                    'Name': colDet["name"],
                    'Type': colDet["data_type"]
                }
                columnList.append(column)
            return columnList
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

    def preparePartitionKey(self):
        try:
            partitionKeys = []
            for pk in self.partitionKeys:
                key = {
                        'Name': pk["name"],
                        'Type': pk["data_type"]
                    }
                partitionKeys.append(key)
            return partitionKeys
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    # Create table function
    def createGlueTable(self):
        try:
            logger.info("Starting Create Glue Table")
            TableInput={
                'Name': self.tableName,
                'Description': self.tableDesc,
                'Owner': 'CTS',
                'StorageDescriptor': {
                    'Columns': self.prepareColumns(),
                    'Location': self.tableLocation,
                    "Compressed": False,
                    "NumberOfBuckets": -1,
                    "BucketColumns": [],
                    "SortColumns": [],
                    "Parameters": {},
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {
                            "serialization.format": "1"
                        }
                    },
                    "SkewedInfo": {
                        "SkewedColumnNames": [],
                        "SkewedColumnValues": [],
                        "SkewedColumnValueLocationMaps": {}
                    },
                    "StoredAsSubDirectories": False
                },
                'PartitionKeys': self.preparePartitionKey(),
                'TableType': 'EXTERNAL_TABLE'
            }
            # PartitionIndexes=[
            #     {
            #         'Keys': [
            #             'string',
            #         ],
            #     'IndexName': 'string'
            #     },
            # ]
            response = self.glue_client.create_table(DatabaseName=self.databaseName, TableInput=TableInput)
            logger.info(str(response))
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info("Glue Table Created")
                return True
            else:
                logger.info("Unable to Create Glue Table")
                return False
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def deleteGlueTable(self):
        try:
            logger.info("Deleting Glue Table")
            response = self.glue_client.delete_table(DatabaseName = self.databaseName, Name=self.tableName)
            if isinstance(response, dict):
                return True
            else:
                return False
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def addOldPartition(self,partitionValues):
        try:
            for row in partitionValues:
                partitionPrefix = ''
                partValues = []
                for p in self.partitionKeys:
                    partValues.append(str(row[p["name"]]))
                    if partitionPrefix == '':
                        partitionPrefix = p["name"] + '=' + str(row[p["name"]]) + '/'
                    else:
                        partitionPrefix = partitionPrefix + p["name"] + '=' + str(row[p["name"]]) + '/'
                partitionUri = self.tableLocation + "/" + partitionPrefix
                msg = "Old Partition Location: {}".format(partitionUri)
                logger.info(msg)
                msg = 'Adding partition with values: {}'.format(str(partValues))
                logger.info(msg)
                input_list = []
                # Initializing empty list
                input_dict = {
                    'Values': list(partValues),
                    'StorageDescriptor': {
                        'Location': partitionUri,
                        'InputFormat': "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        'OutputFormat': "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        'SerdeInfo': {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {
                                "serialization.format": "1"
                            }
                        }
                    }
                }
                input_list.append(input_dict.copy())
                createPartitionResponse = self.glue_client.batch_create_partition(DatabaseName=self.databaseName, TableName=self.tableName, PartitionInputList=input_list)
                if len(createPartitionResponse) > 0:
                    msg = "Partition Added Successfuly: {}".format(partitionUri)
                    logger.info(msg)
                else:
                    msg = "ErrorL Partition Addition Unuccessfuly: {}".format(partitionUri)
                    logger.error(msg)
            return True
        except Exception as e:
            logger.error("Error: " + e.__str__())
            return False
    def getOldPpartitionValues(self):
        try:
            msg = "Table Path for {}: {}".format(self.tableName,self.tableLocation)
            logger.info(msg)
            df = spark.read.option("basePath", self.tableLocation).parquet(self.tableLocation + "*")
            partitionKeyList = []
            for p in self.partitionKeys:
                partitionKeyList.append(p["name"])
            msg = 'Old Partition Keys: '+str(partitionKeyList)
            logger.info(msg)
            # df.select(partList)
            # partitionValueListQuery = "SELECT DISTINCT {} FROM {}.{}".format(partitionKeyList,self.databaseName,self.tableName)
            partitionValues = df.select(partitionKeyList).distinct().collect()
            msg = "Old Partition Values: {}".format(str(partitionValues))
            logger.info(msg)
            return partitionValues
        except Exception as e:
            logger.error("Error: " + e.__str__())
            logger.info("Unable to Retrieve Old Partition Details")
            raise Exception(e)

    def updateGluetable(self):
        try:
            # For adding old partitions uncomment the codes
            partitionValues = self.getOldPpartitionValues()
            logger.info("Starting Update Glue Table")
            response = self.deleteGlueTable()
            if response == False:
                logger.info("Unale to delete existing table")
                return False
            createGlueTable = self.createGlueTable()
            if createGlueTable != True:
                logger.info("Glue Table Creation Unsuccessful")
                return False
            else:
                logger.info("Glue Table Created: {}".format(self.tableName))
                # Add the old partitions
                self.addOldPartition(partitionValues)
                return True
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    # get table Defination
    def getTableDefination(self):
        try:
            msg = "Fetching Glue Table details! Database Name: {} and Table Name: {}".format(self.databaseName,self.tableName)
            logger.info(msg)
            response = self.glue_client.get_table(DatabaseName = self.databaseName, Name = self.tableName)
            if len(response['Table']["StorageDescriptor"]['Columns']) != len(self.columnList):
                logger.info("The source glue table number of columns does not match with the input file. Calling Update DDL step")
                updateGluetable = self.updateGluetable()
                if updateGluetable != True:
                    raise Exception("Unable to Update Glue table")
                else:
                    logger.info("Table Metadata Updated")
                    return self.getTableDefination()
            else:
                # Check if any change in column metadata
                existingTableMetadata = []
                newTableMetadata = []
                for r in self.columnList:
                    newTableMetadata.append({
                        "name": r["name"],
                        "data_type": r["data_type"].upper().strip()
                    })
                for r in response['Table']["StorageDescriptor"]['Columns']:
                    existingTableMetadata.append({
                        "name": r["Name"],
                        "data_type": r["Type"].upper().strip()
                    })
                msg = "Existing Table Metadata of Table " + self.tableName + ": " + str(existingTableMetadata)
                logger.info(msg)
                msg = "New Table Metadata of Table " + self.tableName + ": " + str(newTableMetadata)
                logger.info(msg)
                if len(existingTableMetadata) != 0 and len(newTableMetadata) != 0 and existingTableMetadata != newTableMetadata:
                    logger.info("The source glue table metadata does not match with the input file. Calling Update DDL step")
                    updateGluetable = self.updateGluetable()
                    if updateGluetable != True:
                        raise Exception("Unable to Update Glue table")
                    else:
                        logger.info("Table Metadata Updated")
                        return self.getTableDefination()

            logger.info("Glue Table details Fetched")
            return response
        except ClientError as e:
            if "EntityNotFoundException" in e.__str__():
                logger.info("Glue Table Not Found... Caling Create Glue Table")
                createGlueTable = self.createGlueTable()
                if createGlueTable != True:
                    logger.info("Glue Table Creation Unsuccessful")
                    raise Exception(e.__str__())
                else:
                    logger.info("Glue Table Created")
                    return self.getTableDefination()
            else:
                logger.error("Error: " + e.__str__())
                raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
    def generate_partition_input_list(self,tableData):
        input_list = []
        # Initializing empty list
        part_location = tableData['table_location']
        for p in self.partitionKeys:
            pkName = p["name"]
            part_location = part_location + pkName + "=" + self.partitionValues[pkName] + "/"
        input_dict = {
            'Values': list(self.partitionValues.values()),
            'StorageDescriptor': {
                'Location': part_location,
                'InputFormat': tableData['input_format'],
                'OutputFormat': tableData['output_format'],
                'SerdeInfo': tableData['serde_info']
            }
        }
        input_list.append(input_dict.copy())
        return input_list
    def loadGlueTable(self):
        try:
            logger.info("Glue Table Load Partition Utility")
            tableDefination = self.getTableDefination()
            tableData = {}
            tableData['input_format'] = tableDefination['Table']['StorageDescriptor']['InputFormat']
            tableData['output_format'] = tableDefination['Table']['StorageDescriptor']['OutputFormat']
            tableData['table_location'] = tableDefination['Table']['StorageDescriptor']['Location']
            tableData['serde_info'] = tableDefination['Table']['StorageDescriptor']['SerdeInfo']
            tableData['partition_keys'] = tableDefination['Table']['PartitionKeys']
            inputList = self.generate_partition_input_list(tableData)
            createPartitionResponse = self.glue_client.batch_create_partition(DatabaseName=self.databaseName, TableName=self.tableName, PartitionInputList=inputList)
            if len(createPartitionResponse) > 0:
                return True
            else:
                raise Exception("Unable to Update Glue Catalog Table Partition")
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

    def delete_partitions(self,partitions, batch=25):
        # Delete Partitions of Glue Table
        try:
            logger.info("Starting Module to Delete Old Parition of Glue Table")
            for i in range(0, len(partitions), batch):
                to_delete = [{k:v[k]} for k,v in zip(["Values"]*batch, partitions[i:i+batch])]
                self.glue_client.batch_delete_partition(DatabaseName=self.databaseName,TableName=self.tableName,PartitionsToDelete=to_delete)
            logger.info("Delete Old Parition of Glue Table Completed")
            return True
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

    def getAndDeletePartitions(self):
        # Check if table exists
        logger.info("Fetch Old Parition of Glue table Starting")
        try:
            response = self.glue_client.get_table(DatabaseName = self.databaseName, Name = self.tableName)
        except Exception as e:
            logger.info("Table Does not exists... Exiting Delete Partition Module")
            return True
        try:
            paginator = self.glue_client.get_paginator('get_partitions')
            itr = paginator.paginate(DatabaseName=self.databaseName, TableName=self.tableName)
            logger.info("Fetch Old Parition of Glue table Complteted")
            for page in itr:
                logger.info("Partitions to be Deleted: " + str(page["Partitions"]))
                self.delete_partitions(page["Partitions"])
            return True
        except ClientError as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())
        except Exception as e:
            logger.error("Error: " + e.__str__())
            raise Exception(e.__str__())

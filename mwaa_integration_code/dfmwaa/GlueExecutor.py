#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "CTS"

"""
Doc_Type            : Tech Products
Tech Description    :
Pre_requisites      :
Inputs              :
Outputs             :
Example             : NA
Config_file         : NA
Author              : Mrinal Paul
"""
import sys
import boto3
import sys
import logging
import time
from botocore.exceptions import ClientError
import random

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class GlueExecutor():
    def __init__(self,glueJobName,processName,GLUE_JOB_PARAMS,env,WORKER_TYPE='',NUMBER_OF_WORKERS=None):
        session = boto3.session.Session()
        self.glue_client = session.client('glue')
        self.glueJobName = glueJobName
        self.processName = processName
        self.env = env
        self.GLUE_JOB_PARAMS = GLUE_JOB_PARAMS
        self.NumberOfWorkers = NUMBER_OF_WORKERS
        self.WorkerType = WORKER_TYPE

    def run_glue_job(self, arguments = {}):
        try:
            logger.info("Starting Glue ETL Job")
            if self.NumberOfWorkers != None and self.WorkerType != '':
                job_run_id = self.glue_client.start_job_run(JobName=self.glueJobName, Arguments=arguments,WorkerType=self.WorkerType,NumberOfWorkers=self.NumberOfWorkers)
            elif self.NumberOfWorkers == None and self.WorkerType != '':
                # job_run_id = self.glue_client.start_job_run(JobName=self.glueJobName, Arguments=arguments,WorkerType=self.WorkerType)
                msg = "NumberOfWorkers and WorkerType both are required"
                logger.error(msg)
                raise Exception(msg)
            elif self.NumberOfWorkers != None and self.WorkerType == '':
                # job_run_id = self.glue_client.start_job_run(JobName=self.glueJobName, Arguments=arguments,NumberOfWorkers=self.NumberOfWorkers)
                msg = "NumberOfWorkers and WorkerType both are required"
                logger.error(msg)
                raise Exception(msg)
            else:
                job_run_id = self.glue_client.start_job_run(JobName=self.glueJobName, Arguments=arguments)
            logger.info("Glue ETL Job Trigger Succeeded")
            return job_run_id
        except ClientError as e:
            message = "Boto3 client error in running glue Job: " + e.__str__()
            logger.error(message)
            raise Exception(message)
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            message = e.__str__()
            raise Exception(message)

    def glue_job_status(self,runId):
        try:
            response = self.glue_client.get_job_run(
                JobName=self.glueJobName,
                RunId=runId,
                PredecessorsIncluded=False
            )
            print(response["JobRun"]["JobRunState"])
            if response["JobRun"]["JobRunState"] == "RUNNING":
                logger.info("Running Glue ETL Job " + self.glueJobName + " With Run ID " + runId)
                randomSleepTime = random.randint(30,90)
                time.sleep(randomSleepTime)
                self.glue_job_status(runId)
            elif response["JobRun"]["JobRunState"] == "SUCCEEDED":
                logger.info("Glue ETL Job Successfull")
                return "SUCCESS"
            elif response["JobRun"]["JobRunState"] == "FAILED":
                ErrorMessage = response["JobRun"]["ErrorMessage"]
                raise Exception(ErrorMessage)
            else:
                ErrorMessage = response["JobRun"]["ErrorMessage"]
                raise Exception(ErrorMessage)
            return "SUCCESS"
        except ClientError as e:
            message = "Boto3 client error in running glue Job: " + e.__str__()
            logger.error(message)
            message = "Boto3 client error in checking status of glue Job: " + e.__str__()
            raise Exception(message)
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            message = e.__str__()
            raise Exception(message)

    def getLatestRun(self):
        try:
            response = self.glue_client.get_job_runs(JobName = self.glueJobName)
            jobRunId = response["JobRuns"][0]["Id"]
            if response["JobRuns"][0]["JobRunState"] == "RUNNING":
                # One instance of job is already running
                msg = "Job already running with Run ID:" + str(jobRunId)
                logger.info(msg)
                return jobRunId
            else:
                msg = "No job is in running status!"
                logger.info(msg)
                return False
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            return False

    def GlueExecutor(self):
        try:
            argument = {
                "--enable-auto-scaling": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-continuous-log-filter": "true"
            }
            
            for param, value in self.GLUE_JOB_PARAMS.items():
                param = param.strip("--")
                param = "--"+param
                argument[param] = value
            argument["--env"] = self.env
            msg = "Triggering new instance of Job: " + str(self.glueJobName)
            logger.info(msg)
            runId = self.run_glue_job(argument)
            runId = runId.get("JobRunId")
            logger.info("The Glue ETL Job is running with Run ID: " + str(runId))
            logger.info("Checing the status of job with Run ID: " + str(runId))
            status = self.glue_job_status(runId)
            return status
        except Exception as e:
            message = "Error: " + e.__str__()
            logger.error(message)
            raise Exception(message)
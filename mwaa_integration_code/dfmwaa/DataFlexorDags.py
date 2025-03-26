# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
"""This module fetches batch status.
Type                : Tech Products
Description         : This Module Triggers Glue Jobs from MWAA DAGs
Author              : Mrinal Paul
"""
import logging
import traceback
from urllib.parse import urlparse
import os

from dfmwaa import loadNotification
from dfmwaa import GlueExecutor
from dfmwaa import SparcUtility

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)

def sendNotification(processName,processStatus,env,appName,NOTIFY,stepName="",errorShortDesc="",errorDesc=""):
    try:
        if NOTIFY:
            loadNotificationObj = loadNotification()
            status = loadNotificationObj.sendNotification(processName,processStatus,env,appName,stepName,errorShortDesc,errorDesc)
    except Exception as e:
        logger.error("Error occured during sending notification: ", str(e))
        raise e
def triggerSparcUtility(processName, dagRunId, stepName, env, appName,errorFullDessc,NOTIFY):
    """This method triggers SNOW Utility"""
    try:
        incident_data = {}
        logger.info("Staring function to trigger SNOW Utility")

        logger.info("Preparing the information to be added to the Incident")
        incident_data["processName"] = processName
        incident_data["stepName"] = stepName
        incident_data["errorFullDessc"] = errorFullDessc
        incident_data["errorShortDessc"] = env.upper() + " Process " + processName + " failed at Step " + stepName + " ("+dagRunId+")"
        sendNotification(processName,'failed',env,appName,NOTIFY,stepName,incident_data["errorShortDessc"],errorFullDessc)
        snow_obj = SparcUtility(appName)
        output = snow_obj.createsparcticket(incident_data)
        logger.info("Output of SNOW Trigger: " + str(output))
    except Exception as e:
        logger.error(str(traceback.format_exc()))
        logger.error("Failed while triggering SNOW Utility. Error: ", str(e))
        raise e

def startTask(PROCESS_NAME,processStatus,appName,env,NOTIFY,dagRunId,STEP_NAME):
    try:
        msg = "Starting Process: " + PROCESS_NAME
        logger.info(msg)
        msg = "Environment: " + env
        logger.info(msg)
        sendNotification(PROCESS_NAME,processStatus,env,appName,NOTIFY)
        return True
    except Exception as e:
        triggerSparcUtility(PROCESS_NAME,dagRunId,STEP_NAME, env,appName,e,NOTIFY)
        logger.error("Error: " + e.__str__())
        raise Exception(e.__str__())
def endTask(PROCESS_NAME,processStatus,appName,env,NOTIFY,dagRunId,STEP_NAME):
    try:
        msg = "Process Ended: " + PROCESS_NAME
        logger.info(msg)
        sendNotification(PROCESS_NAME,processStatus,env,appName,NOTIFY)
        return True
    except Exception as e:
        triggerSparcUtility(PROCESS_NAME,dagRunId,STEP_NAME, env,appName,e,NOTIFY)
        logger.error("Error: " + e.__str__())
        raise Exception(e.__str__())

def call_job_executor(PROCESS_NAME, APPLICATION_NAME=None, SUB_APPLICATION_NAME=None,GLUE_JOB_NAME=None,GLUE_JOB_PARAMS={},WORKER_TYPE='',NUMBER_OF_WORKERS=None,DAG_ENV=None,NOTIFY=True, WORKSPACE_NAME=None, **kwargs):
    os.environ["WORKSPACE_NAME"] = str(WORKSPACE_NAME)
    env = str(list(set(os.environ["AIRFLOW_ENV_NAME"].lower().split("-")) & set(["dev","tst","prd"]))[0]).lower()
    if APPLICATION_NAME != None:
        appName = APPLICATION_NAME
    else:
        appName = os.environ["AIRFLOW_ENV_NAME"].lower().split("-")[0]
    if SUB_APPLICATION_NAME != None:
        appName = SUB_APPLICATION_NAME
    if DAG_ENV != None:
        env = DAG_ENV
    """This method triggers Glue ETL Jobs"""
    try:
        context = kwargs
        task_id = context['task'].task_id.lower()
        dagRunId = str(context['dag_run'].run_id)
        if "dag_run" in context:
            dagRunId = str(context['dag_run'].run_id)
        else:
            dagRunId = ""
        STEP_NAME = context['task'].task_id
        match task_id:
            case "start":
                res = startTask(PROCESS_NAME,'started',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case "end":
                res = endTask(PROCESS_NAME,'ended',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case "Start":
                res = startTask(PROCESS_NAME,'started',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case "End":
                res = endTask(PROCESS_NAME,'ended',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case "START":
                res = startTask(PROCESS_NAME,'started',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case "END":
                res = endTask(PROCESS_NAME,'ended',appName,env,NOTIFY,dagRunId,STEP_NAME)
                return res
            case _:
                if GLUE_JOB_NAME == None:
                    msg = "GLUE_JOB_NAME is required"
                    logger.error(msg)
                    raise Exception(msg)
                GLUE_JOB_PARAMS["run_id"] = str(context['dag_run'].run_id)
                GlueExecutorObj = GlueExecutor(GLUE_JOB_NAME,PROCESS_NAME,GLUE_JOB_PARAMS,env,WORKER_TYPE,NUMBER_OF_WORKERS)
                status = GlueExecutorObj.GlueExecutor()
                if status == "SUCCESS":
                    logger.info("Step Completed Successfully")
                else:
                    raise Exception("Step Failed")
    except Exception as e:
        triggerSparcUtility(PROCESS_NAME,dagRunId,STEP_NAME, env,appName,e,NOTIFY)
        raise Exception(e)
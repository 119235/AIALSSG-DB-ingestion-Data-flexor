# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
__author__ = "Mrinal Paul"

"""
Doc_Type            : Tech Products
Tech Description    : This Utility call SNOW API to create Incident on failue of any DAGs. The utility can be called on exception occured while running the DAG. This Utility                       will not trigger and Incident if the DAG fails at Dummy node (E.g. Start or End Step). The incidents will be created with priority 4 (Low). User need to manually resolve the ticked once the issue is
                      resolved. Multiple failue of same step will create multiple incidents.
Pre_requisites      : SNOW Utility should be integrated with existing or new utility in order to generate the Incident automatically.
Inputs              : {
                        "processName": "Name of Process/DAG",
                        "stepName": "Name of The Step",
                        "errorFullDessc": "Error Short Description",
                        "errorShortDessc": "Error Description"
                      }
Outputs             : Incident details in JSON format
Example             : NA
Config_file         : NA
Author              : Mrinal Paul
"""
import os,boto3
from pickle import FALSE
import sys
import requests
import json
import re
import logging
from DataFlexor import secretManager
import environmentParams as environmentParams

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)

class SparcUtility():
    def __init__(self,appName):
        self.appName = appName
    def createsparcticket(self,incident_data):
        try:
            logger.info("Starting Process to create Incident")
            #Retrieving API data from secrets manager
            secObj = secretManager()
            workSpaceName = environmentParams.WORKSPACE
            if workSpaceName != None and workSpaceName != "":
                secretName = workSpaceName + "/" +self.appName + "_service_now"
            else:
                secretName = self.appName + "_service_now"
            secret = secObj.getSecret(secretName)
            # API URL
            url =  secret["url"]
            # API credentials
            user = secret["user"]
            pwd = secret["pwd"]
            # Set Request headers
            headers = {"Accept":"application/json"}
            # call the API to create incident
            logger.info(incident_data)
            logger.info(incident_data["errorShortDessc"])
            logger.info(incident_data["errorFullDessc"])
            logger.info("Starting: Get the SPARC Group Mapping")
            json_data = {
                "cmdb_ci":secret["cmdb_ci"],
                "short_description": str(incident_data["errorShortDessc"]),
                "description": str(incident_data["errorFullDessc"]),
                "assignment_group":secret["assignment_group_id"],
                "u_reporting_customer":secret["u_reporting_customer"],
                "urgency":"3",
                "impact":"3"
            }
            response = requests.post(url, auth=(user, pwd), headers=headers ,data=str(json_data))
            data=response.json()
            logger.info(data)
            if response.status_code != 201:
                STATUS_CODE = False
                logger.info('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
            else:
                STATUS_CODE = True
                logger.info("Incident Created: " + str(data["result"]["number"]))
            return STATUS_CODE
        except Exception as ex:
            msg = "Exception in Calling SNOW API: " + str(ex)
            logger.error(msg)
            raise ex
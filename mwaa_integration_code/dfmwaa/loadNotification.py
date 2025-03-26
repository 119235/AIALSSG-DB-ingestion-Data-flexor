"""
Author
Module Name         :   Common Library
Purpose             :   This Module will trigger Notification to MA stakeholder once
Input Parameters    :
Output              :   JSON/Bool
Predecessor module  :   NA
Successor module    :   NA
Pre-requisites      :   NA
Last changed on     :   2023-05-02
Last changed by     :   Mrinal Paul
Reason for change   :   New Function added
"""
import sys
import os
from datetime import datetime,timedelta
import boto3
import math
from dfmwaa import notification,secretManager

import logging
logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)

class loadNotification:
    def __init__(self):
        self.today = datetime.now()
    def generate_email_body(self,processName,processStatus,appName,stepName="",errorShortDesc="",errorDesc=""):
        html = '''<head>
        <style>
        .style0
            {mso-number-format:General;
            text-align:general;
            vertical-align:bottom;
            white-space:nowrap;
            mso-rotate:0;
            mso-background-source:auto;
            mso-pattern:auto;
            color:black;
            font-size:11.0pt;
            font-weight:400;
            font-style:normal;
            text-decoration:none;
            font-family:Calibri, sans-serif;
            mso-font-charset:0;
            border:none;
            mso-protection:locked visible;
            mso-style-name:Normal;
            mso-style-id:0;}
        td
            {mso-style-parent:style0;
            padding-top:1px;
            padding-right:1px;
            padding-left:1px;
            mso-ignore:padding;
            color:black;
            font-size:11.0pt;
            font-weight:400;
            font-style:normal;
            text-decoration:none;
            font-family:Calibri, sans-serif;
            mso-font-charset:0;
            mso-number-format:General;
            text-align:general;
            vertical-align:bottom;
            border:none;
            mso-background-source:auto;
            mso-pattern:auto;
            mso-protection:locked visible;
            white-space:nowrap;
            mso-rotate:0;}
        .even {
            mso-style-parent:style0;
            color:black;
            text-align:center;
            vertical-align:middle;
            background:#B7DEE8;
            mso-pattern:black none;
            white-space:normal;
        }
        .odd {
        mso-style-parent:style0;
            color:black;
            text-align:center;
            vertical-align:middle;
            background:#DAEEF3;
            mso-pattern:black none;
            white-space:normal;
        }
        tr th {
            mso-style-parent:style0;
            color:white;
            text-align:center;
            vertical-align:middle;
            background:#70AD47;
            mso-pattern:black none;
            white-space:normal;
        }
        </style>
        </head>
        <body link="#0563C1" vlink="#954F72" lang=EN-US style='tab-interval:.5in; word-wrap:break-word'><span></br>Hi All,</br>
        '''
        if processStatus == "started":
            html += "</br>The Execution of " + processName + " has started in "+appName.upper()+" Datalake. </br>"
        elif processStatus == "ended":
            html += "</br>The Execution of " + processName + " Load is Completed in "+appName.upper()+" Datalake. </br>"
        elif processStatus == "failed":
            html += "</br>The Execution of " + processName + " Load is Failed in step name "+stepName+" </br></br><b><i>Error Short Description:</i></b> " + errorShortDesc + " </br></br><b><i>Error Description: </i></b>" + str(errorDesc)
        # Lofic for generating notification from new design ends
        html += "</br></br></br>Regards,</br>"+appName.upper()+" Datalake DevOps</body></html>"
        return html
    def sendNotification(self,processName,processStatus,env,appName,stepName="",errorShortDesc="",errorDesc=""):
        try:
            notificationObj = notification()
            # print("Get data from Secretmanager")
            secretManagerObj = secretManager()
            workSpaceName = os.environ["WORKSPACE_NAME"]
            if workSpaceName != None and workSpaceName != "":
                secretName = workSpaceName+"/"+appName + "_load_notification"
            else:
                secretName = appName + "_load_notification"
            secret = secretManagerObj.getSecret(secretName)
            emailSender = secret["emailSender"]
            emailRecipientList = secret["emailRecipientList"]
            ccEmailRecipientList = secret["ccEmailRecipientList"]
            bccEmailRecipientList = secret["bccEmailRecipientList"]
            if env != None and env != "":
                emailSubject = "("+env.upper()+") "+appName.upper() + " Process "+ processName + " Status Update for " + str(datetime.today().date())
            else:
                emailSubject = appName.upper() + " Process "+ processName + " Status Update for " + str(datetime.today().date())
            emailBody = self.generate_email_body(processName,processStatus,appName,stepName,errorShortDesc,errorDesc)
            output = notificationObj.sendEmail(emailSubject, emailBody, emailSender, emailRecipientList, ccEmailRecipientList,bccEmailRecipientList)
            if output != True:
                raise Exception(str(output))
            else:
                print("Email Sent")
                return True
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise e
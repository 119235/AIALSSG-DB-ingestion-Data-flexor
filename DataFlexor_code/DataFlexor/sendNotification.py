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
from datetime import datetime,timedelta
import sys
import boto3
import math
from DataFlexor import notification,secretManager
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
from pyspark.sql.types import *

class sendNotification:
    def __init__(self):
        self.today = datetime.now()
        env = None
        if environmentParams.PLATFORM_NAME.lower() == "glue":
            self.args = getResolvedOptions(sys.argv, ["env"])
        else:
            self.args = dbutils.widgets.getAll()
            env = environmentParams.ENV_NAME.lower()
        if env != None:
            self.env = env
        else:
            self.env = self.args['env']
    def generate_email_body(self,datasetName,appName,executionCycleId,processName):
        try:
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
                background:#FFFFFF;
                mso-pattern:black none;
                white-space:normal;
            }
            .odd {
            mso-style-parent:style0;
                color:black;
                text-align:center;
                vertical-align:middle;
                background:#D9E2F3;
                mso-pattern:black none;
                white-space:normal;
            }
            tr th {
                mso-style-parent:style0;
                color:white;
                text-align:center;
                vertical-align:middle;
                background:#002060;
                mso-pattern:black none;
                white-space:normal;
            }
            </style>
            </head>
            <body link="#0563C1" vlink="#954F72" lang=EN-US style='tab-interval:.5in; word-wrap:break-word'><span></br>Hi All,</br></br>Please find the Pre-Ingstion DQM Details of Dataset "'''+datasetName+'''" for the ''' + str(str(datetime.utcnow().strftime("%Y-%m-%d"))) + ''' run.</br></br></br>
            <table border=0 cellpadding=0 cellspacing=0 width=3800 style='border-collapse: collapse;table-layout:fixed;width:2500pt;mso-padding-bottom-alt:0in; mso-padding-left-alt:0in;mso-padding-right-alt:0in;mso-padding-top-alt:0in; mso-yfti-tbllook:1184'> <tr>
            <th height=58 class=xl65 width=65 style='height:43.5pt;width:49pt;padding-bottom: 0in;padding-top:.75pt'>Sr.</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>File Name</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>QC ID</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>QC Message</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Criticality</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Source Record Count</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Total Records Ingested</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Total Valid Records</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Total Error Records</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Allowed Error Threshold</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Error Percentage</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Execution Start Time</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Execution End Time</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>QC Passed</th>
            <th class=xl65 width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>Ingestion Status</th></tr>
            '''
            applicationName = environmentParams.APPLICATION_NAME.lower()
            try:
                subApplicationName = environmentParams.SUB_APPLICATION_NAME.lower()
                if subApplicationName != None and subApplicationName != "":
                    applicationName = subApplicationName
            except:
                logger.info("Not a Sub Application")
            logTableName = "log_dqm_dg_{}_{}".format(applicationName.replace("-","_"),"ing")
            fileLogTable = "file_log_"+applicationName.lower()
            query = "SELECT file_name, total_record_count, validated_record_count, errored_record_count,file_name,record_count, execution_start_time, execution_end_time, ingestion_status, qc_id, message, error_percentage, threshold, criticality, qc_passed_flag FROM (SELECT dataset_name, qc_id, criticality, pre_ingestion_dqm_file, total_record_count, validated_record_count, errored_record_count, message, error_percentage, threshold, qc_passed_flag FROM `{}`.{} WHERE dataset_name = '{}' AND ingestion_cycle_id = '{}') A LEFT JOIN (SELECT dataset_name, file_name,record_count, execution_start_time, execution_end_time, ingestion_status FROM `{}`.{} WHERE dataset_name = '{}' AND execution_cycle_id = '{}') B ON A.dataset_name = B.dataset_name AND A.pre_ingestion_dqm_file = B.file_name ORDER BY B.file_name,qc_id".format(environmentParams.LOG_SCHEMA.replace("`",""),logTableName,datasetName,executionCycleId,environmentParams.LOG_SCHEMA.replace("`",""),fileLogTable,datasetName,executionCycleId)
            msg = "Notification Query: " + query
            logger.info(msg)
            df = spark.sql(query)
            sr = 1
            for row in df.rdd.toLocalIterator():
                if (sr % 2) == 0:
                    clield_class = "even"
                else:
                    clield_class = "odd"
                if row["threshold"] == None:
                    threshold = ''
                else:
                    threshold = str(row["threshold"]) + "%"

                if row["error_percentage"] == None:
                    errorPercentage = "0%"
                else:
                    errorPercentage = str(row["error_percentage"]) + "%"
                html += '''<tr><td class = '''+clield_class+''' width=65 style='width:49pt;padding-bottom: 0in;padding-top:.75pt'>'''+str(sr)+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["file_name"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["qc_id"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["message"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["criticality"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["total_record_count"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["record_count"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["validated_record_count"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["errored_record_count"])+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+threshold+'''</td>
                <td class = ''' + clield_class + ''' width=165 style='width:124pt;padding-bottom:0in;padding-top: .75pt'>'''+errorPercentage+'''</td>
                <td class = '''+clield_class+''' width=249 style='width:187pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["execution_start_time"])+'''</td>
                <td class = '''+clield_class+''' width=249 style='width:187pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["execution_end_time"])+'''</td>
                <td class = '''+clield_class+''' width=249 style='width:187pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["qc_passed_flag"])+'''</td>
                <td class = '''+clield_class+''' width=204 style='width:153pt;padding-bottom:0in;padding-top: .75pt'>'''+str(row["ingestion_status"])+'''</td></tr>'''
                sr = sr + 1
            # Lofic for generating notification from new design ends
            html += "</table></br></br></br>Regards,</br>"+appName.upper()+" DevOps</body></html>"
            return html
        except Exception as e:
            msg = "Error: Error in Generating Email Body"
            logger.error(msg)
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
            raise e
    def sendNotification(self,datasetName,executionCycleId,processName="",workSpaceName="",dqmInd=False):
        try:
            appName = environmentParams.APPLICATION_NAME.lower()
            try:
                subAppName = environmentParams.SUB_APPLICATION_NAME.lower()
                if subAppName != None and subAppName != "":
                    appName = subAppName
            except:
                logger.info("Not a Sub Application")
            notificationObj = notification()
            logger.info("Get data from Secretmanager")
            secretManagerObj = secretManager()
            if dqmInd == True:
                secretName = workSpaceName+"/"+appName + "_dqm_notification"
            else:
                secretName = workSpaceName+"/"+appName + "_load_notification"
            secret = secretManagerObj.getSecret(secretName)
            emailSender = secret["emailSender"]
            emailRecipientList = secret["emailRecipientList"]
            ccEmailRecipientList = secret["ccEmailRecipientList"]
            bccEmailRecipientList = secret["bccEmailRecipientList"]
            if processName == "":
                emailSubject = "(" + self.env.upper() + ") " + appName.upper() + " Dataset "+ datasetName + " Pre-Ingestion DQM Status for " + str(datetime.today().date())
            else:
                emailSubject = "(" + self.env.upper() + ") " + appName.upper() + " Dataset "+ datasetName + " Under Process " + processName + " Pre-Ingestion DQM Status for " + str(datetime.today().date())

            msg = "Email Subject: " + emailSubject
            logger.info(msg)
            emailBody = self.generate_email_body(datasetName,appName,executionCycleId,processName)
            msg = "Email Body Generated"
            logger.info(msg)
            output = notificationObj.sendEmail(emailSubject, emailBody, emailSender, str(emailRecipientList), str(ccEmailRecipientList),str(bccEmailRecipientList))
            logger.info(str(output))
            if output != True:
                raise Exception(str(output))
            else:
                logger.info("Email Sent")
                return True
        except Exception as e:
            msg = "Error: " + str(e.__str__())
            logger.error(msg)
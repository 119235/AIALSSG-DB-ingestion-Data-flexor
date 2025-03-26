"""
Author              : Mrinal Paul
Owner               : Medical Affairs DevOps
Date                : 2023-05-09
Doc_Type            : Tech Products
Description         : This utility is used for sending Notification emails using AWS SES service. Default AWS
                      Region for SES is "us-west-2" if not defind by calling script.
Pre_requisites      :
Inputs              :
Outputs             :
Example             :
Config_file         : NA
Last changed on     : 2023-05-09
Last changed by     : Mrinal Paul
Reason for change   : New Module
"""

import os
import argparse
import json
import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import boto3
import ast
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
import base64
import json

class notification:
    def __init__(self,sesRegion='us-west-2'):
        self.sesRegion = sesRegion

    def emailValidator(self, emailSubject, emailBody, emailSender, emailRecipientList):
        """
        Purpose   :   This function will validate all the inputs
        Input     :
        Output    :   Returns True if all the inputs are validated else raises Exception
        """
        if emailSubject is None or emailSubject == "":
            statusMessage = "Email Subject cannot be Empty for Sending Email Notifications"
            raise Exception(statusMessage)

        if emailSender is None or emailSender == "":
            statusMessage = "Email Sender cannot be Empty for Sending Email Notifications"
            raise Exception(statusMessage)

        if emailRecipientList is None or emailRecipientList == "":
            statusMessage = "Email Recipient list should be a comma separated string"
            raise Exception(statusMessage)

        if emailBody is None or emailBody == "":
            statusMessage = "Email Boby cannot be Empty for Sending Email Notifications"
            raise Exception(statusMessage)
        return True

    def sendEmail(self, emailSubject, emailBody, emailSender, emailRecipientList, ccEmailRecipientList=None,bccEmailRecipientList=None):
        """Function to send Email"""
        try:
            msg = "Email Subject: {}".format(emailSubject)
            logger.info(msg)
            # msg = "Email Body: {}".format(emailBody)
            # logger.info(msg)
            msg = "Email Sender: {}".format(emailSender)
            logger.info(msg)
            msg = "Email Recipients: {}".format(emailRecipientList)
            logger.info(msg)
            msg = "Email CC: {}".format(str(ccEmailRecipientList))
            logger.info(msg)
            msg = "Email BCC: {}".format(str(bccEmailRecipientList))
            logger.info(msg)
            logger.info("Sending Email")
            if self.emailValidator(emailSubject, emailBody, emailSender, emailRecipientList):
                logger.info("Email Inputs Validated successfully" + str(emailRecipientList))
                email = MIMEMultipart()
                email["Subject"] = str(emailSubject)
                email["From"] = str(emailSender)
                email["To"] = str(emailRecipientList)
                if ccEmailRecipientList != None and ccEmailRecipientList != "":
                    email["Cc"] = str(ccEmailRecipientList)
                if bccEmailRecipientList != None and bccEmailRecipientList != "":
                    email["Bcc"] = str(ccEmailRecipientList)
                emailBody = MIMEText(emailBody, _subtype="html")
                email.attach(emailBody)
                with smtplib.SMTP("mailrelay.gilead.com",25) as server:
                    server.ehlo()
                    server.send_message(email)
                    logger.info("Email Sent!!!")
                return True
        except Exception as e:
            logger.error(e.__str__())
            raise Exception(e.__str__())
    def sendEmailFileS3(self, emailSubject, emailBody, s3Bucket, s3Key, emailSender, emailRecipientList, ccEmailRecipientList=None,bccEmailRecipientList=None):
        """Function to send Email"""
        try:
            if self.emailValidator(emailSubject, emailBody, emailSender, emailRecipientList):
                logger.info("Email Inputs Validated successfully" + str(emailRecipientList))
                msg = "Email Subject: {}".format(emailSubject)
                logger.info(msg)
                msg = "Email Body: {}".format(emailBody)
                logger.info(msg)
                msg = "Email Sender: {}".format(emailSender)
                logger.info(msg)
                msg = "Email Recipients: {}".format(emailRecipientList)
                logger.info(msg)
                msg = "Email CC: {}".format(str(ccEmailRecipientList))
                logger.info(msg)
                msg = "Email BCC: {}".format(str(bccEmailRecipientList))
                logger.info(msg)
                email = MIMEMultipart()
                email["Subject"] = emailSubject
                email["From"] = emailSender
                email["To"] = emailRecipientList
                if ccEmailRecipientList != None or ccEmailRecipientList != "":
                    email["Cc"] = ccEmailRecipientList
                if bccEmailRecipientList != None or bccEmailRecipientList != "":
                    email["Bcc"] = ccEmailRecipientList
                emailBody = MIMEText(emailBody, _subtype="html")
                email.attach(emailBody)

                logger.info("Reading S3 File!!!")
                s3Object = boto3.client('s3', 'us-west-2')
                s3Object = s3Object.get_object(Bucket=s3Bucket, Key=s3Key)
                body = s3Object['Body'].read()
                fileName=s3Key.split("/")[-1]
                part = MIMEApplication(body, fileName)
                part.add_header("Content-Disposition", 'attachment', filename=fileName)
                logger.info("Attaching File")
                email.attach(part)
                logger.info("Sending Email")
                with smtplib.SMTP("mailrelay.gilead.com",25) as server:
                    server.ehlo()
                    server.send_message(email)
                    logger.info("Email Sent!!!")
                return True
        except Exception as e:
            logger.error(e.__str__())
            raise Exception(e.__str__())
    def sendEmailAttachment(self, emailSubject, emailBody, data, fileName, emailSender, emailRecipientList, ccEmailRecipientList=None,bccEmailRecipientList=None):
        """Function to send Email"""
        try:
            if self.emailValidator(emailSubject, emailBody, emailSender, emailRecipientList):
                logger.info("Email Inputs Validated successfully" + str(emailRecipientList))
                msg = "Email Subject: {}".format(emailSubject)
                logger.info(msg)
                msg = "Email Body: {}".format(emailBody)
                logger.info(msg)
                msg = "Email Sender: {}".format(emailSender)
                logger.info(msg)
                msg = "Email Recipients: {}".format(emailRecipientList)
                logger.info(msg)
                msg = "Email CC: {}".format(str(ccEmailRecipientList))
                logger.info(msg)
                msg = "Email BCC: {}".format(str(bccEmailRecipientList))
                logger.info(msg)
                email = MIMEMultipart()
                email["Subject"] = emailSubject
                email["From"] = emailSender
                email["To"] = emailRecipientList
                if ccEmailRecipientList != None or ccEmailRecipientList != "":
                    email["Cc"] = ccEmailRecipientList
                if bccEmailRecipientList != None or bccEmailRecipientList != "":
                    email["Bcc"] = ccEmailRecipientList
                emailBody = MIMEText(emailBody, _subtype="html")
                email.attach(emailBody)
                part = MIMEApplication(data, fileName)
                part.add_header("Content-Disposition", 'attachment', filename=fileName)
                logger.info("Attaching File")
                email.attach(part)
                logger.info("Sending Email")
                with smtplib.SMTP("mailrelay.gilead.com",25) as server:
                    server.ehlo()
                    server.send_message(email)
                    logger.info("Email Sent!!!")
                return True
        except Exception as e:
            logger.error(e.__str__())
            raise Exception(e.__str__())
__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Common Function Library
   Purpose             :   This module will provide commol function that can be used accross all the ETL script in Glue

   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   New Function added
"""
import boto3
from botocore.exceptions import ClientError
import base64
import json
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

class secretManager:
    def __init__(self,region_name='us-west-2'):
        # create S3 Client
        self.secret_client = boto3.client(
            service_name='secretsmanager',
            region_name=region_name
        )

    def getSecret(self, secret_name):
        try:
            logger.info("Fetching the details for the secret name %s" % secret_name)
            get_secret_value_response = self.secret_client.get_secret_value(
                SecretId=secret_name
            )
            logger.info("Fetched the Encrypted Secret from Secrets Manager for %s" % secret_name)
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                logger.info("Decrypted the Secret")
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                logger.info("Decrypted the Secret")
            return json.loads(secret)
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise exc
            elif exc.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise exc
            elif exc.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise exc
            elif exc.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise exc
            elif exc.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise exc
            else:
                raise exc
        except Exception as exc:
            logger.exception(exc, exc_info=True)
__AUTHOR__ = 'Mrinal Paul'

"""
   Module Name         :   Input File Pattern Validator Module
   Purpose             :   This Module is used for Validating File Name Pattern of Input File

   Last changed on     :   2023-05-02
   Last changed by     :   Mrinal Paul
   Reason for change   :   New Function added
"""
import re
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

class patternValidator(object):
    @staticmethod
    def len_check(source, pattern):
        try:
            return len(source) == len(pattern)
        except Exception as e:
            # print(f"Exception occured in length check {str(e)}")
            msg = "Exception occured in length check: " + str(e)
            logger.error(msg)
            return False

    def patternValidator(self, source, pattern, customPattern=False):
        msg = "Source: " + source + " --- Pattern: " + pattern
        logger.info(msg)
        if customPattern:
            logger.info("Start Executing Custom Validator")
            try:
                from customPatternValidator import customPatternValidator
                customPatternValidatorObj = customPatternValidator()
                validPattern = customPatternValidatorObj.patternValidator(source, pattern)
                if validPattern:
                    logger.info("Custom Pattern is Valid")
                    return True
                else:
                    logger.info("Custom Pattern is not Valid")
                    return False
            except Exception as e:
                msg = "Error: " + str(e.__str__())
                logger.error(msg)
                return False
        if pattern.startswith("*") or pattern.endswith("*"):
            if pattern.startswith("*"):
                if "." in source:
                    ext = pattern.split(".")[-1]
                    if not source.endswith(ext):
                        return False
            pattern = pattern.strip("*")
            if "YYYYMMDDHHMMSS" in pattern:
                regex = pattern.replace(
                    "YYYYMMDDHHMMSS", "([0-9]{4})(1[0-2]|0[1-9])(0[1-9]|[1-2][0-9]|3[0-1])(0[0-9]|1[0-9]|2[0-3])(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])"
                )
                regex = re.compile(regex)
                if regex.search(source):
                    return True
                return False
            elif "YYYYMMDD_HHMMSS" in pattern:
                regex = pattern.replace(
                    "YYYYMMDD_HHMMSS", "([0-9]{4})(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_([0-9]{6})"
                )
                regex = re.compile(regex)
                if regex.search(source):
                    return True
                return False
            elif "YYYY-MM-DD" in pattern:
                regex = pattern.replace(
                    "YYYY-MM-DD", "([0-9]{4})-(1[0-2]|0[1-9])-(0[1-9]|[1-2][0-9]|3[0-1])"
                )
                regex = re.compile(regex)
                if regex.search(source):
                    return True
                return False
            elif "MMYYYY" in pattern:
                regex = re.compile(r"(1[0-2]|0[1-9])([0-9]{4})")
                if regex.search(source):
                    return True
                return False
            elif "YYYYMMDD" in pattern:
                date_pattern = pattern.replace(
                    "YYYYMMDD", "([0-9]{4})(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])"
                )
                regex = re.compile(date_pattern)
                if regex.search(source):
                    return True
                return False
            elif "YYYYMM" in pattern:
                regex = re.compile(r"([0-9]{4})(1[0-2]|0[1-9])")
                if regex.search(source):
                    start_name = pattern.split("YYYYMM")[0]
                    if source.startswith(start_name):
                        return True
                return False
            elif "MM-DD-YYYY" in pattern:
                date_pattern = pattern.replace(
                    "MM-DD-YYYY", "(1[0-2]|0[1-9])-(0[1-9]|[1-2][0-9]|3[0-1])-([0-9]{4})"
                )
                regex = re.compile(date_pattern)
                if regex.search(source):
                    return True
                return False
            # for handling month in alphabets
            elif "DDMONYYYY" in pattern:
                date_pattern = pattern.replace(
                    "DDMONYYYY", "(3[01]|[12][0-9]|0[1-9])([A-Z]{3})([0-9]{4})"
                )
                regex = re.compile(date_pattern)
                if regex.search(source):
                    return True
                return False
            elif "YYYY_MM_DD_######" in pattern:
                regex = pattern.replace(
                    "YYYY_MM_DD_######",
                    "([0-9]{4})_(1[0-2]|0[1-9])_(3[01]|[12][0-9]|0[1-9])_([0-9]{6})",
                )
                pattern = re.compile(regex)
                if pattern.search(source):
                    return True
                else:
                    return False
            elif "YYYYQ#" in pattern:
                regex = re.compile(r"([0-9]{4}Q[1-4]{1})")
                if pattern.search(source):
                    return True
                else:
                    return False
            elif "YYYYQ" in pattern:
                date_pattern = pattern.replace("YYYYQ", "([0-9]{4})([1-4]{1})")
                regex = re.compile(date_pattern)
                if regex.search(source):
                    return True
                return False
            elif "N_YYYYMM" in pattern:
                regex = pattern.replace("N_YYYYMM", "([0-9])_([0-9]{4})(1[0-2]|0[1-9])")
                pattern = re.compile(regex)
                if pattern.search(source):
                    return True
                return False
            elif "YYYY" in pattern:
                regex = re.compile(r"([0-9]{4})")
                if regex.search(source):
                    return True
            elif re.search(pattern, source):
                return True
            else:
                return False
            return False
        else:
            if "YYYYMMDDHHMMSS" in pattern:
                if self.len_check(source, pattern):
                    regex = pattern.replace(
                        "YYYYMMDDHHMMSS", "([0-9]{4})(1[0-2]|0[1-9])(0[1-9]|[1-2][0-9]|3[0-1])(0[0-9]|1[0-9]|2[0-3])(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])"
                    )
                    regex = re.compile(regex)
                    if regex.match(source):
                        return True
                return False
            elif "YYYYMMDD_HHMMSS" in pattern:
                regex = pattern.replace(
                    "YYYYMMDD_HHMMSS", "([0-9]{4})(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_([0-9]{6})"
                )
                regex = re.compile(regex)
                if regex.search(source):
                    return True
                return False
            elif "YYYY-MM-DD" in pattern:
                if self.len_check(source, pattern):
                    regex = pattern.replace(
                        "YYYY-MM-DD", "([0-9]{4})-(1[0-2]|0[1-9])-(0[1-9]|[1-2][0-9]|3[0-1])"
                    )
                    pattern = re.compile(regex)
                    if pattern.match(source):
                        return True
                return False
            elif "MMYYYY" in pattern:
                if self.len_check(source, pattern):
                    regex = re.compile(r"(1[0-2]|0[1-9])([0-9]{4})")
                    if regex.search(source):
                        start_name = pattern.split("MMYYYY")[0]
                        if source.startswith(start_name):
                            return True
                return False
            elif "YYYYMMDD" in pattern:
                if self.len_check(source, pattern):
                    date_pattern = pattern.replace(
                        "YYYYMMDD", "([0-9]{4})(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])"
                    )
                    if re.match(date_pattern, source):
                        return True
                    else:
                        return False
            elif "YYYYMM" in pattern:
                if self.len_check(source, pattern):
                    regex = re.compile(r"([0-9]{4})(1[0-2]|0[1-9])")
                    if regex.search(source):
                        start_name = pattern.split("YYYYMM")[0]
                        if source.startswith(start_name):
                            return True
                return False
            elif "MM-DD-YYYY" in pattern:
                if self.len_check(source, pattern):
                    regex = pattern.replace(
                        "MM-DD-YYYY", "(1[0-2]|0[1-9])-(0[1-9]|[1-2][0-9]|3[0-1])-([0-9]{4})"
                    )
                    pattern = re.compile(regex)
                    if pattern.match(source):
                        return True
                return False
            # for handling month in alphabets
            elif "DDMONYYYY" in pattern:
                if self.len_check(source, pattern):
                    date_pattern = pattern.replace(
                        "DDMONYYYY", "(3[01]|[12][0-9]|0[1-9])([A-Z]{3})([0-9]{4})"
                    )
                    if re.match(date_pattern, source):
                        return True
                    else:
                        return False
            elif "YYYY_MM_DD_######" in pattern:
                if self.len_check(source, pattern):
                    regex = pattern.replace(
                        "YYYY_MM_DD_######",
                        "([0-9]{4})_(1[0-2]|0[1-9])_(3[01]|[12][0-9]|0[1-9])_([0-9]{6})",
                    )
                    pattern = re.compile(regex)
                    if pattern.search(source):
                        return True
                    else:
                        return False
            elif "YYYYQ#" in pattern:
                if self.len_check(source, pattern):
                    regex = re.compile(r"([0-9]{4}Q[1-4]{1})")
                    if pattern.search(source):
                        return True
                    else:
                        return False
            elif "YYYYQ" in pattern:
                if self.len_check(source, pattern):
                    date_pattern = pattern.replace("YYYYQ", "([0-9]{4})([1-4]{1})")
                    if re.match(date_pattern, source):
                        return True
                    else:
                        return False
            elif "N_YYYYMM" in pattern:
                if self.len_check(source, pattern):
                    regex = pattern.replace("N_YYYYMM", "([0-9])_([0-9]{4})(1[0-2]|0[1-9])")
                    pattern = re.compile(regex)
                    if pattern.search(source):
                        return True
                return False
            elif "YYYY" in pattern:
                if self.len_check(source, pattern):
                    regex = re.compile(r"([0-9]{4})")
                    if regex.search(source):
                        start_name = pattern.split("YYYY")[0]
                        if source.startswith(start_name):
                            return True
                return False
            elif re.match(pattern, source):
                return True
            else:
                return False
        return False
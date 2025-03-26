import re
from awsglue.context import GlueContext
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
class customPatternValidator(object):
    @staticmethod
    def len_check(source, pattern):
        try:
            return len(source) == len(pattern)
        except Exception as e:
            print(f"Exception occured in length check {str(e)}")
            return False

    def patternValidator(self, source, pattern):
        logger.info("Inside Custom Validator")
        if "YYYYWW" in pattern:
            if self.len_check(source, pattern):
                regex = re.compile(r"([0-9]{4})(0[1-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-3])")
                if regex.search(source):
                    return True
                return False
        return False
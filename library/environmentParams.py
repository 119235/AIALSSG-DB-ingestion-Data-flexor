# S3 Variables
RAW_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-raw'
CURATED_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-curated'
SANDBOX_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-sandbox'
PROCESSED_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-processed'
PUBLISH_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-publish'
PLATFORM_ARTIFACTS_S3_BUCKET = 'gilead-commercial-{}-us-west-2-platform-artifacts'
LOG_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-sandbox'
ARCHIVE_S3_BUCKET = 'gilead-edp-commercial-{}-us-west-2-us-comm-mdh-archive'

# Schema
STAGING_SCHEMA = 'comm_cur_mdh_staging'
PROCESSED_SCHEMA = 'comm_pro_mdh_dw'
REPORTING_SCHEMA = 'comm_pro_mdh_rpt'
PUBLISH_SCHEMA = 'comm_pro_mdh_publish'
DQ_SCHEMA = 'comm_mdh_dq'
LOG_SCHEMA = 'comm_mdh_logs'
DP_SCHEMA = 'comm_pro_mdh_dp'

# Other Variables
APPLICATION_NAME = "mdh"
SUB_APPLICATION_NAME = ""
WORKSPACE = "us-dna-mdh-ops"
PLATFORM_NAME = 'glue'|'databricks'
MWAA_INTEGRATION=True
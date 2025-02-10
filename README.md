select da.*,count(ivr.customer_key) from digital_adoption_all_channels da JOIN "call"."call_ivr_fact" ivr
    ON da.customer_key = ivr.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3--IVR_call_total_customers
select da.*,count(mch.customer_key) from digital_adoption_all_channels da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_app_total_customers
select da.*,count(wch.customer_key) from digital_adoption_all_channels da JOIN webanalytics.web_contact_history wch
    ON da.customer_key = wch.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_com_total_customers



import sys
import logging
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# ✅ Configure logging for AWS Glue (ensures logs go to CloudWatch)
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]  # Ensure logs go to stdout (CloudWatch Output Logs)
)

logger = logging.getLogger(__name__)  # Create a logger instance

# ✅ Initialize AWS Glue and Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ✅ Log messages (will appear in CloudWatch Output Logs)
logger.info("Hello world")
logger.info("Hey, how are you?")
logger.info("What are you doing?")
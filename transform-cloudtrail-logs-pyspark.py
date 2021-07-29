# code based in https://aws.amazon.com/pt/blogs/big-data/run-usage-analytics-on-amazon-quicksight-using-aws-cloudtrail/
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glueContext = GlueContext(sc)

# accountid  = '44026XXXXXX89' # Input your accountId
# region = 'us-west-2'
# logfolder = 'CloudTrail'

# For the purpose this demonstration , we are limiting the logs to specific date

# year = '2020'
# month = '10'
# day = '17' 
# pdp = "(partition_0 = '{}' and partition_1 = '{}' and partition_2 = '{}' and partition_3 = '{}' and partition_4 = '{}' and partition_5 = '{}')".format(accountid,logfolder,region,year,month,day)

rawcloudtraildata = glueContext.create_dynamic_frame.from_catalog(database = "cloudtrail_logs_julho", table_name = '07')
    
    
    
    
    # Get QuickSight Metadata E.g. Users, Analysis, Dashboards, Datasets etc.
    
    # qsmetadata = glueContext.create_dynamic_frame.from_catalog(database = "quicksightbionbi", table_name = 'metadata',region='us-west-2')
    
    
    
maindataset = rawcloudtraildata.toDF()
    # qsmetadatadf = qsmetadata.toDF()
    
    #maindataset.printSchema()
    
    # Select only specific fields from Cloudtrail which are are required for our analysis.
    
finalQScloudtrail = maindataset.where("eventSource = 'quicksight.amazonaws.com'").select("useridentity.type","eventtime","eventname","awsregion","useridentity.sessionContext.sessionIssuer.accountId","userIdentity.sessionContext.sessionIssuer.userName", "serviceEventDetails.eventResponseDetails.analysisDetails.analysisName","serviceEventDetails.eventResponseDetails.dashboardDetails.dashboardName")
    
finalQScloudtrail = finalQScloudtrail.withColumn("date",F.expr("replace(substr(eventTime,1,10),'-','')"))
    #finalQScloudtrail.show(truncate = False)
QScloudtrailfinals3 = "s3://<your-output-bucket>/aggregatedoutput/"
    #finalQScloudtrail.show()
finalQScloudtrail.write.mode("overwrite").format("parquet").partitionBy("date").save(QScloudtrailfinals3,header = 'true')
job.commit()

# except Exception as e:
#     print(e)
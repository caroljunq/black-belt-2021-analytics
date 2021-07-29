# https://aws.amazon.com/blogs/big-data/aws-cloudtrail-and-amazon-athena-dive-deep-to-analyze-security-compliance-and-operational-activity/

CREATE EXTERNAL TABLE cloudtrail_logs (
eventversion STRING,
userIdentity STRUCT<
  type:STRING,
  principalid:STRING,
  arn:STRING,
  accountid:STRING,
  invokedby:STRING,
  accesskeyid:STRING,
  userName:STRING,
  sessioncontext:STRUCT<
    attributes:STRUCT<
      mfaauthenticated:STRING,
      creationdate:STRING>,
    sessionIssuer:STRUCT<
      type:STRING,
      principalId:STRING,
      arn:STRING,
      accountId:STRING,
      userName:STRING>>>,
eventTime STRING,
eventSource STRING,
eventName STRING,
awsRegion STRING,
sourceIpAddress STRING,
userAgent STRING,
errorCode STRING,
errorMessage STRING,
requestParameters STRING,
responseElements STRING,
additionalEventData STRING,
requestId STRING,
eventId STRING,
resources ARRAY<STRUCT<
  ARN:STRING,accountId:
  STRING,type:STRING>>,
eventType STRING,
apiVersion STRING,
readOnly STRING,
recipientAccountId STRING,
serviceEventDetails STRING,
sharedEventID STRING,
vpcEndpointId STRING
)
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://<bucket-where-cloudtrail-store-logs>/AWSLogs/<your-account-id>/CloudTrail/<year-you-want-analyze>/<month>/';

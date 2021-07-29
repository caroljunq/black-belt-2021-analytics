# Code based in https://aws.amazon.com/pt/blogs/big-data/run-usage-analytics-on-amazon-quicksight-using-aws-cloudtrail/

import boto3
import json

def lambda_handler(event, context): 
    try:
        var_awsaccountid = 'xxxxxxxxxxx' # Input your accountId
        region = 'us-east-1'
        qsclient = boto3.client('quicksight',region)
        s3client = boto3.client('s3')
        bucketName = 'your-bucket-name'
        
        
        QSusers = qsclient.list_users(
            AwsAccountId=var_awsaccountid,
            MaxResults=100,
            Namespace='default'
        )
        #print(QSusers)
        
        
        qsusers = {}
        qsusers['users'] = []
        for users in QSusers['UserList']:
            var_username = users['UserName'].split('/',2)[0]
            var_role = users['Role']
            var_active = users['Active']
            userrecord = { 'UserName': var_username,
                                  'Role': var_role,
                                  'Active':var_active}
            qsusers['users'].append(userrecord)
                
        #print(qsusers)
        QSmetadatajson_users = json.dumps(qsusers)
        #print(QSmetadatajson_users)
        s3client.put_object(Body=QSmetadatajson_users, Bucket=bucketName, Key='metadata/users/qsusers.json')
        
        
        QSdatasets = qsclient.list_data_sets(
            AwsAccountId=var_awsaccountid,
            MaxResults=100
        )
        
        qsdatasets = {}
        qsdatasets['datasets'] = []
        for datasets in QSdatasets['DataSetSummaries']:
            var_datasetname = datasets['Name']
            var_importmode = datasets['ImportMode']
            var_dataset_id = datasets['DataSetId']
            var_create_time = str(datasets['CreatedTime'])[0:19]
            var_last_updated = str(datasets['LastUpdatedTime'])[0:19]
            
            datasetrecord = { 'datasetname':  var_datasetname,
                                  'importmode': var_importmode,'dataset_id':var_dataset_id,"spice_size":0,
                                  "created_time": var_create_time,"last_updated": var_last_updated
            }
            qsdatasets['datasets'].append(datasetrecord)
        
        for dataset in qsdatasets['datasets']:
            response = qsclient.describe_data_set(
                AwsAccountId=var_awsaccountid,
                DataSetId=dataset["dataset_id"]
            )
            dataset["spice_size"] = response["DataSet"]['ConsumedSpiceCapacityInBytes']
            
        QSmetadatajson_datasets = json.dumps(qsdatasets)
        #print(QSmetadatajson_datasets)
        s3client.put_object(Body=QSmetadatajson_datasets, Bucket=bucketName, Key='metadata/datasets/qsdatasets.json')
        
        
        QSanalyses = qsclient.list_analyses(
            AwsAccountId=var_awsaccountid,
            MaxResults=100
        )
        #print(QSanalyses)
        
        analysisrecords = {}
        analysisrecords['analysis'] = []
        
        analysisdatasets = {}
        analysisdatasets['analysisdatasets'] = []
        
        for analyses in QSanalyses['AnalysisSummaryList']:
            var_analysisid = analyses['AnalysisId']
            var_analysisname = analyses['Name']
            analysisrecord = { 'AnalysisId':  var_analysisid,
                                  'analysisname': var_analysisname}
            analysisrecords['analysis'].append(analysisrecord)
            
            
            
            analysesdetails = qsclient.describe_analysis(
            AwsAccountId=var_awsaccountid,
            AnalysisId=var_analysisid)
            
            #print(analysesdetails)
            for datasets in analysesdetails['Analysis']['DataSetArns']:
                var_datasetid = datasets.split('/',2)[1]
                # print(analysesdetails['Analysis']['DataSetArns'])
                try: 
                    datasetdetails = qsclient.describe_data_set(
                          AwsAccountId=var_awsaccountid,
                          DataSetId=var_datasetid)
                except Exception as e:
                    print("Error occured:",str(e))
                
                
                #print(datasetdetails)
                var_datasetname = datasetdetails['DataSet']['Name']
                
                analysisdatasetrecord = { 'AnalysesName': var_analysisname,
                                  'AnalysisId': var_analysisid,
                                  'DatasetID':var_datasetid,
                                  'DatasetName':var_datasetname }
                analysisdatasets['analysisdatasets'].append(analysisdatasetrecord)
                
        
        QSDashboards = qsclient.list_dashboards(
            AwsAccountId=var_awsaccountid,
            MaxResults=100
        )
        
        qsdashboards = {}
        qsdashboards['dashboards'] = []
        
        for dashboard in QSDashboards['DashboardSummaryList']:
            var_dashboadid = dashboard['DashboardId']
            var_dashname =  dashboard['Name']
            dashrecord = { 'DashboardId':  var_dashboadid,
                                  'DashboardName': var_dashname}
            qsdashboards['dashboards'].append(dashrecord)
        
        #print(analysisrecords)
        #print(analysisdatasets)
        QSmetadatajson_analysis = json.dumps(analysisrecords)
        QSmetadatajson_analysisdatasets = json.dumps(analysisdatasets)
        QSmetadatajson_dashboards = json.dumps(qsdashboards)
        #print(QSmetadatajson_analysisdatasets)
        s3client.put_object(Body=QSmetadatajson_analysis, Bucket=bucketName, Key='metadata/analysis/qsanalysis.json')
        s3client.put_object(Body=QSmetadatajson_analysisdatasets, Bucket=bucketName, Key='metadata/analysisdatasets/qsanalysisdatasets.json')
        s3client.put_object(Body=QSmetadatajson_dashboards, Bucket=bucketName, Key='metadata/dashboards/qsdashboards.json')

        
        #print(dashboard)
        
        # client = boto3.client('s3')
        # client.put_object(Body=QSmetadatajson, Bucket=bucketName, Key='metadata/QSMetadata.json')
        
    except Exception as e: print(e)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
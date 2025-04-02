#Glue Script for Data Quality where it fails overall file even if a single transaction fails the data quality check.



import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, array_contains
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

# Get resolved options (job parameters)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'configS3Path'])

# Initialize Glue Context and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

configS3Path = args['configS3Path']

# Fetch job configuration from S3
s3 = boto3.client('s3')
path_parts = configS3Path.replace("s3://", "").split("/")
bucket = path_parts.pop(0)
key = "/".join(path_parts)
print(f"Fetching S3 object: Bucket={bucket}, Key={key}")

s3_response = s3.get_object(Bucket=bucket, Key=key)
s3_object_body = s3_response.get('Body')
content = s3_object_body.read()
config = json.loads(content)
print("Config loaded:", config)

# Set job parameters from the config
job_type = config.get('jobType')
source_s3_path = config.get('sourceS3Path')
good_data_s3_bucket = config.get('goodDataS3Bucket')
grief_data_s3_bucket = config.get('griefDataS3Bucket')
rds_secret_name = config.get('rdsSecretName')
rds_table_name = config.get('rdsTableName')
database_name = config.get('databaseName')
glue_dq_context_id = config.get('glueDataQualityContext', 'dq_evaluation')

if job_type == "Data Quality":
    try:
        # Read raw data from S3
        datasource0 = glueContext.create_dynamic_frame.from_options(
            format_options={"withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [source_s3_path]},
            transformation_ctx="datasource0",
        )
        raw_data_df = datasource0.toDF()
        raw_data_dynamic_frame = datasource0  # Keep the original DynamicFrame

        # Fetch RDS credentials from Secrets Manager
        secretsmanager = boto3.client('secretsmanager', region_name=boto3.session.Session().region_name)
        secret_value_response = secretsmanager.get_secret_value(SecretId=rds_secret_name)
        rds_credentials = json.loads(secret_value_response['SecretString'])
        jdbc_url = f"jdbc:mysql://{rds_credentials['host']}:{rds_credentials['port']}/{database_name}"
        connection_properties = {
            "user": rds_credentials['username'],
            "password": rds_credentials['password'],
            "driver": "com.mysql.cj.jdbc.Driver",
        }

        # Read data quality rules from RDS MySQL
        rules_df = spark.read.jdbc(
            url=jdbc_url,
            table=rds_table_name,
            properties=connection_properties,
        )

        # Prepare DQ rules in ruleset format
        dq_rules = []
        for row in rules_df.where("is_active = true").collect():
            # Assuming rule_expression is already in Glue DQ syntax (e.g., "Completeness \"FAC_CD\" > 0.95")
            rule_expression = row['rule_expression']
            dq_rules.append(rule_expression)

        if dq_rules:
            # Format ruleset string
            ruleset = "Rules = [\n" + ",\n".join(dq_rules) + "\n]"
            print("Generated RuleSet:", ruleset)

            # Apply EvaluateDataQuality transform
            dq_results_dynamic_frame = EvaluateDataQuality().apply(
                frame=raw_data_dynamic_frame,  # Use the original DynamicFrame
                ruleset=ruleset,
                publishing_options={
                    "dataQualityEvaluationContext": glue_dq_context_id,
                    "enableDataQualityResultsPublishing": True,
                    "resultsS3Prefix": f"s3://{bucket}/dq-results/"  # Use the same bucket for consistency
                }
            )

            # Convert results to DataFrame for analysis
            dq_results_df = dq_results_dynamic_frame.toDF()
            dq_results_df.printSchema()
            dq_results_df.show()  # Inspect the output

            # Check if any rule failed for any record
            failed_rule_count = dq_results_df.filter(col("Outcome") != "Passed").count()
            print(failed_rule_count)
			
            if failed_rule_count > 0:
                # Write the entire input data to the grief data bucket in parquet format
                glueContext.write_dynamic_frame.from_options(
                    frame=raw_data_dynamic_frame,
                    connection_type="s3",
                    format="parquet",
                    connection_options={"path": grief_data_s3_bucket},
                    transformation_ctx="grief_data",
                )
                print(f"Overall data written to grief bucket: {grief_data_s3_bucket}")
            else:
                # Write the entire input data to the good data bucket in parquet format
                glueContext.write_dynamic_frame.from_options(
                    frame=raw_data_dynamic_frame,
                    connection_type="s3",
                    format="parquet",
                    connection_options={"path": good_data_s3_bucket},
                    transformation_ctx="good_data",
                )
                print(f"Overall data written to good bucket: {good_data_s3_bucket}")

        else:
            print("No active data quality rules found in RDS.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

elif job_type == "Curation":
    print("Curation job logic will be implemented here.")

else:
    print(f"Unknown job type: {job_type}")

# Commit the job
job.commit()
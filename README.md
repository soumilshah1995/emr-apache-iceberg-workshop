# emr-apache-iceberg-workshop
emr-apache-iceberg-workshop
![1731687994629](https://github.com/user-attachments/assets/14272ae0-0096-4a88-8506-30c094e91e73)

##### Blog with steps 
https://www.linkedin.com/pulse/building-medallion-architecture-emr-serverless-apache-soumil-shah-xboqe/?trackingId=hZ%2Bw5BhVQWOi9JaFQ33jbw%3D%3D

# Command 




# Step 1: Create EMR Serverless Cluster and Setup ENV
```
export APPLICATION_ID="00fnfk60j0shmh09"
export AWS_ACCOUNT="867098943567"
export BUCKET="XX" 
export IAM_ROLE="arn:aws:iam::${AWS_ACCOUNT}:role/EMRServerlessS3RuntimeRole" 

aws s3 cp raw-bronze.py s3://$BUCKET/jobs/raw-bronze.py
aws s3 cp bronze-silver.py s3://$BUCKET/jobs/bronze-silver.py

```

# Step 2: Simulate New data files has arrvied in S3
```agsl
python3 <PATH>/scripts/aws-scripts/datagen/raw-datagen.py
```

# Step 3: Ingest into bronze layers Incrementally 
```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "iceberg-raw-bronze" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://soumilshah-dev-1995/jobs/raw-bronze.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://XX/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://soumilshah-dev-1995/logs/"
            }
        }
    }'


```
# Step 4: Incrementally Fetch New Delta adn Merge into Silver Zone 
```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "iceberg-bronze-silver" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://soumilshah-dev-1995/jobs/bronze-silver.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://XX/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://soumilshah-dev-1995/logs/"
            }
        }
    }'

```

# Step 5: New Files Coming in Updates 
```
python3 <PATH>/scripts/aws-scripts/datagen/updates_iceberg.py
```

# Step 6: Run Sync Command to Flow data into Bronze and silver zone 
```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "iceberg-raw-bronze" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://soumilshah-dev-1995/jobs/raw-bronze.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://XX/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://soumilshah-dev-1995/logs/"
            }
        }
    }'


aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "iceberg-bronze-silver" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://soumilshah-dev-1995/jobs/bronze-silver.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://XX/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://soumilshah-dev-1995/logs/"
            }
        }
    }'


```

# Step 7: Athena 
```
SELECT COUNT(*) FROM bronze_orders

SELECT COUNT(*) FROM silver_orders
```
# Query The same Data with different Query Engine 


###  Query Snowflake
```
-- Step 1: Create a new database named TEMPDB
CREATE DATABASE TEMPDB;

-- Step 2: Switch to the newly created TEMPDB database
USE TEMPDB;

-- Step 3: Create an external volume in Snowflake pointing to an S3 bucket
CREATE OR REPLACE EXTERNAL VOLUME ext_vol
STORAGE_LOCATIONS = (
    (
        NAME = 'my-s3-us-east-1', 
        STORAGE_PROVIDER = 'S3', 
        STORAGE_BASE_URL = 's3://XXX/warehouse/icebergdb.db/silver_orders',  
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XX:role/snowflakes_role',  
        STORAGE_AWS_EXTERNAL_ID = 'ext_vol'  
    )
);

-- Step 4: Retrieve the IAM user ARN for updating the trust policy in AWS
DESC EXTERNAL VOLUME ext_vol;

--  STRING JSON COPY : STORAGE_AWS_IAM_USER_ARN VALUES in Notepad
-- value is STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::XXXX:user/e37q0000-s


DROP CATALOG INTEGRATION sf_glue_catalog_integ;

CREATE CATALOG INTEGRATION sf_glue_catalog_integ
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='icebergdb'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::XX:role/snow_flake_glue_access'
  GLUE_CATALOG_ID='XX'
  GLUE_REGION='us-east-1'
  ENABLED=TRUE;



DESCRIBE CATALOG INTEGRATION sf_glue_catalog_integ

-- COPY GLUE_AWS_IAM_USER_ARN and GLUE_AWS_EXTERNAL_ID
-- GLUE_AWS_IAM_USER_ARN XXXXXX
-- GLUE_AWS_EXTERNAL_ID XXXXXX
-- UPDATE IAM ROLE



CREATE OR REPLACE ICEBERG TABLE silver_orders
EXTERNAL_VOLUME='ext_vol'
CATALOG='sf_glue_catalog_integ'
CATALOG_TABLE_NAME='silver_orders'


SELECT * FROM silver_orders
```
###  Query DuckDB 
```
cd /Users/sshah/IdeaProjects/workshop/aws-scripts/query-engine/duckdbdemo
pip3 install -r requirements.txt
python3 /Users/sshah/IdeaProjects/workshop/aws-scripts/query-engine/duckdbdemo/read_iceberg_duckdb.py

```
####  Query StarRocks
```
SELECT COUNT(*) FROM my_glue_catalog.icebergdb.silver_orders;
```

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
        STORAGE_BASE_URL = 's3://XXX/warehouse/icebergdb.db/',
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXX:role/snowflakes_role',
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
  GLUE_AWS_ROLE_ARN='arn:aws:iam::XXX:role/snow_flake_glue_access'
  GLUE_CATALOG_ID='XXX'
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
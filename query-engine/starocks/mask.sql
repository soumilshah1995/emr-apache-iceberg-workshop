

-- mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

CREATE EXTERNAL CATALOG my_glue_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "glue",
    "aws.glue.use_instance_profile" = "false",
    "aws.glue.access_key" = "XX",
    "aws.glue.secret_key" = "XXXXC",
    "aws.glue.region" = "us-east-1",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "XXX",
    "aws.s3.secret_key" = "XXX",
    "aws.s3.region" = "us-east-1"
);

SHOW TABLES IN my_glue_catalog.icebergdb;

SHOW TABLES IN my_glue_catalog.icebergdb;

SELECT * FROM my_glue_catalog.icebergdb.silver_orders;

SELECT COUNT(*) FROM my_glue_catalog.icebergdb.silver_orders;
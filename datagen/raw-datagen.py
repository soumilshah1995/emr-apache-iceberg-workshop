import os
import random
import csv
import uuid
import os
import boto3
from urllib.parse import urlparse
from io import StringIO
from faker import Faker


def generate_fake_data(num_entries):
    fake = Faker()
    fake_data = []
    for _ in range(num_entries):
        op = random.choice(['I', 'U', 'D'])
        replicadmstimestamp = fake.date_time_this_year()
        invoiceid = fake.unique.random_number(digits=5)
        itemid = fake.unique.random_number(digits=2)
        category = fake.word()
        price = round(random.uniform(10, 100), 2)
        quantity = random.randint(1, 5)
        orderdate = fake.date_this_decade()
        destinationstate = fake.state_abbr()
        shippingtype = random.choice(['2-Day', '3-Day', 'Standard'])
        referral = fake.word()

        fake_data.append(
            [op, replicadmstimestamp, invoiceid, itemid, category, price, quantity, orderdate, destinationstate,
             shippingtype, referral])

    return fake_data


def generate_csv_from_data(data, output_path, minio_config=None):
    parsed_url = urlparse(output_path)
    scheme = parsed_url.scheme

    if scheme in ['s3', 's3a']:
        # S3 or MinIO path
        if minio_config:
            # Use MinIO configuration
            s3 = boto3.client('s3',
                              endpoint_url=minio_config['endpoint_url'],
                              aws_access_key_id=minio_config['access_key'],
                              aws_secret_access_key=minio_config['secret_key'],
                              region_name='us-east-1')
        else:
            # Use default S3 configuration
            s3 = boto3.client('s3')

        bucket = parsed_url.netloc
        key = f"{parsed_url.path.lstrip('/')}/{uuid.uuid4()}.csv"

        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer, delimiter='\t')
        csv_writer.writerow(
            ['Op', 'replicadmstimestamp', 'invoiceid', 'itemid', 'category', 'price', 'quantity', 'orderdate',
             'destinationstate', 'shippingtype', 'referral'])
        csv_writer.writerows(data)

        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"CSV file '{output_path}/{key}' generated successfully.")

    elif scheme == 'file' or not scheme:
        # Local file system
        if scheme == 'file':
            directory = parsed_url.path
        else:
            directory = output_path

        os.makedirs(directory, exist_ok=True)
        output_file = os.path.join(directory, f"{uuid.uuid4()}.csv")

        with open(output_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter='\t')
            csv_writer.writerow(
                ['Op', 'replicadmstimestamp', 'invoiceid', 'itemid', 'category', 'price', 'quantity', 'orderdate',
                 'destinationstate', 'shippingtype', 'referral'])
            csv_writer.writerows(data)

        print(f"CSV file '{output_file}' generated successfully.")

    else:
        raise ValueError(f"Unsupported scheme: {scheme}")


def write_static_data(output_path, minio_config=None):
    # Static data as requested
    static_data = [
        ['I', '2024-02-16 15:30:41.041474', 24137, 34, 'degree', 53.51, 1, '2023-03-29', 'SC', '3-Day', 'book'],
        ['I', '2024-08-20 17:16:03.213831', 15587, 59, 'bit', 40.94, 5, '2022-07-16', 'PW', '3-Day', 'management'],
        ['I', '2024-10-28 20:02:37.424182', 42918, 69, 'school', 27.23, 3, '2024-04-29', 'CT', '2-Day', 'trouble'],
        ['I', '2024-06-27 14:36:25.103244', 40994, 67, 'market', 92.02, 1, '2021-05-21', 'VI', '2-Day', 'others'],
        ['I', '2024-02-01 19:52:59.444793', 83597, 37, 'language', 97.07, 3, '2021-09-10', 'SC', 'Standard', 'play']
    ]

    generate_csv_from_data(static_data, output_path, minio_config)


if __name__ == "__main__":
    write_static_data('s3://datalake-demo-1995/raw')

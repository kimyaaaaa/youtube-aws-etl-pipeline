import boto3
import pandas as pd
from io import BytesIO

BUCKET = 'youtube-etl-project-769044546127-eu-north-1-an'
REGISTRY_KEY = 'metadata/processed_files.parquet'

s3 = boto3.client('s3')


def get_new_files():

    new_files = []

    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=REGISTRY_KEY
        )

        registry = pd.read_parquet(
            BytesIO(
                obj['Body'].read()
            )
        )

        processed_etags = set(
            registry['etag']
        )

    except:

        # first run → registry does not exist
        processed_etags = set()

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='raw/videos/'
    )

    for item in response.get(
        'Contents',
        []
    ):

        key = item['Key']

        if not key.endswith('.csv'):
            continue

        etag = item['ETag'].replace('"', '')

        if etag not in processed_etags:

            country = key.split(
                'country='
            )[1].split('/')[0]

            new_files.append({

                'file_key': key,
                'etag': etag,
                'country': country
            })

    return new_files


def update_processed_files(files):

    new_df = pd.DataFrame(files)

    try:

        obj = s3.get_object(
            Bucket=BUCKET,
            Key=REGISTRY_KEY
        )

        existing_df = pd.read_parquet(
            BytesIO(
                obj['Body'].read()
            )
        )

        combined_df = pd.concat(
            [existing_df, new_df],
            ignore_index=True
        )

        combined_df = combined_df.drop_duplicates(
            subset=['etag']
        )

    except Exception:

        # first run
        combined_df = new_df


    parquet_buffer = BytesIO()

    combined_df.to_parquet(
        parquet_buffer,
        index=False
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=REGISTRY_KEY,
        Body=parquet_buffer.getvalue()
    )

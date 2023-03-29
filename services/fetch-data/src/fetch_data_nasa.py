#! /usr/bin/env python
import os
import boto3
from urllib import request, error
import click

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "transition-storage-dev"
S3_KEY = "raw-data"

def client_session_boto3(aws_access_key: str, aws_secret_key: str) -> boto3.Session:
    """Return a client session to S3
    
    Parameters
    ----------
    `aws_access_key` : (str) access key id of AWS services
    `aws_secret_key` : (str) secret access key of AWS services

    Return
    ------
    `boto3.session.Session.client` : boto3 client session
    """
    return boto3.client("s3",
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key
                    )

def upload_file_to_s3(filename: str, bucket_name: str, s3_key: str) -> None:
    """Upload file to S3
    
    Parameters
    ----------
    `filename` : (str) The path to the file to upload.
    `s3_bucket_namw` : (str) The name of the bucket to upload to.
    `key` : (str) The name of the key[path inside of the bucket] to download to.
    """
    s3_client = client_session_boto3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    s3_client.upload_file(Filename=filename,
                          Bucket=bucket_name,
                          Key=s3_key
                        )

@click.command()
@click.argument("url", type=str)
@click.option(
    "--filename",
    default="/tmp/temp-file.csv",
    type=click.Path(dir_okay=False, writable=True),
    help="Optional file to write output to."
)
def fetch_data_nasa(url: str, filename: str):
    """CLI application for fetching weather forecasts from NASA
    CSV format.
    
    Parameter:
    
    `url` (str)
    """
    path_file = f"/tmp/{filename}"
    try:
        request.urlretrieve(url=url, filename=path_file)    
    except error.HTTPError:
        print(f"{error.HTTPError.reason}")
    else:
        upload_file_to_s3(filename=path_file, 
                          bucket_name=BUCKET_NAME,
                          s3_key=f"{S3_KEY}/{filename}"
                        )

if __name__=="__main__":
    fetch_data_nasa()
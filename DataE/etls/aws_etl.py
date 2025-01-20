import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

# Connect to AWS S3
def connect_to_s3():
    try:
        # Create an S3 client using boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY
        )
        return s3_client
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
    except Exception as e:
        print(f"Error connecting to S3: {e}")

# Check if a bucket exists, and create it if not
def create_bucket_if_not_exist(s3_client, bucket_name):
    try:
        # Check if the bucket exists
        response = s3_client.list_buckets()
        bucket_exists = any(bucket['Name'] == bucket_name for bucket in response['Buckets'])
        
        if not bucket_exists:
            # Create the bucket if it doesn't exist
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        print(f"Error checking or creating the bucket: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Upload a file to S3
def upload_to_s3(s3_client, file_path, bucket_name, s3_file_name):
    try:
        # Upload the file to the S3 bucket
        s3_client.upload_file(file_path, bucket_name, f'raw/{s3_file_name}')
        print(f"File '{s3_file_name}' uploaded to S3 bucket '{bucket_name}'.")
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
    except NoCredentialsError:
        print("Credentials are not available.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


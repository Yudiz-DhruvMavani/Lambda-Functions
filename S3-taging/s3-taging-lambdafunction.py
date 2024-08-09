import boto3
from botocore.exceptions import ClientError
import time

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    response = s3.list_buckets()
        def add_tag_with_retries(bucket_name, tags, retries=3):
        for attempt in range(retries):
            try:
                s3.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': tags})
                print(f'Tag added to bucket: {bucket_name}')
                return True
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'OperationAborted':
                    print(f'OperationAborted error for bucket {bucket_name}, retrying...')
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f'Error processing bucket {bucket_name}: {e}')
                    return False
        return False

    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        try:
            tag_response = s3.get_bucket_tagging(Bucket=bucket_name)
            tags = tag_response['TagSet']
            tag_keys = [tag['Key'] for tag in tags]

            if 'Name' not in tag_keys:
                tags.append({'Key': 'Name', 'Value': bucket_name})
                if add_tag_with_retries(bucket_name, tags):
                    print(f'Tag added to bucket: {bucket_name}')
                else:
                    print(f'Failed to add tag to bucket: {bucket_name}')
            else:
                print(f'Tag "Name" already exists in bucket: {bucket_name}')
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchTagSet':
                # If the bucket has no tags at all, we need to add the 'Name' tag
                tags = [{'Key': 'Name', 'Value': bucket_name}]
                if add_tag_with_retries(bucket_name, tags):
                    print(f'Tag added to bucket: {bucket_name}')
                else:
                    print(f'Failed to add tag to bucket: {bucket_name}')
            else:
                print(f'Error processing bucket {bucket_name}: {e}')

    return {
        'statusCode': 200,
        'body': 'Tagging operation completed'
    }

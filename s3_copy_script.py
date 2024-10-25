import aioboto3
import asyncio
import logging

# Configure logging to output to console and log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('s3_copy.log')
    ]
)

# Configuration variables for source and destination roles and buckets
source_role_arn = 'arn:aws:iam::SOURCE_ACCOUNT_ID:role/YOUR_SOURCE_ROLE'
destination_role_arn = 'arn:aws:iam::DESTINATION_ACCOUNT_ID:role/YOUR_DESTINATION_ROLE'
source_bucket = 'your-source-bucket-name'
destination_bucket = 'your-destination-bucket-name'
BATCH_SIZE = 100

# Global variables to hold assumed role credentials
source_credentials = None
destination_credentials = None

async def assume_role(role_arn):
    async with aioboto3.client('sts') as sts_client:
        logging.info(f'Assuming role: {role_arn}')
        response = await sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName='SessionName'
        )
        return response['Credentials']

async def refresh_credentials():
    global source_credentials, destination_credentials
    source_credentials = await assume_role(source_role_arn)
    destination_credentials = await assume_role(destination_role_arn)
    logging.info('Refreshed AWS credentials.')

async def get_source_s3_client():
    return aioboto3.client(
        's3',
        aws_access_key_id=source_credentials['AccessKeyId'],
        aws_secret_access_key=source_credentials['SecretAccessKey'],
        aws_session_token=source_credentials['SessionToken'],
        region_name='source-region'
    )

async def get_destination_s3_client():
    return aioboto3.client(
        's3',
        aws_access_key_id=destination_credentials['AccessKeyId'],
        aws_secret_access_key=destination_credentials['SecretAccessKey'],
        aws_session_token=destination_credentials['SessionToken'],
        region_name='destination-region'
    )

async def copy_batch(keys):
    async with get_source_s3_client() as source_client, get_destination_s3_client() as dest_client:
        for key in keys:
            try:
                logging.info(f'Copying {key}...')
                source_object = await source_client.get_object(Bucket=source_bucket, Key=key)
                data = await source_object['Body'].read()
                await dest_client.put_object(Bucket=destination_bucket, Key=key, Body=data)
                logging.info(f'Successfully copied {key}')
            except Exception as e:
                logging.error(f'Failed to copy {key}: {e}')

async def copy_objects():
    await refresh_credentials()
    async with get_source_s3_client() as source_client:
        response = await source_client.list_objects_v2(Bucket=source_bucket)
        keys = [obj['Key'] for obj in response.get('Contents', [])]

        tasks = [
            copy_batch(keys[i:i + BATCH_SIZE])
            for i in range(0, len(keys), BATCH_SIZE)
        ]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(copy_objects())

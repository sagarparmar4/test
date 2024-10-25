import aioboto3
import asyncio
from botocore.exceptions import ClientError, BotoCoreError

# Configuration variables for source and destination profiles and buckets
source_profile = 'your_source_profile'
destination_profile = 'your_destination_profile'
source_bucket = 'your-source-bucket-name'
destination_bucket = 'your-destination-bucket-name'
BATCH_SIZE = 100  # Adjust based on your requirements

async def get_s3_client(profile_name, region_name='us-west-2'):
    session = aioboto3.Session(profile_name=profile_name)
    return await session.client('s3', region_name=region_name)

async def copy_object(source_client, dest_client, key):
    try:
        # Use the copy_object method to copy only the metadata
        await dest_client.copy_object(
            Bucket=destination_bucket,
            CopySource={'Bucket': source_bucket, 'Key': key},
            Key=key
        )
        return True
    except (ClientError, BotoCoreError) as e:
        if e.response['Error']['Code'] in ['ExpiredToken', 'InvalidClientTokenId']:
            return False  # Indicate the need to refresh the token
    except Exception:
        return False  # Handle any unexpected errors
    return True  # Indicate success

async def copy_batch(keys):
    source_client = await get_s3_client(source_profile)
    dest_client = await get_s3_client(destination_profile)

    async with source_client, dest_client:
        for key in keys:
            # Add a retry mechanism for token expiration
            while True:
                success = await copy_object(source_client, dest_client, key)
                if not success:
                    # If token expired, refresh and retry
                    source_client = await get_s3_client(source_profile)
                    dest_client = await get_s3_client(destination_profile)
                else:
                    break

async def copy_objects():
    async with get_s3_client(source_profile) as source_client:
        paginator = source_client.get_paginator('list_objects_v2')
        async for page in paginator.paginate(Bucket=source_bucket):
            keys = [obj['Key'] for obj in page.get('Contents', [])]

            # Process keys in batches
            for i in range(0, len(keys), BATCH_SIZE):
                batch = keys[i:i + BATCH_SIZE]
                await copy_batch(batch)

if __name__ == '__main__':
    asyncio.run(copy_objects())

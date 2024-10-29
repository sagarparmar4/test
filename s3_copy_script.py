import aioboto3
import asyncio
import time
from botocore.exceptions import ClientError

SOURCE_BUCKET = 'source-bucket-name'
DEST_BUCKET = 'destination-bucket-name'
SOURCE_PROFILE = 'source-profile'
DEST_PROFILE = 'destination-profile'

# Counters for tracking progress
total_keys_read = 0
total_uploaded = 0
total_failed = 0

# Function to create a new session for the specified profile
def create_session(profile_name):
    return aioboto3.Session(profile_name=profile_name)

# Function to download and upload a single object
async def transfer_object(key, source_session, dest_session):
    global total_uploaded, total_failed
    while True:
        try:
            # Establish clients for source and destination buckets
            async with source_session.client('s3') as s3_source, dest_session.client('s3') as s3_dest:
                # Download the object from the source bucket
                response = await s3_source.get_object(Bucket=SOURCE_BUCKET, Key=key)
                data = await response['Body'].read()

                # Upload the object to the destination bucket
                await s3_dest.put_object(Bucket=DEST_BUCKET, Key=key, Body=data)
            total_uploaded += 1  # Increment uploaded count if successful
            break  # Exit loop after successful transfer
        except ClientError as e:
            # Check for token expiry error
            if e.response['Error']['Code'] == 'ExpiredToken':
                # Refresh the sessions on token expiry
                source_session = create_session(SOURCE_PROFILE)
                dest_session = create_session(DEST_PROFILE)
            else:
                total_failed += 1  # Increment failed count if error persists
                print(f"Failed to upload {key}: {e}")
                break  # Break on persistent errors other than token expiry

# Main function to paginate and process all objects in the source bucket
async def main():
    global total_keys_read
    source_session = create_session(SOURCE_PROFILE)
    dest_session = create_session(DEST_PROFILE)

    async with source_session.client('s3') as s3_source:
        paginator = s3_source.get_paginator('list_objects_v2')
        
        # Process each page of objects
        async for page in paginator.paginate(Bucket=SOURCE_BUCKET):
            page_start_time = time.time()  # Start time for each page

            if 'Contents' not in page:
                continue  # Skip if there are no objects in this page

            # Get all keys in the current page
            keys = [obj['Key'] for obj in page['Contents']]
            total_keys_read += len(keys)  # Track total keys read

            # Process keys in parallel
            tasks = [transfer_object(key, source_session, dest_session) for key in keys]
            await asyncio.gather(*tasks)

            # Log time taken for page and cumulative totals after each page
            page_duration = time.time() - page_start_time
            print(f"Processed page with {len(keys)} keys in {page_duration:.2f} seconds.")
            print(f"Cumulative totals - Keys read: {total_keys_read}, Uploaded: {total_uploaded}, Failed: {total_failed}")

# Run the main function
asyncio.run(main())

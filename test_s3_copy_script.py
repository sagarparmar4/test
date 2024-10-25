import unittest
from unittest.mock import patch, AsyncMock
from aiohttp import ClientConnectionError

class TestS3CopyScript(unittest.IsolatedAsyncioTestCase):
    @patch('aioboto3.Session.client', new_callable=AsyncMock)
    async def test_copy_object_batch(self, mock_client):
        mock_client.return_value.get_object.return_value = {
            'Body': AsyncMock(read=AsyncMock(return_value=b'Test content'))
        }

        from s3_copy_script import copy_batch, source_bucket, destination_bucket
        keys = ['file1.txt', 'file2.txt']
        
        await copy_batch(keys)

        for key in keys:
            mock_client.return_value.put_object.assert_any_call(
                Bucket=destination_bucket,
                Key=key,
                Body=b'Test content'
            )

    @patch('aioboto3.Session.client', new_callable=AsyncMock)
    async def test_network_issue_handling(self, mock_client):
        mock_client.return_value.get_object.side_effect = ClientConnectionError('Network issue')

        from s3_copy_script import copy_batch
        with self.assertLogs('s3_copy_script', level='ERROR') as cm:
            await copy_batch(['file1.txt'])

        self.assertIn('Failed to copy file1.txt: Network issue', cm.output)

if __name__ == '__main__':
    unittest.main()

import boto3
class S3Helper:
    def __init__(self,access_key=None,secret_access_key=None):
        if access_key is None and secret_access_key is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = boto3.client('s3',
              aws_access_key_id=access_key,
              aws_secret_access_key=secret_access_key)

    def list_objects(self,bucket,prefix=None):
        _key_list = []
        continuation_token = None
        while True:
            if prefix is None and continuation_token is None:
                response = self.s3_client.list_objects_v2( Bucket=bucket)
            elif prefix is None and continuation_token is not None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, ContinuationToken=continuation_token)
            elif prefix is not None and continuation_token is None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, Prefix=prefix)
            elif prefix is not None and continuation_token is not None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                return None
            if 'Contents' in response:
                _key_list.extend(response['Contents'])

            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                return _key_list

    def download_file(self,bucket,key,local_path):
        with open(local_path, 'wb') as f:
            self.s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=f)

    def upload_file(self,bucket,key,local_path):
        with open(local_path, 'rb') as f:
            self.s3_client.upload_fileobj(f,Bucket=bucket, Key=key)
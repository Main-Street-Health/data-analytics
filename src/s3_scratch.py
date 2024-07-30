import boto3

client = boto3.client('s3')
client.list_objects_v2(Bucket='cb-member-doc-us-east-2-prd', Prefix='parsed_claim_file_archive/')
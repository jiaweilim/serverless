import sys
import boto3
import datetime
import sys
sys.path.append('../')
from functions.transform_json_to_csv import transform_json_to_csv

if __name__ == "__main__":

    SFTP_BUCKET = 'jiawei-s3-sftp'
    WORKING_BUCKET = 'jiawei-working-bucket'
    QUARANTINE_BUCKET = 'jiawei-quarantine'
    HEALTHY_BUCKET = 'jiawei-healthy'

    s3_client = boto3.client('s3')

    print('Retrieve all files from sftp s3 bucket')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(SFTP_BUCKET)
    sftp_files = list(bucket.objects.all())

    print('Move files to working s3 bucket')
    for file in sftp_files:
        source_bucket_name = SFTP_BUCKET
        destination_bucket_name = WORKING_BUCKET
        file_key_name = file.key

        source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
        s3_client.copy_object(CopySource=source_object, Bucket=destination_bucket_name, Key=file_key_name)
        print(f'Moved {file_key_name}')

    print('Create new Sub Folder in quarantine and healthy')
    dt = datetime.datetime.now()
    format_dt = f'{dt.year}-{dt.month}-{dt.day}-{dt.hour}-{dt.minute}'
    folder_name = format_dt

    s3_client.put_object(Bucket=QUARANTINE_BUCKET, Key=(folder_name+'/'))
    s3_client.put_object(Bucket=HEALTHY_BUCKET, Key=(folder_name+'/'))

    print('Retrieve all files from working s3 bucket')
    bucket = s3.Bucket(WORKING_BUCKET)
    working_files = list(bucket.objects.all())

    print('Move files to quarantine or healthy buckets')
    for file in working_files:
        source_bucket_name = WORKING_BUCKET
        healthy_bucket_name = HEALTHY_BUCKET
        unhealthy_bucket_name = QUARANTINE_BUCKET
        file_key_name = file.key
        dest_file_name = folder_name + '/' + file.key

        if file_key_name.split('_')[0] == 'healthy':
            source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
            transform_json_to_csv(source_object, folder_name)
            print(f'Transformed {file_key_name} to healthy')

        else:
            source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
            s3_client.copy_object(CopySource=source_object, Bucket=unhealthy_bucket_name, Key=dest_file_name)   
            print(f'Moved {file_key_name} to quarantine')           

    print('Start glue crawler')
    glue_client = boto3.client('glue')
    glue_client.start_crawler(Name='healthy-data-crawler')
    # exit()
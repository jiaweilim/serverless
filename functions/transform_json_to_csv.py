import boto3
import json
import pandas as pd
from io import StringIO

WORKING_BUCKET = 'jiawei-working-bucket'
HEALTHY_BUCKET = 'jiawei-healthy'

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def create_df_from_json(json_data={}, column_names=[]):
    df = pd.json_normalize(json_data)
    return df.reindex(df.columns.union(
        column_names,
        sort=False), axis=1, fill_value='NA'
    )[column_names]

def transform_json_to_csv(file_object, folder_name):

    print('Get json data')
    data_obj = s3_client.get_object(
        Bucket=file_object['Bucket'],
        Key=file_object['Key']
    )
    json_data = json.load(data_obj['Body'])


    print('Convert json to dataframe')
    DATA_FIELDS = [
        "name",
        "price",
        "quantity",
    ]

    df = create_df_from_json(
        json_data=json_data,
        column_names=DATA_FIELDS
    )

    print('Tranformation - delete last column')
    del df['quantity']

    print('Dataframe to csv')
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer,
        index_label='id'
    )

    print('Put file into healthy')
    file_name = file_object['Key'].split('.')[0]
    file_path = f'{folder_name}/{file_name}.csv'
    s3_resource.Bucket(HEALTHY_BUCKET).put_object(
        Key=file_path,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f'Put {file_name}.csv to quarantine')   
    return None
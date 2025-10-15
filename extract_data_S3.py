import boto3
import pandas as pd

# eu-west-3
# url s3 -> s3://test-bucket-v1.0.0-386510763288/source_crm/

bucket_name = 'test-bucket-v1.0.0-386510763288'
file_key = 'source_crm/*'

# Je compte récupérer tous les fichiers csv dans le bucket S3 en fonction de bucket_name et file_key dans un premier temps
def extrat_data_from_s3():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='source_crm/')
    
    data_frames = []
    
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        if file_key.endswith('.csv'):
            obj_response = s3.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(obj_response['Body'])
            data_frames.append(df)
    
    if data_frames:
        combined_df = pd.concat(data_frames, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()
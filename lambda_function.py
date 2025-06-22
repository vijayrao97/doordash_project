import json
import pandas as pd
import boto3
import io
import os
from datetime import date
from dotenv import load_dotenv

load_dotenv()

def lambda_handler(event, context):
    # TODO implement
    # print("Event : ",event)
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    # print(f"input_bucket : {input_bucket} and input_key {input_key}")

    s3 = boto3.client('s3')

    response = s3.get_object(Bucket=input_bucket, Key=input_key)

    # print(response['Body'].read())

    body = response['Body'].read().decode('utf-8').split('\r\n')

    # Step 1: Decode bytes to string
    decoded_str = body

    print(decoded_str)

    records = [json.loads(entry) for entry in decoded_str]

    print("records : ", records)

    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Display
    print(df.columns)

    # Print DataFrame to CloudWatch logs
    print(df.to_string(index=False))

    filtered_df = df[df['status'] == 'delivered']

    print(filtered_df.to_string(index=False))

    csv_buffer = io.StringIO()
    filtered_df.to_csv(csv_buffer, index=False)

    file_name = ''
    try:
        date_var = str(date.today())
        file_name = 'processed_data/{}_processed_data.csv'.format(date_var)
    except:
        file_name = 'processed_data/processed_data.csv'

    s3.put_object(
        Bucket=os.getenv('output_bucket'),  # Replace with your bucket name
        Key=file_name,  # Desired S3 object key
        Body=csv_buffer.getvalue()
    )

    print(f'File uploaded in bucket doordash-target-zn-1307')

    # sns to deliver file processed request
    sns = boto3.client('sns')
    response = sns.publish(
        TopicArn=os.getenv('TopicArn'),
        Message="File has been formatted and filtered. Its been stored in doordash-target-zn-1307 as processed_data"
    )

    print(f'SNS message sent successfully.')


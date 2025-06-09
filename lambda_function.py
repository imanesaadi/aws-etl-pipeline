import boto3
import csv
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # 1.
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # 2.
    response = s3.get_object(Bucket=source_bucket, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    reader = csv.reader(io.StringIO(content))
    
    # 3.
    output = []
    for row in reader:
        output.append([cell.upper() for cell in row])
    
    # 4.
    output_bucket = 'my-etl-output-bucket-imanee'  
    output_key = f'transformed-{file_key}'
    
    output_csv = io.StringIO()
    writer = csv.writer(output_csv)
    writer.writerows(output)
    
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=output_csv.getvalue())

    return {
        'statusCode': 200,
        'body': f'Processed {file_key} and saved as {output_key}'
    }
import json
import os
import boto3
from datetime import datetime, timezone
import csv
from io import StringIO
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

EXPORT_JOBS_TABLE = os.environ['EXPORT_JOBS_TABLE']
USERS_TABLE = os.environ['USERS_TABLE']
ORDERS_TABLE = os.environ['ORDERS_TABLE']
REPORTS_BUCKET = os.environ['REPORTS_BUCKET']

jobs_table = dynamodb.Table(EXPORT_JOBS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
orders_table = dynamodb.Table(ORDERS_TABLE)


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def handler(event, context):
    try:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            
            job_id = message_body['job_id']
            export_type = message_body['export_type']
            export_format = message_body.get('format', 'csv')
            filters = message_body.get('filters', {})
            
            print(f"Processing export job: {job_id}, type: {export_type}")
            
            update_job_status(job_id, 'processing')
            
            try:
                if export_type == 'users':
                    data = export_users(filters)
                elif export_type == 'orders':
                    data = export_orders(filters)
                else:
                    raise ValueError(f"Unknown export type: {export_type}")
                
                if export_format == 'csv':
                    file_content = generate_csv(data, export_type)
                    content_type = 'text/csv'
                    file_extension = 'csv'
                else:
                    file_content = generate_json(data)
                    content_type = 'application/json'
                    file_extension = 'json'
                
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
                s3_key = f"exports/{export_type}/{job_id}_{timestamp}.{file_extension}"
                
                s3.put_object(
                    Bucket=REPORTS_BUCKET,
                    Key=s3_key,
                    Body=file_content,
                    ContentType=content_type
                )
                
                download_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': REPORTS_BUCKET, 'Key': s3_key},
                    ExpiresIn=7 * 24 * 60 * 60
                )
                
                update_job_status(
                    job_id, 
                    'completed', 
                    s3_key=s3_key,
                    download_url=download_url,
                    record_count=len(data)
                )
                
                print(f"Export job {job_id} completed successfully. Records: {len(data)}")
                
            except Exception as e:
                print(f"Error processing job {job_id}: {str(e)}")
                update_job_status(job_id, 'failed', error_message=str(e))
                raise
        
        return {'statusCode': 200, 'body': 'Export jobs processed successfully'}
        
    except Exception as e:
        print(f"Error in export job processor: {str(e)}")
        raise


def export_users(filters):
    items = []
    scan_kwargs = {}
    
    filter_expressions = []
    expression_values = {}
    
    if 'status' in filters:
        filter_expressions.append("accountStatus = :status")
        expression_values[':status'] = filters['status']
    
    if 'start_date' in filters:
        filter_expressions.append("createdAt >= :start_date")
        expression_values[':start_date'] = filters['start_date']
    
    if 'end_date' in filters:
        filter_expressions.append("createdAt <= :end_date")
        expression_values[':end_date'] = filters['end_date']
    
    if filter_expressions:
        scan_kwargs['FilterExpression'] = ' AND '.join(filter_expressions)
        scan_kwargs['ExpressionAttributeValues'] = expression_values
    
    while True:
        response = users_table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    
    return items


def export_orders(filters):
    items = []
    scan_kwargs = {}
    
    filter_expressions = []
    expression_values = {}
    
    if 'status' in filters:
        filter_expressions.append("#status = :status")
        expression_values[':status'] = filters['status']
        scan_kwargs['ExpressionAttributeNames'] = {'#status': 'status'}
    
    if 'start_date' in filters:
        filter_expressions.append("createdAt >= :start_date")
        expression_values[':start_date'] = filters['start_date']
    
    if 'end_date' in filters:
        filter_expressions.append("createdAt <= :end_date")
        expression_values[':end_date'] = filters['end_date']
    
    if 'user_id' in filters:
        filter_expressions.append("user_id = :user_id")
        expression_values[':user_id'] = filters['user_id']
    
    if filter_expressions:
        scan_kwargs['FilterExpression'] = ' AND '.join(filter_expressions)
        scan_kwargs['ExpressionAttributeValues'] = expression_values
    
    while True:
        response = orders_table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    
    return items


def generate_csv(data, export_type):
    if not data:
        return ""
    
    output = StringIO()
    
    if export_type == 'users':
        fieldnames = ['user_id', 'userName', 'email', 'fullName', 'phoneNumber', 
                     'accountStatus', 'createdAt', 'updatedAt']
    elif export_type == 'orders':
        fieldnames = ['order_id', 'user_id', 'status', 'total_amount', 
                     'payment_method', 'createdAt', 'updatedAt']
    else:
        fieldnames = list(data[0].keys())
    
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    
    for item in data:
        row = {}
        for key in fieldnames:
            value = item.get(key, '')
            if isinstance(value, Decimal):
                value = float(value)
            row[key] = value
        writer.writerow(row)
    
    return output.getvalue()


def generate_json(data):
    return json.dumps(data, indent=2, default=decimal_default)


def update_job_status(job_id, status, **kwargs):
    update_expression = "SET #status = :status, updatedAt = :updated"
    expression_values = {
        ':status': status,
        ':updated': datetime.now(timezone.utc).isoformat()
    }
    expression_names = {'#status': 'status'}
    
    if 's3_key' in kwargs:
        update_expression += ", s3_key = :s3_key"
        expression_values[':s3_key'] = kwargs['s3_key']
    
    if 'download_url' in kwargs:
        update_expression += ", download_url = :download_url"
        expression_values[':download_url'] = kwargs['download_url']
    
    if 'record_count' in kwargs:
        update_expression += ", record_count = :record_count"
        expression_values[':record_count'] = kwargs['record_count']
    
    if 'error_message' in kwargs:
        update_expression += ", error_message = :error_message"
        expression_values[':error_message'] = kwargs['error_message']
    
    jobs_table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_names,
        ExpressionAttributeValues=expression_values
    )
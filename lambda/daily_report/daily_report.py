import json
import boto3
import csv
import io
import os
from datetime import datetime, timedelta
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

USERS_TABLE = os.environ['USERS_TABLE']
ORDERS_TABLE = os.environ['ORDERS_TABLE']
REPORTS_BUCKET = os.environ['REPORTS_BUCKET']

class DecimalEncoder(json.JSONEncoder):
    """Helper to convert Decimal to float for JSON serialization"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def scan_table(table_name, limit=None):
    """Scan entire DynamoDB table with pagination"""
    table = dynamodb.Table(table_name)
    items = []
    
    scan_kwargs = {}
    if limit:
        scan_kwargs['Limit'] = limit
    
    try:
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            
        return items
    except Exception as e:
        print(f"Error scanning {table_name}: {str(e)}")
        return []

def convert_dynamodb_to_dict(items):
    """Convert DynamoDB items (with Decimal) to regular dict"""
    return json.loads(json.dumps(items, cls=DecimalEncoder))

def generate_users_csv(users):
    """Generate CSV content for users"""
    if not users:
        return None
    
    fieldnames = ['user_id', 'email', 'userName', 'firstName', 'lastName', 
                  'phoneNumber', 'createdAt', 'updatedAt']
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    
    writer.writeheader()
    for user in users:
        writer.writerow(user)
    
    return output.getvalue()

def generate_orders_csv(orders):
    """Generate CSV content for orders"""
    if not orders:
        return None
    
    fieldnames = ['order_id', 'user_id', 'status', 'totalAmount', 
                  'currency', 'createdAt', 'updatedAt', 'items']
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    
    writer.writeheader()
    for order in orders:
        if 'items' in order:
            order['items'] = json.dumps(order['items'])
        writer.writerow(order)
    
    return output.getvalue()

def generate_payment_pending_report(orders):
    """Generate CSV report for pending payments only"""
    
    pending_orders = [o for o in orders if o.get('status') == 'pending']
    
    if not pending_orders:
        return None
    
    fieldnames = ['order_id', 'user_id', 'totalAmount', 'currency', 
                  'createdAt', 'payment_method', 'customer_email', 'customer_phone']
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    
    writer.writeheader()
    for order in pending_orders:
        row = {
            'order_id': order.get('order_id', ''),
            'user_id': order.get('user_id', ''),
            'totalAmount': order.get('totalAmount', 0),
            'currency': order.get('currency', 'MYR'),
            'createdAt': order.get('createdAt', ''),
            'payment_method': order.get('payment_method', 'N/A'),
            'customer_email': order.get('customer_email', ''),
            'customer_phone': order.get('customer_phone', '')
        }
        writer.writerow(row)
    
    return output.getvalue()

def generate_payment_summary(orders):
    """Generate payment batch summary statistics"""
    
    pending_orders = [o for o in orders if o.get('status') == 'pending']
    
    total_pending = len(pending_orders)
    total_amount = sum(float(o.get('totalAmount', 0)) for o in pending_orders)
    
    payment_methods = {}
    for order in pending_orders:
        method = order.get('payment_method', 'Unknown')
        if method not in payment_methods:
            payment_methods[method] = {'count': 0, 'amount': 0}
        payment_methods[method]['count'] += 1
        payment_methods[method]['amount'] += float(order.get('totalAmount', 0))
    
    currencies = {}
    for order in pending_orders:
        curr = order.get('currency', 'MYR')
        if curr not in currencies:
            currencies[curr] = {'count': 0, 'amount': 0}
        currencies[curr]['count'] += 1
        currencies[curr]['amount'] += float(order.get('totalAmount', 0))
    
    now = datetime.now()
    age_buckets = {'<24h': 0, '1-7d': 0, '7-30d': 0, '>30d': 0}
    for order in pending_orders:
        created = order.get('createdAt', '')
        try:
            created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
            age = (now - created_dt.replace(tzinfo=None)).days
            if age < 1:
                age_buckets['<24h'] += 1
            elif age <= 7:
                age_buckets['1-7d'] += 1
            elif age <= 30:
                age_buckets['7-30d'] += 1
            else:
                age_buckets['>30d'] += 1
        except:
            pass
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Payment Batch Processing Report'])
    writer.writerow(['Generated At', datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')])
    writer.writerow(['Scheduled Processing Time', '11:00 PM UTC'])
    writer.writerow([])
    
    writer.writerow(['PENDING PAYMENTS SUMMARY'])
    writer.writerow(['Total Pending Orders', total_pending])
    writer.writerow(['Total Pending Amount', f'${total_amount:.2f}'])
    writer.writerow([])
    
    writer.writerow(['BY PAYMENT METHOD'])
    for method, data in payment_methods.items():
        writer.writerow([method, f"{data['count']} orders", f"${data['amount']:.2f}"])
    writer.writerow([])
    
    writer.writerow(['BY CURRENCY'])
    for curr, data in currencies.items():
        writer.writerow([curr, f"{data['count']} orders", f"{data['amount']:.2f}"])
    writer.writerow([])
    
    writer.writerow(['ORDER AGE ANALYSIS'])
    writer.writerow(['Less than 24 hours', age_buckets['<24h']])
    writer.writerow(['1-7 days', age_buckets['1-7d']])
    writer.writerow(['7-30 days', age_buckets['7-30d']])
    writer.writerow(['Over 30 days', age_buckets['>30d']])
    writer.writerow([])
    
    writer.writerow(['RECOMMENDATIONS'])
    if age_buckets['>30d'] > 0:
        writer.writerow(['WARNING', f"{age_buckets['>30d']} orders are over 30 days old - Review urgently"])
    if total_pending == 0:
        writer.writerow(['SUCCESS', 'No pending payments to process'])
    else:
        writer.writerow(['INFO', f'{total_pending} payments ready for batch processing'])
    
    return output.getvalue()

def upload_to_s3(content, filename):
    """Upload CSV content to S3"""
    try:
        s3.put_object(
            Bucket=REPORTS_BUCKET,
            Key=filename,
            Body=content.encode('utf-8'),
            ContentType='text/csv',
            ServerSideEncryption='AES256'
        )
        return True
    except Exception as e:
        print(f"Error uploading {filename} to S3: {str(e)}")
        return False

def handler(event, context):
    """Main handler for batch payment pending report generation"""
    print("Starting batch payment pending report generation...")
    print(f"Scheduled run at 11:00 PM UTC")
    
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        print("Fetching users...")
        users = scan_table(USERS_TABLE)
        users = convert_dynamodb_to_dict(users)
        print(f"Found {len(users)} users")
        
        print("Fetching orders...")
        orders = scan_table(ORDERS_TABLE)
        orders = convert_dynamodb_to_dict(orders)
        print(f"Found {len(orders)} orders")
        
        pending_count = sum(1 for o in orders if o.get('status') == 'pending')
        print(f"Found {pending_count} pending payments to process")
        
        uploads = []
        
        payment_pending_csv = generate_payment_pending_report(orders)
        if payment_pending_csv:
            filename = f"payment-batch/{date_str}/pending_payments_{timestamp}.csv"
            if upload_to_s3(payment_pending_csv, filename):
                uploads.append(filename)
                print(f"Uploaded: {filename}")
        else:
            print("No pending payments to process")
        
        payment_summary_csv = generate_payment_summary(orders)
        if payment_summary_csv:
            filename = f"payment-batch/{date_str}/batch_summary_{timestamp}.csv"
            if upload_to_s3(payment_summary_csv, filename):
                uploads.append(filename)
                print(f"Uploaded: {filename}")
        
        users_csv = generate_users_csv(users)
        if users_csv:
            filename = f"payment-batch/{date_str}/users_reference_{timestamp}.csv"
            if upload_to_s3(users_csv, filename):
                uploads.append(filename)
                print(f"Uploaded: {filename}")
        
        orders_csv = generate_orders_csv(orders)
        if orders_csv:
            filename = f"payment-batch/{date_str}/orders_reference_{timestamp}.csv"
            if upload_to_s3(orders_csv, filename):
                uploads.append(filename)
                print(f"Uploaded: {filename}")
        
        print(f"Batch payment report completed. {len(uploads)} files uploaded.")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Batch payment pending report generated successfully',
                'files': uploads,
                'pending_payments_count': pending_count,
                'total_users': len(users),
                'total_orders': len(orders),
                'timestamp': timestamp,
                'scheduled_time': '11:00 PM UTC'
            })
        }
        
    except Exception as e:
        print(f"Error generating batch payment report: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error generating batch payment report',
                'error': str(e)
            })
        }
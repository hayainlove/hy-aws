import json
import os
import boto3
import time
import random
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')  # ✅ ADD THIS

ORDERS_TABLE = os.environ['ORDERS_TABLE']
USER_NOTIFICATION_TOPIC_ARN = os.environ['USER_NOTIFICATION_TOPIC_ARN']  # ✅ ADD THIS


def send_order_notification(order_id, user_id, status, total_amount=None):
    """
    Send SNS notification for order status update
    Returns: bool (success/failure)
    """
    try:
        message = f'''
Order Status Update

Order ID: {order_id}
User ID: {user_id}
Status: {status}
'''
        if total_amount:
            message += f'Total Amount: ${total_amount}\n'
        
        message += f'\nTimestamp: {datetime.utcnow().isoformat()}'
        
        if status == 'COMPLETED':
            subject = f'✅ Order Completed: {order_id}'
        elif status == 'FAILED':
            subject = f'❌ Order Failed: {order_id}'
        else:
            subject = f'Order Update: {order_id}'
        
        response = sns.publish(
            TopicArn=USER_NOTIFICATION_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        
        print(f"✓ SNS notification sent successfully. MessageId: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to send SNS notification: {e}")
        # Don't fail the entire process if notification fails
        return False


def process_single_order(order_id):
    """
    Process a single order
    Returns: (success: bool, error_message: str or None)
    """
    try:
        table = dynamodb.Table(ORDERS_TABLE)
        
        # Get the order
        response = table.get_item(Key={'order_id': order_id})
        if 'Item' not in response:
            print(f"Order not found: {order_id}")
            return False, "Order not found"
        
        order = response['Item']
        user_id = order.get('user_id', 'unknown')
        total_amount = order.get('total', 0)
        
        # Update status to PROCESSING
        table.update_item(
            Key={'order_id': order_id},
            UpdateExpression='SET #status = :status, updatedAt = :updated',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'PROCESSING',
                ':updated': datetime.utcnow().isoformat()
            }
        )
        
        print(f"Processing order {order_id} with {len(order['items'])} items, total: ${total_amount}")
        
        # Simulate payment processing
        print(f"  [1/4] Validating payment for ${total_amount}...")
        time.sleep(1)  # Simulate API call
        
        # Simulate random failures (10% chance)
        if random.random() < 0.1:
            raise Exception("Payment gateway timeout")
        
        # Simulate inventory check
        print(f"  [2/4] Checking inventory for {len(order['items'])} items...")
        time.sleep(0.5)
        
        # Simulate inventory update
        print(f"  [3/4] Updating inventory...")
        time.sleep(0.5)
        
        # Simulate notification
        print(f"  [4/4] Sending confirmation to user {user_id}...")
        time.sleep(0.5)
        
        # Update status to COMPLETED
        table.update_item(
            Key={'order_id': order_id},
            UpdateExpression='SET #status = :status, updatedAt = :updated, processedAt = :processed',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'COMPLETED',
                ':updated': datetime.utcnow().isoformat(),
                ':processed': datetime.utcnow().isoformat()
            }
        )
        
        print(f"✓ Order {order_id} completed successfully")
        
        # ✅ Send success notification
        send_order_notification(
            order_id=order_id,
            user_id=user_id,
            status='COMPLETED',
            total_amount=total_amount
        )
        
        return True, None
        
    except Exception as e:
        error_msg = str(e)
        print(f"✗ Error processing order {order_id}: {error_msg}")
        
        # Update status to FAILED
        try:
            table.update_item(
                Key={'order_id': order_id},
                UpdateExpression='SET #status = :status, updatedAt = :updated, errorMessage = :error',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':status': 'FAILED',
                    ':updated': datetime.utcnow().isoformat(),
                    ':error': error_msg
                }
            )
            
            # ✅ Send failure notification
            send_order_notification(
                order_id=order_id,
                user_id=order.get('user_id', 'unknown'),
                status='FAILED'
            )
            
        except Exception as update_error:
            print(f"Failed to update order status: {update_error}")
        
        return False, error_msg


def handler(event, context):
    """
    Process orders from SQS queue
    This Lambda is triggered by SQS and processes orders concurrently
    """
    print(f"Received batch of {len(event['Records'])} orders to process")
    
    # Track failed messages for partial batch response
    failed_items = []
    
    for record in event['Records']:
        try:
            # Parse message
            message_body = json.loads(record['body'])
            order_id = message_body['order_id']
            
            print(f"\n=== Processing order: {order_id} ===")
            
            # Process the order
            success, error = process_single_order(order_id)
            
            if not success:
                # Add to failed items so SQS will retry
                failed_items.append({
                    'itemIdentifier': record['messageId']
                })
                print(f"Order {order_id} failed and will be retried")
            
        except Exception as e:
            print(f"Error processing record: {e}")
            import traceback
            traceback.print_exc()
            
            # Add to failed items
            failed_items.append({
                'itemIdentifier': record['messageId']
            })
    
    # Return partial batch response
    # Failed items will be retried, successful ones will be deleted from queue
    response = {
        'batchItemFailures': failed_items
    }
    
    print(f"\nBatch processing complete: {len(event['Records']) - len(failed_items)} succeeded, {len(failed_items)} failed")
    
    return response
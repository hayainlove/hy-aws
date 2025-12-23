import json
import os
import boto3

sns_client = boto3.client('sns')
TOPIC_ARN = os.environ['USER_NOTIFICATION_TOPIC_ARN']

def handler(event, context):
    """
    Send email notification via SNS
    
    Expected event:
    {
        "subject": "Welcome to MyHayati",
        "message": "Hello Haya, your account has been created!",
        "user_email": "haya.seas@example.com"  # Optional
    }
    """
    try:
        subject = event.get('subject', 'MyHayati Notification')
        message = event.get('message', '')
        
        if not message:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Message is required'})
            }
        
        # Publish to SNS topic
        response = sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notification sent successfully',
                'messageId': response['MessageId']
            })
        }
    
    except Exception as e:
        print(f"Error sending notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
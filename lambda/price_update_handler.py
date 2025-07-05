import json
import os
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Lambda function to handle Amazon SP-API price change events
    and update DynamoDB with new pricing information
    """
    
    table_name = os.environ['DYNAMODB_TABLE']
    markup_percentage = float(os.environ.get('MARKUP_PERCENTAGE', '15'))
    
    table = dynamodb.Table(table_name)
    
    try:
        # Parse the EventBridge event
        detail = event.get('detail', {})
        
        # Extract product information from the event
        asin = detail.get('asin')
        marketplace_id = detail.get('marketplaceId')
        
        if not asin or not marketplace_id:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing required fields: asin or marketplaceId')
            }
        
        # Get current offer information
        offers = detail.get('offers', [])
        
        if not offers:
            return {
                'statusCode': 200,
                'body': json.dumps('No offers found in event')
            }
        
        # Find the lowest price among offers
        lowest_price = None
        for offer in offers:
            price_info = offer.get('listingPrice', {})
            amount = price_info.get('amount')
            
            if amount and (lowest_price is None or amount < lowest_price):
                lowest_price = amount
        
        if lowest_price is None:
            return {
                'statusCode': 200,
                'body': json.dumps('No valid pricing found')
            }
        
        # Calculate new price with markup
        new_price = lowest_price * (1 + markup_percentage / 100)
        
        # Update DynamoDB
        response = table.update_item(
            Key={
                'asin': asin,
                'marketplace_id': marketplace_id
            },
            UpdateExpression='SET updated_price = :price, last_updated = :timestamp',
            ExpressionAttributeValues={
                ':price': Decimal(str(round(new_price, 2))),
                ':timestamp': context.aws_request_id
            },
            ReturnValues='UPDATED_NEW'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price updated successfully',
                'asin': asin,
                'old_price': lowest_price,
                'new_price': round(new_price, 2),
                'updated_attributes': response.get('Attributes', {})
            })
        }
        
    except Exception as e:
        print(f"Error processing price update: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
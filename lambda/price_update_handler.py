import json
import os
import boto3
import requests
from decimal import Decimal
from db_utils import get_db_connection

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
        
        # Get existing item to check min/max prices
        existing_item = table.get_item(
            Key={'asin': asin, 'marketplace_id': marketplace_id}
        ).get('Item', {})
        
        min_price = float(existing_item.get('min_price', 0))
        max_price = float(existing_item.get('max_price', float('inf')))
        
        # Calculate new price with markup
        new_price = lowest_price * (1 + markup_percentage / 100)
        
        # Apply min/max constraints
        if new_price < min_price:
            new_price = min_price
        elif new_price > max_price:
            new_price = max_price
        
        # Calculate business price (1% lower than main price)
        business_price = new_price * 0.99
        min_business_price = float(existing_item.get('min_business_price', 0))
        max_business_price = float(existing_item.get('max_business_price', float('inf')))
        
        if business_price < min_business_price:
            business_price = min_business_price
        elif business_price > max_business_price:
            business_price = max_business_price
        
        # Update both DynamoDB and database
        response = table.update_item(
            Key={
                'asin': asin,
                'marketplace_id': marketplace_id
            },
            UpdateExpression='SET updated_price = :price, business_price = :bprice, last_updated = :timestamp',
            ExpressionAttributeValues={
                ':price': Decimal(str(round(new_price, 2))),
                ':bprice': Decimal(str(round(business_price, 2))),
                ':timestamp': context.aws_request_id
            },
            ReturnValues='UPDATED_NEW'
        )
        

        
        # Update SP-API prices
        sku = existing_item.get('sku')
        if sku:
            update_spapi_prices(sku, new_price, business_price, marketplace_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price updated successfully',
                'asin': asin,
                'old_price': lowest_price,
                'new_price': round(new_price, 2),
                'business_price': round(business_price, 2),
                'updated_attributes': response.get('Attributes', {})
            })
        }
        
    except Exception as e:
        print(f"Error processing price update: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def update_spapi_prices(sku, regular_price, business_price, marketplace_id):
    """Update prices via SP-API endpoints"""
    access_token = os.environ.get('SPAPI_ACCESS_TOKEN')
    
    if not access_token:
        print("No SP-API access token configured")
        return
    
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }
    
    # Calculate quantity discounts
    quantity_discounts = [
        {'quantity': 5, 'type': 'FIXED', 'amount': {'currency': 'USD', 'amount': round(business_price * 0.99, 2)}},
        {'quantity': 10, 'type': 'FIXED', 'amount': {'currency': 'USD', 'amount': round(business_price * 0.98, 2)}},
        {'quantity': 25, 'type': 'FIXED', 'amount': {'currency': 'USD', 'amount': round(business_price * 0.97, 2)}},
        {'quantity': 50, 'type': 'FIXED', 'amount': {'currency': 'USD', 'amount': round(business_price * 0.96, 2)}},
        {'quantity': 100, 'type': 'FIXED', 'amount': {'currency': 'USD', 'amount': round(business_price * 0.95, 2)}}
    ]
    
    # Update with both regular and business pricing
    payload = {
        'productType': 'PRODUCT',
        'patches': [
            {
                'op': 'replace',
                'path': '/attributes/purchasable_offer',
                'value': [{
                    'marketplace_id': marketplace_id,
                    'currency': 'USD',
                    'our_price': [{'schedule': [{'value_with_tax': regular_price}]}]
                }]
            },
            {
                'op': 'replace',
                'path': '/attributes/businessPricing',
                'value': {
                    'businessPrice': {
                        'currency': 'USD',
                        'amount': business_price
                    },
                    'quantityDiscounts': quantity_discounts
                }
            }
        ]
    }
    
    try:
        requests.patch(
            f'https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{sku}',
            headers=headers,
            json=payload,
            params={'marketplaceIds': marketplace_id}
        )
        
    except Exception as e:
        print(f"Error updating SP-API prices: {str(e)}")
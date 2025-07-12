import json
import os
import boto3
import requests
from decimal import Decimal
from db_utils import get_db_connection
from spapi_utils import get_spapi_credentials

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Lambda function to poll SP-API hourly for pricing data
    """
    
    table_name = os.environ['DYNAMODB_TABLE']
    marketplace_id = os.environ['MARKETPLACE_ID']
    spapi_creds = get_spapi_credentials()
    
    table = dynamodb.Table(table_name)
    
    try:
        # Get access token
        access_token = get_access_token()
        if not access_token:
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to get access token')
            }
        
        # Scan DynamoDB for items updated in the last hour
        from boto3.dynamodb.conditions import Attr
        import time
        
        one_hour_ago = int(time.time()) - 3600
        
        response = table.scan(
            FilterExpression=Attr('updated_price').exists() & Attr('last_updated_timestamp').gte(one_hour_ago)
        )
        
        updated_count = 0
        for item in response['Items']:
            asin = item['asin']
            updated_price = float(item.get('updated_price', 0))
            business_price = float(item.get('business_price', 0))
            
            if updated_price > 0:
                success = update_amazon_price(asin, updated_price, business_price, marketplace_id, access_token)
                if success:
                    updated_count += 1
                    # Remove the updated_price flag after successful update
                    table.update_item(
                        Key={'asin': asin, 'marketplace_id': marketplace_id},
                        UpdateExpression='REMOVE updated_price'
                    )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price updates completed',
                'updated_count': updated_count
            })
        }
        
    except Exception as e:
        print(f"Error in SP-API polling: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def get_access_token():
    """Generate SP-API access token using LWA credentials"""
    try:
        spapi_creds = get_spapi_credentials()
        
        token_url = "https://api.amazon.com/auth/o2/token"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': spapi_creds['refresh_token'],
            'client_id': spapi_creds['lwa_app_id'],
            'client_secret': spapi_creds['lwa_client_secret']
        }
        
        response = requests.post(token_url, data=payload)
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            print(f"Token refresh failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error getting access token: {str(e)}")
        return None

def update_amazon_price(asin, regular_price, business_price, marketplace_id, access_token):
    """Update Amazon listing prices via SP-API Product Pricing API"""
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }
    
    # Calculate business quantity discounts
    quantity_discounts = [
        {'quantityTier': 5, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.99, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 10, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.98, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 25, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.97, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 50, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.96, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 100, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.95, 2), 'currencyCode': 'USD'}}
    ]
    
    payload = {
        'requests': [{
            'uri': f'/products/pricing/v0/items/{asin}/offers',
            'method': 'PUT',
            'body': {
                'MarketplaceId': marketplace_id,
                'RegularPrice': {
                    'Amount': regular_price,
                    'CurrencyCode': 'USD'
                },
                'BusinessPrice': {
                    'Amount': business_price,
                    'CurrencyCode': 'USD'
                },
                'QuantityDiscountPrices': quantity_discounts
            }
        }]
    }
    
    try:
        print(f"Updating ASIN {asin}: Regular=${regular_price}, Business=${business_price} with quantity discounts")
        response = requests.post(
            'https://sellingpartnerapi-na.amazon.com/products/pricing/v0/offers/batch',
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            print(f"Successfully updated pricing for ASIN {asin}")
            return True
        else:
            print(f"Failed to update ASIN {asin}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"Error updating ASIN {asin}: {str(e)}")
        return False
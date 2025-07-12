import json
import os
import boto3
import urllib3
import asyncio
import concurrent.futures
import time
from decimal import Decimal
from spapi_utils import get_spapi_credentials

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    return asyncio.run(async_lambda_handler(event, context))

async def async_lambda_handler(event, context):
    """
    Lambda function to poll SP-API hourly for pricing data
    """
    
    table_name = os.environ['DYNAMODB_TABLE']
    marketplace_id = os.environ['MARKETPLACE_ID']
    
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
        
        one_hour_ago = int(time.time()) - 3600
        
        response = table.scan(
            FilterExpression=Attr('updated_price').exists() & Attr('last_updated_timestamp').gte(one_hour_ago)
        )
        
        # Prepare batch requests for patchListingsItem
        batch_requests = []
        for item in response['Items']:
            asin = item['asin']
            updated_price = float(item.get('updated_price', 0))
            business_price = float(item.get('business_price', 0))
            
            if updated_price > 0:
                batch_requests.append({
                    'uri': f'/listings/2021-08-01/items/{asin}',
                    'method': 'PATCH',
                    'body': create_patch_payload(updated_price, business_price, marketplace_id)
                })
        
        # Send parallel PATCH requests
        updated_count = 0
        if batch_requests:
            success_asins = await send_parallel_patch_requests(response['Items'], access_token, marketplace_id)
            updated_count = len(success_asins)
            
            # Remove updated_price flag for successfully updated items
            for asin in success_asins:
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
        print(f"Error in price patching: {str(e)}")
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
        
        import urllib.parse
        http = urllib3.PoolManager()
        encoded_data = urllib.parse.urlencode(payload)
        response = http.request(
            'POST',
            token_url,
            body=encoded_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        if response.status == 200:
            token_data = json.loads(response.data.decode('utf-8'))
            return token_data.get('access_token')
        else:
            print(f"Token refresh failed: {response.status} - {response.data.decode('utf-8')}")
            return None
    except Exception as e:
        print(f"Error getting access token: {str(e)}")
        return None

def create_patch_payload(regular_price, business_price, marketplace_id):
    """Create patch payload for patchListingsItem"""
    quantity_discounts = [
        {'quantityTier': 5, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.99, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 10, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.98, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 25, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.97, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 50, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.96, 2), 'currencyCode': 'USD'}},
        {'quantityTier': 100, 'quantityDiscountType': 'QUANTITY_DISCOUNT', 'listingPrice': {'amount': round(business_price * 0.95, 2), 'currencyCode': 'USD'}}
    ]
    
    return {
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
                'path': '/attributes/business_price',
                'value': [{
                    'marketplace_id': marketplace_id,
                    'value_with_tax': business_price,
                    'currency': 'USD'
                }]
            },
            {
                'op': 'replace',
                'path': '/attributes/quantity_discount_prices',
                'value': quantity_discounts
            }
        ]
    }

def patch_single_item(asin, regular_price, business_price, marketplace_id, access_token):
    """Send single PATCH request for one item"""
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }
    
    payload = create_patch_payload(regular_price, business_price, marketplace_id)
    
    try:
        http = urllib3.PoolManager()
        response = http.request(
            'PATCH',
            f'https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{asin}?marketplaceIds={marketplace_id}',
            headers=headers,
            body=json.dumps(payload)
        )
        
        if response.status == 200:
            print(f"Successfully updated ASIN {asin}")
            return asin
        else:
            print(f"Failed to update ASIN {asin}: {response.status}")
            return None
            
    except Exception as e:
        print(f"Error updating ASIN {asin}: {str(e)}")
        return None

async def send_parallel_patch_requests(items, access_token, marketplace_id):
    """Send parallel PATCH requests using ThreadPoolExecutor"""
    loop = asyncio.get_event_loop()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        tasks = []
        for item in items:
            asin = item['asin']
            updated_price = float(item.get('updated_price', 0))
            business_price = float(item.get('business_price', 0))
            
            if updated_price > 0:
                task = loop.run_in_executor(
                    executor, 
                    patch_single_item, 
                    asin, updated_price, business_price, marketplace_id, access_token
                )
                tasks.append(task)
        
        print(f"Sending {len(tasks)} parallel PATCH requests")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        success_asins = [asin for asin in results if asin and not isinstance(asin, Exception)]
        print(f"Parallel update completed: {len(success_asins)}/{len(tasks)} successful")
        
        return success_asins
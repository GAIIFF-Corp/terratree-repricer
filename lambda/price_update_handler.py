import json
import os
import boto3
import urllib3
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
        # Parse SP-API notification
        payload = event.get('Payload', {})
        notification = payload.get('AnyOfferChangedNotification', {})
        
        # Extract ASIN and marketplace from OfferChangeTrigger
        trigger = notification.get('OfferChangeTrigger', {})
        asin = trigger.get('ASIN')
        marketplace_id = trigger.get('MarketplaceId')
        
        if not asin or not marketplace_id:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing ASIN or MarketplaceId')
            }
        
        # Get offers and summary data
        offers = notification.get('Offers', [])
        summary = notification.get('Summary', {})
        lowest_prices = summary.get('LowestPrices', [])
        
        if not offers:
            return {
                'statusCode': 200,
                'body': json.dumps('No offers found in event')
            }
        
        # Find lowest competitor price from LowestPrices
        lowest_price = None
        for price_info in lowest_prices:
            if price_info.get('Condition') == 'new':
                listing_price = price_info.get('ListingPrice', {})
                amount = listing_price.get('Amount')
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
        
        # Only reprice if offer price is above our minimum price
        if lowest_price <= min_price:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No repricing needed - offer price at or below minimum',
                    'offer_price': lowest_price,
                    'min_price': min_price
                })
            }
        
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
        
        # Store competitor offers
        competitor_offers = []
        for offer in offers:
            listing_price = offer.get('ListingPrice', {})
            if listing_price.get('Amount'):
                competitor_offers.append({
                    'seller_id': offer.get('SellerId'),
                    'price': Decimal(str(listing_price['Amount'])),
                    'currency': listing_price.get('CurrencyCode', 'USD'),
                    'condition': offer.get('SubCondition'),
                    'is_fba': offer.get('IsFulfilledByAmazon', False)
                })
        
        # Update DynamoDB with new prices and competitor data
        response = table.update_item(
            Key={
                'asin': asin,
                'marketplace_id': marketplace_id
            },
            UpdateExpression='SET updated_price = :price, business_price = :bprice, last_updated = :timestamp, competitor_offers = :offers',
            ExpressionAttributeValues={
                ':price': Decimal(str(round(new_price, 2))),
                ':bprice': Decimal(str(round(business_price, 2))),
                ':timestamp': context.aws_request_id,
                ':offers': competitor_offers
            },
            ReturnValues='UPDATED_NEW'
        )
        

        
        # Update SP-API prices
        sku = existing_item.get('sku')
        if sku:
            print(f"Found SKU: {sku}, updating SP-API prices")
            update_spapi_prices(sku, new_price, business_price, marketplace_id)
        else:
            print("No SKU found in existing item, skipping SP-API update")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price updated successfully',
                'asin': asin,
                'old_price': lowest_price,
                'new_price': round(new_price, 2),
                'business_price': round(business_price, 2)
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
        print(f"Sending PATCH request for SKU: {sku}, Regular Price: {regular_price}, Business Price: {business_price}")
        http = urllib3.PoolManager()
        response = http.request(
            'PATCH',
            f'https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{sku}?marketplaceIds={marketplace_id}',
            headers=headers,
            body=json.dumps(payload)
        )
        print(f"SP-API PATCH response status: {response.status}")
        
    except Exception as e:
        print(f"Error updating SP-API prices: {str(e)}")
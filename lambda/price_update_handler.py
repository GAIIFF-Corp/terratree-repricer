import json
import os
import boto3
import time
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
        event_time = event.get('EventTime')
        
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
        
        # Check if we are already the featured offer
        our_seller_id = 'AERPN1UM8O1I4'
        featured_offer_price = None
        we_are_featured = False
        
        # Find the featured offer (lowest price)
        for price_info in lowest_prices:
            if price_info.get('Condition') == 'new':
                listing_price = price_info.get('ListingPrice', {})
                amount = listing_price.get('Amount')
                if amount:
                    featured_offer_price = amount
                    break
        
        # Check if we are the featured seller
        for offer in offers:
            if offer.get('SellerId') == our_seller_id:
                offer_price = offer.get('ListingPrice', {}).get('Amount')
                if offer_price and offer_price == featured_offer_price:
                    we_are_featured = True
                    break
        
        if we_are_featured:
            return {
                'statusCode': 200,
                'body': json.dumps('We are already the featured offer - no repricing needed')
            }
        
        if featured_offer_price is None:
            return {
                'statusCode': 200,
                'body': json.dumps('No valid pricing found')
            }
        
        # Get existing item to check min/max prices
        existing_item = table.get_item(
            Key={'asin': asin, 'marketplace_id': marketplace_id}
        ).get('Item', {})
        
        min_price = float(existing_item.get('min_price', 0))
        
        # Set new price to 1 cent below featured offer
        new_price = featured_offer_price - 0.01
        
        # Only reprice if new price is above our minimum price
        if new_price <= min_price:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No repricing needed - would be at or below minimum',
                    'featured_price': featured_offer_price,
                    'target_price': new_price,
                    'min_price': min_price
                })
            }
        
        max_price = float(existing_item.get('max_price', float('inf')))
        
        # Apply max constraint
        if new_price > max_price:
            new_price = max_price
        
        # Calculate business price (1 cent below regular price)
        business_price = new_price - 0.01
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
            UpdateExpression='SET updated_price = :price, business_price = :bprice, last_updated = :timestamp, last_updated_timestamp = :ts, competitor_offers = :offers',
            ExpressionAttributeValues={
                ':price': Decimal(str(round(new_price, 2))),
                ':bprice': Decimal(str(round(business_price, 2))),
                ':timestamp': context.aws_request_id,
                ':ts': int(time.mktime(time.strptime(event_time, '%Y-%m-%dT%H:%M:%S.%fZ'))),
                ':offers': competitor_offers
            },
            ReturnValues='UPDATED_NEW'
        )
        

        
        # Price update will be handled by hourly poller
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price updated successfully',
                'asin': asin,
                'featured_offer_price': featured_offer_price,
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
import json
import os
import boto3
import requests
from decimal import Decimal
from db_utils import get_db_connection

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Lambda function to poll SP-API hourly for pricing data
    """
    
    table_name = os.environ['DYNAMODB_TABLE']
    access_token = os.environ['SPAPI_ACCESS_TOKEN']
    marketplace_id = os.environ['MARKETPLACE_ID']
    
    table = dynamodb.Table(table_name)
    
    try:
        # Get all SKUs and ASINs from database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sku, asin FROM products WHERE active = true")
                products = cur.fetchall()
                
        skus = [p[0] for p in products if p[0]]
        asins = [p[1] for p in products]
        
        # Call getListingOfferBatch for SKUs
        if skus:
            listing_offers = get_listing_offer_batch(skus, marketplace_id, access_token)
            update_pricing_from_offers(table, listing_offers)
        
        # Call getCompetitiveSummary for ASINs
        if asins:
            competitive_summary = get_competitive_summary(asins, marketplace_id, access_token)
            update_pricing_from_summary(table, competitive_summary)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'SP-API polling completed',
                'processed_skus': len(skus),
                'processed_asins': len(asins)
            })
        }
        
    except Exception as e:
        print(f"Error in SP-API polling: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def get_listing_offer_batch(skus, marketplace_id, access_token):
    """Call SP-API getListingOfferBatch"""
    url = f"https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/offers/batch"
    
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }
    
    payload = {
        'requests': [{'uri': f'/listings/2021-08-01/offers/{sku}', 'method': 'GET'} for sku in skus[:20]]  # Batch limit
    }
    
    response = requests.post(url, headers=headers, json=payload)
    return response.json() if response.status_code == 200 else {}

def get_competitive_summary(asins, marketplace_id, access_token):
    """Call SP-API getCompetitiveSummary"""
    url = f"https://sellingpartnerapi-na.amazon.com/products/pricing/v0/competitivePrice"
    
    headers = {
        'x-amz-access-token': access_token
    }
    
    params = {
        'MarketplaceId': marketplace_id,
        'Asins': ','.join(asins[:20])  # API limit
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.status_code == 200 else {}

def update_pricing_from_offers(table, offers_data):
    """Update DynamoDB with listing offer data"""
    for response in offers_data.get('responses', []):
        if response.get('status', {}).get('code') == 200:
            body = response.get('body', {})
            # Extract pricing and update table
            # Implementation depends on SP-API response structure

def update_pricing_from_summary(table, summary_data):
    """Update DynamoDB with competitive summary data"""
    for asin, data in summary_data.items():
        if 'Product' in data:
            # Extract competitive pricing and update table
            # Implementation depends on SP-API response structure
            pass
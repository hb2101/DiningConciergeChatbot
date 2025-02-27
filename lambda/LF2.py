import boto3
import json
import requests
import os
import logging
from requests.auth import HTTPBasicAuth

# Configure logging for Lambda (CloudWatch-friendly)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

# Environment configurations
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/850995547368/DiningRequestsQueue'
DYNAMODB_TABLE = 'yelp-restaurants'
SENDER_EMAIL = 'hb2987@nyu.edu'  # Ensure this email is verified in AWS SES
ES_URL = "https://search-cloud-assignment-2lru2uzo676yhjv2pmnbamhhd4.us-east-1.es.amazonaws.com"

# OpenSearch credentials from environment variables
ES_USERNAME = os.getenv('ELASTICSEARCH_USERNAME')
ES_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD')

def send_email(recipient, subject, body):
    """Sends an email via AWS SES."""
    try:
        logger.info(f"Sending email to {recipient} with subject: {subject}")
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        logger.info(f"Email sent successfully! SES Message ID: {response['MessageId']}")
        return True
    except ses.exceptions.MessageRejected as e:
        logger.error(f"SES Message Rejected: {str(e)}")
    except ses.exceptions.ClientError as e:
        logger.error(f"SES Client Error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected SES Error: {str(e)}")
    return False

def get_restaurant_details(restaurant_ids):
    """Fetches restaurant details from DynamoDB."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    restaurant_details = []
    try:
        logger.info(f"Fetching restaurant details for IDs: {restaurant_ids}")
        for rid in restaurant_ids:
            response = table.get_item(Key={'businessId': rid})
            if 'Item' in response:
                restaurant_details.append(response['Item'])
            else:
                logger.warning(f"No item found in DynamoDB for RestaurantID: {rid}")
        logger.info(f"Retrieved restaurant details: {restaurant_details}")
        return restaurant_details
    except Exception as e:
        logger.error(f"Error fetching restaurant details: {str(e)}")
        raise  # Re-raise to catch in caller if needed

def fetch_restaurant_ids_from_elasticsearch(cuisine):
    """Fetches restaurant IDs based on cuisine from OpenSearch."""
    try:
        if not ES_USERNAME or not ES_PASSWORD:
            logger.error("OpenSearch credentials are missing. Set ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD environment variables.")
            return []

        logger.info(f"Fetching restaurant IDs for cuisine: {cuisine}")
        response = requests.get(
            f"{ES_URL}/restaurants/_search",
            json={
                "query": {
                    "function_score": {
                        "query": {"match": {"Cuisine": cuisine}},
                        "random_score": {}
                    }
                },
                "size": 3  # Fetch up to 3 restaurants
            },
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            timeout=5
        )

        if response.status_code != 200:
            logger.error(f"OpenSearch error {response.status_code}: {response.text}")
            return []

        response_json = response.json()
        hits = response_json.get('hits', {}).get('hits', [])
        restaurant_ids = [hit['_source']['RestaurantID'] for hit in hits]
        logger.info(f"Extracted restaurant IDs: {restaurant_ids}")
        return restaurant_ids

    except requests.exceptions.RequestException as e:
        logger.error(f"OpenSearch request failed: {str(e)}")
        return []

def delete_sqs_message(receipt_handle):
    """Deletes a processed message from SQS."""
    try:
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        logger.info("Message deleted from SQS successfully.")
    except Exception as e:
        logger.error(f"SQS Delete Error: {str(e)}")

def process_message(message):
    """Processes an SQS message, fetches restaurant data, sends an email, and deletes the message."""
    try:
        logger.info(f"Processing message: {message}")
        body = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']

        # Handle both possible structures (slots vs direct fields)
        cuisine = body.get('Cuisine') or body.get('slots', {}).get('cuisine')
        email = body.get('Email') or body.get('slots', {}).get('email')

        if not cuisine or not email:
            logger.warning("Missing required slots (cuisine or email).")
            return
            

        restaurant_ids = fetch_restaurant_ids_from_elasticsearch(cuisine)
        if not restaurant_ids:
            logger.info(f"No restaurants found for cuisine: {cuisine}")
            return

        restaurants = get_restaurant_details(restaurant_ids)
        if not restaurants:
            logger.info(f"No restaurant details retrieved for IDs: {restaurant_ids}")
            return
       
        #print(restaurants)
        recommendations = "\n".join(
            [f"{r['name']}, located at {r['address']} (rating: {r.get('rating', 'N/A')})" for r in restaurants]
        )
        subject = f"Your {cuisine.capitalize()} Restaurant Recommendations!"
        email_body = f"Hello! Here are your recommendations:\n\n{recommendations}"

        if send_email(email, subject, email_body):
            delete_sqs_message(receipt_handle)
        else:
            logger.error("Email sending failed. SQS message not deleted.")

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

def lambda_handler(event, context):
    """Lambda handler function to process SQS messages."""
    
    try:
        logger.info(f"Received event: {json.dumps(event)}")
       
        if 'Records' in event:
            logger.info(f"Received {len(event['Records'])} messages.")
            for message in event['Records']:
                process_message(message)
        else:
            logger.info("No messages received.")
    except Exception as e:
        logger.error(f"Error in Lambda handler: {str(e)}")
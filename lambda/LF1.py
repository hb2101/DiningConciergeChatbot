'''
import json

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    
    if intent_name == "GreetingIntent":
        return generate_response("Hi there, how can I help?")
    
    elif intent_name == "ThankYouIntent":
        return generate_response("You're welcome! Let me know if you need anything else.")
    
    elif intent_name == "DiningSuggestionsIntent":
        slots = event['sessionState']['intent']['slots']
        location = slots['Location']['value']['interpretedValue']
        cuisine = slots['Cuisine']['value']['interpretedValue']
        dining_time = slots['DiningTime']['value']['interpretedValue']
        num_people = slots['NumPeople']['value']['interpretedValue']
        email = slots['Email']['value']['interpretedValue']
        
        # Simulating sending data to a queue (SQS)
        print(f"Received request: {location}, {cuisine}, {dining_time}, {num_people}, {email}")
        
        return generate_response(f"Got it! I’ll send {cuisine} restaurant recommendations for {num_people} people at {dining_time} in {location} to {email}.")

def generate_response(message):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "state": "Fulfilled"
            }
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }
'''

import json
import boto3

sqs = boto3.client('sqs')

SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/850995547368/DiningRequestsQueue"

def lambda_handler(event, context):
    """Handles Lex chatbot intents and pushes data to SQS for Dining Suggestions."""

    intent_name = event['sessionState']['intent']['name']

    if intent_name == "GreetingIntent":
        return generate_response("Hi there, how can I help?")

    elif intent_name == "ThankYouIntent":
        return generate_response("You're welcome! Let me know if you need anything else.")

    elif intent_name == "DiningSuggestionsIntent":
        slots = event['sessionState']['intent']['slots']
        location = slots['Location']['value']['interpretedValue']
        cuisine = slots['Cuisine']['value']['interpretedValue']
        dining_time = slots['DiningTime']['value']['interpretedValue']
        num_people = slots['NumPeople']['value']['interpretedValue']
        email = slots['Email']['value']['interpretedValue']

        # Create the request payload for SQS
        request_data = {
            "Location": location,
            "Cuisine": cuisine,
            "DiningTime": dining_time,
            "NumPeople": num_people,
            "Email": email
        }

        # Push the request to SQS
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(request_data)
        )

        # Log for debugging
        print(f"SQS Message ID: {response['MessageId']}")
        print(f"Sent request to SQS: {request_data}")

        return generate_response(f"Got it! I’ll send {cuisine} restaurant recommendations for {num_people} people at {dining_time} in {location} to {email}.")

def generate_response(message):
    """Generates a Lex response."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "state": "Fulfilled"
            }
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

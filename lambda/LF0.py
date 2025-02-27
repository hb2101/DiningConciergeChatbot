import json
import boto3

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    # Extract user message safely
    try:
        body = json.loads(event.get('body', '{}'))  # Ensure event['body'] exists
        message_to_lex = body.get('message', '').strip()  # Extract the actual text message
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }

    if not message_to_lex:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Message is required"})
        }

    print("User message:", message_to_lex)

    # Initialize Lex client
    lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

    try:
        # Send message to Lex
        response = lex_client.recognize_text(
            botId='IAVLCX2GGG',       
            botAliasId='TSTALIASID', 
            localeId='en_US',         
            sessionId='123456',        
            text=message_to_lex  
        )

        print("Lex raw response:", json.dumps(response))

        # Extract chatbot's reply
        lex_response_text = "Sorry, I didn't understand that."
        if "messages" in response and len(response["messages"]) > 0:
            lex_response_text = response["messages"][0].get("content", lex_response_text)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"response": lex_response_text})  # Clean response
        }
    
    except Exception as e:
        print("Error calling Lex:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error", "error": str(e)})
        }

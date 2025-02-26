# from elasticsearch import Elasticsearch, helpers
# from elastic_transport import RequestsHttpNode
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
import boto3
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth
import os
import csv

load_dotenv()
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_REGION')

if not aws_access_key_id:
    print("Error: aws_access_key_id not found. Check .env file.")
    exit(1)

if not aws_secret_access_key:
    print("Error: aws_secret_access_key not found. Check .env file.")
    exit(1)


opensearch_endpoint = os.getenv('OPENSEARCH_ENDPOINT')
if not opensearch_endpoint:
    raise ValueError("OPENSEARCH_ENDPOINT not found in environment variables")

# Create AWS authentication object
awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region, 'es')

# Initialize ElasticSearch client
client = OpenSearch(
    hosts=[opensearch_endpoint],
    http_auth=awsauth,
    verify_certs=True,
    # node_class=RequestsHttpNode,
    connection_class=RequestsHttpConnection
)

def read_from_csv(filename="restaurants.csv"):
    restaurants = []
    with open(filename, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            restaurants.append({
                "Cuisine": row['Cuisine'],
                "RestaurantID": row['Restaurant ID']
            })
    return restaurants

# def insert_into_elasticsearch(restaurants):
#     actions = [
#         {
#             "_index": "restaurants",
#             "_id": restaurant['RestaurantID'],
#             "_source": {
#                 "RestaurantID": restaurant['RestaurantID'],
#                 "Cuisine": restaurant['Cuisine']
#             }
#         }
#         for restaurant in restaurants
#     ]
    
#     try:
#         # Create the index if it doesn't exist
#         if not es.indices.exists(index="restaurants"):
#             es.indices.create(index="restaurants")
        
#         # Bulk insert into ElasticSearch
#         success, _ = es.bulk(body=actions)
#         print(f"Successfully indexed {len(actions)} restaurants")
#     except Exception as e:
#         print(f"Error inserting into ElasticSearch: {str(e)}")

# if __name__ == "__main__":
#     restaurants = read_from_csv()
#     insert_into_elasticsearch(restaurants)
#     print("ElasticSearch population complete.")

def insert_into_opensearch(restaurants):
    actions = [
        {
            "_index": "restaurants",
            "_id": restaurant['RestaurantID'],
            "_source": {
                "RestaurantID": restaurant['RestaurantID'],
                "Cuisine": restaurant['Cuisine']
            }
        }
        for restaurant in restaurants
    ]
    
    try:
        # Create the index if it doesn't exist
        if not client.indices.exists(index="restaurants"):
            client.indices.create(index="restaurants")
        
        # Bulk insert into OpenSearch
        success, _ = helpers.bulk(client, actions)
        print(f"Successfully indexed {success} restaurants")
    except Exception as e:
        print(f"Error inserting into OpenSearch: {str(e)}")

if __name__ == "__main__":
    restaurants = read_from_csv()
    insert_into_opensearch(restaurants)
    print("OpenSearch population complete.")
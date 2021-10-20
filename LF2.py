import json
import boto3
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key


def poll_sqs_msg(url, message):
    client = boto3.client('sqs')
    response = client.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=message,
        MessageAttributeNames=['All']
    )
    return response


def delete_sqs_msg(url, handler):
    client = boto3.client('sqs')
    response = client.delete_message(
        QueueUrl=url,
        ReceiptHandle=handler
    )
    return response


def get_id(cuisine):
    region = 'us-west-2'
    service = 'es'

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token
    )

    query = {
        "size": 3,
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [{"match": {"categories": cuisine}}]
                    },
                },
                "functions": [
                    {
                        "random_score": {}
                    }
                ]
            }
        }
    }

    headers = {"Content-Type": "application/json"}

    host = 'search-domainyelprest-opbbigm37ekeuhwnf6vezd25ua.us-west-2.es.amazonaws.com'
    index = 'restaurants'
    url = f"https://{host}/{index}/_search"
    res = requests.get(
        url,
        auth=awsauth,
        headers=headers,
        data=json.dumps(query)
    )

    es_response = json.loads(res.text)
    rests_id = [r['_id'] for r in es_response['hits']['hits']]
    return rests_id


def get_restaurants(cuisine):
    rests_id = get_id(cuisine)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurant')

    rest_string = ""
    for i, id in enumerate(rests_id):
        response = table.query(
            KeyConditionExpression=Key('uuid').eq(id)
        )
        item = response['Items'][0]
        rest_name = item['Name']
        rest_address = item['Address']
        rest_string += f"{i+1}. {rest_name}, located at {rest_address} "

    return rest_string


def lambda_handler(event, context):
    queue_url = 'https://sqs.us-west-2.amazonaws.com/483648360079/LF1QUEUE'
    sqs_response = poll_sqs_msg(queue_url, 5)

    if "Messages" not in sqs_response:
        return sqs_response

    message = sqs_response['Messages']

    for sqs_msg in message:
        cuisine = sqs_msg['MessageAttributes']['cuisine']['StringValue']
        phone = "+1" + sqs_msg['MessageAttributes']['phone']['StringValue']
        people = sqs_msg['MessageAttributes']['numPeople']['StringValue']
        time = sqs_msg['MessageAttributes']['DiningTime']['StringValue']
        date = sqs_msg['MessageAttributes']['DiningDate']['StringValue']

        rest = get_restaurants(cuisine)

        sns_message = f"Hello! Here are my {cuisine} restaurant sugestions for {people} people, for {date} at {time}: {rest[:-1]}. Enjoy your meal!"

        sns = boto3.client('sns')
        sns.publish(PhoneNumber = phone, Message = sns_message)
        
        print("success send the message to the phone")

        del_response = delete_sqs_msg(queue_url, sqs_msg['ReceiptHandle'])

    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }
    response['body'] = "LF2 runs successfully"
    return response
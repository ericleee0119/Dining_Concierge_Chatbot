# import the JSON utility package since we will be working with a JSON object
import json
import boto3

client = boto3.client('lexv2-runtime')

# define the handler function that the Lambda service will use an entry point
def lambda_handler(event, context):
# extract values from the event object we got from the Lambda service

    # Get the database and its value
    database = boto3.resource('dynamodb')
    table = database.Table("previousSearch")
    item = table.get_item(Key={'uuid': '0'})
    print("item: ", str(item))
    previousLocation = item['Item']['location']
    previousCuisine = item['Item']['type']
    print("LOG: previous location: ", previousLocation)
    print("LOG: previousCuisine: ", previousCuisine)
    
    print("event LOG: ", str(event))
    print("context LOG: ", str(context))
    lastUserMessage = ""
    #Get the message from lex bot
    if len(event) > 0:
        lastUserMessage = event['messages'][0]['unstructured']['text']
    
    botMessage = "Something went wrong"
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    
    print("LOG: lastmessage: ", str(lastUserMessage))
    if lastUserMessage is None or len(lastUserMessage) < 1:
        print("LOG: lastmessage is none: ", str(botResponse))
        return {
            'statusCode': 200,
            'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST'
            },
            'body': botMessage
        }
    
    #We use the lex2 version
    
    print("LOG check: ", client)
    
    #Get the bot
    response = client.recognize_text(
        botId='TYCSUUCQEU',
        botAliasId='PZUKIYIBGR',
        localeId='en_US',
        sessionId='test_session',
        text=lastUserMessage)
        
    test = client.get_session(
        botId='TYCSUUCQEU',
        botAliasId='PZUKIYIBGR',
        localeId='en_US',
        sessionId='test_session'
        )
        

    print("test LOG: ", str(test))
    print("reponse LOG: ", response)

    if response['messages'][0]['content'] is not None or len(response['messages'][0]['content']) > 0:
        botMessage = response['messages'][0]['content']
    '''if response['sessionState']['dialogAction']['type'] == 'ElicitSlot':
        botMessage = response['requestAttributes']['string']'''
    # If the database contains the previous data, we take it out and pass the data to the next column, then clean the original column
    if(response['sessionState']['intent']['name'] != "ThankYouIntent" and previousLocation is not None and len(previousLocation) > 2 and previousCuisine is not None and len(previousCuisine) > 2):
        botMessage = "Your previous location is at " + previousLocation + ", Your previous cuisine is " + previousCuisine + ", do you want to keep continue?"
        table.put_item(
        Item={
            'uuid': '1',
            'location': previousLocation,
            'type': previousCuisine
        })
        table.put_item(
        Item={
            'uuid': '0',
            'location': '0',
            'type': '0'
        })
        
    print("Bot Message", botMessage)
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST'
        },
        'messages': botResponse
    }

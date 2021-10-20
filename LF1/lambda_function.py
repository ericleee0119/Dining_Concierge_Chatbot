import json
import datetime
import time
import os
import boto3
import dateutil.parser
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def build_validation_result(isvalid, violated_slot, message_content):
    print("LOG: message_content: ", str(message_content))
    print("LOG: violated slot: ", violated_slot)
    print("LOG: isvalid: ", isvalid)
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def isvalid_location(location):
    locations = ['manhattan', 'brooklyn', 'soho', 'uptown', 'flushing']
    return location['value']['originalValue'].lower() in locations

def isvalid_cuisine(cuisine):
    cuisines = ['indian','japanese','italian','chinese','american']
    return cuisine['value']['originalValue'].lower() in cuisines

   
def isvalid_date(date):
    try:
        dateutil.parser.parse(date['value']['originalValue'])
        return True
    except ValueError:
        return False

def isvalid_time(dine_time, dining_date):
    dine_time = dine_time['value']['originalValue']
    dining_date = dining_date['value']['originalValue']
    print("len dine time: ", len(dine_time))
    print("last 2 dine time: ", str(dine_time[-2:].lower()))
    if len(dine_time) < 2 or (str(dine_time[-2:]).lower() != "am" and str(dine_time[-2:]).lower() != "pm"):
        return False
    
    #hour = datetime.datetime.strptime(dine_time, '%H:%M').time().hour
    #minu = datetime.datetime.strptime(dine_time, '%H:%M').time().minute
    
    #print("LOG: hour: ", hour)
    #print("LOG: minute: ", minu)
    
    if datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() == datetime.date.today():
        return False
    
    return True

def isvalid_people(num_people):
    if not num_people:
         return build_validation_result(False,'NumberPeople','')
    num_people = int(num_people['value']['originalValue'])
    if num_people > 50 or num_people < 1:
        return build_validation_result(False,
                                  'NumberPeople',
                                  'Range of 1 to 50 people allowed')
    return build_validation_result(True,'','')

def isvalid_phonenum(phone_num):
    phone_num = phone_num['value']['originalValue']
    if not phone_num:
        return build_validation_result(False, 'PhoneNumber', '')
    if len(phone_num)!= 10 and (phone_num.startswith('+1') is False and len(phone_num) != 12):
        return build_validation_result(False, 'PhoneNumber', 'Phone Number must be in form +1xxxxxxxxxx or a 10 digit number')
    elif len(phone_num) == 10 and (phone_num.startswith('+1') is True):
        return build_validation_result(False, 'PhoneNumber', 'Phone Number must be in form +1xxxxxxxxxx or a 10 digit number')
    return build_validation_result(True,'','')


def validate_reservation(restaurant):
    
    location = try_ex(lambda: restaurant['locationslot'])
    numberpeople = try_ex(lambda: restaurant['peopleslot'])
    cuisine = try_ex(lambda: restaurant['cuisineslot'])
    diningdate = try_ex(lambda: restaurant['dateslot'])
    diningtime = try_ex(lambda: restaurant['timeslot'])
    phonenumber = try_ex(lambda: restaurant['phoneslot'])
    #email = try_ex(lambda: slots['Email'])


    # Determine each input data is correnct or not
    if location and not isvalid_location(location):
        return build_validation_result(
            False,
            'locationslot',
            'We are still expanding our horizon. Sorry for the inconvenience but you might want to try a different city. Current supports soho, manhattan, flushing, brooklyn, uptown')
            
    if cuisine and not isvalid_cuisine(cuisine):
        return build_validation_result(
            False,
            'cuisineslot',
            'Sorry, I don\'t know much about this cuisine. Lets try something else!, current supports chinese, italian, america, japanese, indian')
            
    if numberpeople and not isvalid_people(numberpeople):
        return build_validation_result(
            False,
            'numberpeople',
            'Sorry. Please enter number of people between 1 to 30')
            
    if diningdate:
        if not isvalid_date(diningdate):
            return build_validation_result(False, 'diningdate', 'I did not understand your dining date.  When would you like to dine?')
        if datetime.datetime.strptime(diningdate['value']['originalValue'], '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dateslot', 'Reservations must be scheduled at least one day in advance. Please try a different date?')

    if diningtime and not isvalid_time(diningtime, diningdate):
        print("time break in time")
        return build_validation_result(False, 'timeslot', 'Not a valid time. What time would you like to dine?, time should be look like this 9 am and at least one day in advance')
           
    if phonenumber and not isvalid_phonenum(phonenumber):
        return build_validation_result(False, 'phoneslot', 'Please enter a valid phone number of yours, so that I can notify you about your booking!')

       
    # return True json if nothing is wrong
    return build_validation_result(True,'','')
    
def validate_reservation_2(restaurant):
    
    numberpeople = try_ex(lambda: restaurant['peopleslot'])
    diningdate = try_ex(lambda: restaurant['dateslot'])
    diningtime = try_ex(lambda: restaurant['timeslot'])
    phonenumber = try_ex(lambda: restaurant['phoneslot'])
    #email = try_ex(lambda: slots['Email'])
            
    if numberpeople and not isvalid_people(numberpeople):
        return build_validation_result(
            False,
            'numberpeople',
            'Sorry. Please enter number of people between 1 to 30')
            
    if diningdate:
        if not isvalid_date(diningdate):
            return build_validation_result(False, 'dateslot', 'I did not understand your dining date.  When would you like to dine?')
        if datetime.datetime.strptime(diningdate['value']['originalValue'], '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dateslot', 'Reservations must be scheduled at least one day in advance. Please try a different date?')
            

    if diningtime and not isvalid_time(diningtime, diningdate):
        print("time break in time")
        return build_validation_result(False, 'timeslot', 'Not a valid time. What time would you like to dine?, time should be look like this 9 am and at least one day in advanc')
           
    if phonenumber and not isvalid_phonenum(phonenumber):
      return build_validation_result(False, 'phoneslot', 'Please enter a valid phone number of yours, so that I can notify you about your booking!')

       
    # return True json if nothing is wrong
    return build_validation_result(True,'','')
    

def elicit_slot(intent_name, slots, slot_to_elicit, message):
    #print("Debug: Session attr: ",session_attributes)
    print("message content: ", message['content'])
    if not message['content']:
        return {
             "sessionState": {
                 
                'dialogAction': {
                'type': 'ElicitSlot',
                'intentName': intent_name,
                'slots': slots,
                'slotToElicit': slot_to_elicit
                },
                "intent":{
                "state": "InProgress",
                "name": intent_name,
                "slots": slots
                }
             }
            
        }
    return {
         "messages": [
                {
                    'content': message['content'],
                    'contentType': "PlainText"
                }
             ],
         "sessionState": {
            'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            },
            "intent":{
            "state": "InProgress",
            "name": intent_name,
            "slots": slots
            }
         },
         "requestAttributes":{
            "string": message['content']
         }  
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }

def close(intent_name, message):
    response = {
        "sessionState":{
            'dialogAction': {
            'type': 'Close',
            'fulfillmentState':"Fulfilled",
            'message': message
            },
            "intent":{
            "state": "Fulfilled",
            "name": intent_name
        }
      }
    }
    return response


def delegate(intent_name, slots):
    print("delegate return")
    return {
        "sessionState":{
            'dialogAction': {
            'type': 'Delegate',
            'slots': slots
            },
            "intent":{
            "state": "ReadyForFulfillment",
            "name": intent_name,
            "slots": slots
            
            }
        }  
    }

def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.
    Note that this function would have negative impact on performance.
    """
    try:
        return func()
    except KeyError:
        return None

def restaurantSQSRequest(requestData):
   
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-west-2.amazonaws.com/483648360079/LF1QUEUE'
    delaySeconds = 5
    messageAttributes = {
        'location': {
            'DataType': 'String',
            'StringValue': requestData['Location']
        },
        'cuisine': {
            'DataType': 'String',
            'StringValue': requestData['Cuisine']
        },
        "DiningTime": {
            'DataType': "String",
            'StringValue': requestData['DiningTime']
        },
        "DiningDate": {
            'DataType': "String",
            'StringValue': requestData['DiningDate']
        },
        'numPeople': {
            'DataType': 'Number',
            'StringValue': requestData['NumberPeople']
        },
        'phone': {
            'DataType' : 'String',
            'StringValue' : requestData['PhoneNumber']
        }
    }
    messageBody=('Recommendation for the food')
   
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = delaySeconds,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
        )
    
    print("response", response)
    #print("response messagebody: ", response['MessageAttributes'])
    print ('send data to queue')
   
    return response
    
def put_info(location, foodType):
    database = boto3.resource('dynamodb')
    table = database.Table("previousSearch")
    response = table.put_item(
        Item={
            'uuid': '0',
            'location': location,
            'type': foodType
        }    
    )
    
def PreviousSuggestion(intentRequest):
    
    database = boto3.resource('dynamodb')
    table = database.Table("previousSearch")
    item = table.get_item(Key={'uuid': '1'})
    print()
    location = item['Item']['location']
    cuisine = item['Item']['type']
    print("Log: Location is", location)
    print("Log: Cuisine is ", cuisine)
    #print("LOG: location slot", intentRequest['sessionState']['intent']['slots']['locationslot'])
    #print("LOG: cuisine slot", intentRequest['sessionState']['intent']['slots']['cuisineslot'])
    
    diningdate = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['dateslot'])
    print("Log: Dining date is: ", diningdate)
    timedate = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['timeslot'])
    print("Log: Dining time is: ", timedate)
    numberpeople = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['peopleslot'])
    print("Log: People Num: ", numberpeople)
    phonenumber = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['phoneslot'])
    print("Log: Phone Num: ", phonenumber)
    #intentRequest['sessionState']['intent']['slots']['locationslot']
    if intentRequest['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_reservation_2(intentRequest['sessionState']['intent']['slots'])
        print("LOG: validation_result2: ", str(validation_result))
        if not validation_result['isValid']:
            print("is not valid")
            slots = intentRequest['sessionState']['intent']['slots']
            slots[validation_result['violatedSlot']] = None

            return elicit_slot(
                intentRequest['sessionState']['intent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
        # len(diningdate['value']['originalValue']) < 1 or len(timedate['value']['originalValue']) < 1 or len(numberpeople['value']['originalValue']) < 1 or len(phonenumber['value']['originalValue']) < 1:
        print("LOG: intent request: ", str(intentRequest))
        if diningdate is None or timedate is None or numberpeople is None or phonenumber is None:
            return delegate(intentRequest['sessionState']['intent']['name'], intentRequest['sessionState']['intent']['slots'])
    print("after dialog codebook")
    requestData = {
                    'Location': location,
                    'Cuisine': cuisine,
                    'DiningDate': diningdate['value']['originalValue'],
                    'DiningTime': timedate['value']['originalValue'],
                    'NumberPeople': numberpeople['value']['originalValue'],
                    'PhoneNumber':phonenumber['value']['originalValue']
                  }
                
    messageId = restaurantSQSRequest(requestData)
    print ("Debug: messageId:",messageId)
    
    # Save the data to the DynamoDB
    put_info(location, cuisine)

    return close(
             intentRequest['sessionState']['intent']['name'],
             {'contentType': 'PlainText',
              'content': 'I have received your request and will be sending the suggestions shortly. Have a Great Day !!'})

def DiningSuggestion(intentRequest):
    print("Log, in Dining suggestion function")
    location = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['locationslot'])
    print("Log: Location is  ", location)
    cuisine = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['cuisineslot'])
    print("Log: Cuisine is ", cuisine)
    diningdate = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['dateslot'])
    print("Log: Dining date is: ", diningdate)
    timedate = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['timeslot'])
    print("Log: Dining time is: ", timedate)
    numberpeople = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['peopleslot'])
    print("Log: People Num: ", numberpeople)
    phonenumber = try_ex(lambda: intentRequest['sessionState']['intent']['slots']['phoneslot'])
    print("Log: Phone Num: ", phonenumber)
    
    if intentRequest['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_reservation(intentRequest['sessionState']['intent']['slots'])
        print("LOG: validation_result: ", str(validation_result))
        if not validation_result['isValid']:
            print("is not valid")
            slots = intentRequest['sessionState']['intent']['slots']
            slots[validation_result['violatedSlot']] = None

            return elicit_slot(
                intentRequest['sessionState']['intent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
    

        # Otherwise, let native DM rules determine how to elicit for slots and prompt for confirmation.  Pass price
        # back in sessionAttributes once it can be calculated; otherwise clear any setting from sessionAttributes.
        

        # We still need to wait for fullfill
        return delegate(intentRequest['sessionState']['intent']['name'], intentRequest['sessionState']['intent']['slots'])
    
    # The request data will pass to SQS, and then the lf2 will get this information
    requestData = {
                    'Location': location['value']['originalValue'],
                    'Cuisine': cuisine['value']['originalValue'],
                    'DiningDate': diningdate['value']['originalValue'],
                    'DiningTime': timedate['value']['originalValue'],
                    'NumberPeople': numberpeople['value']['originalValue'],
                    'PhoneNumber':phonenumber['value']['originalValue']
                  }
                
    messageId = restaurantSQSRequest(requestData)
    print ("Debug: messageId:",messageId)
    
    # Save the data to the DynamoDB
    put_info(location['value']['originalValue'], cuisine['value']['originalValue'])

    return close(
             intentRequest['sessionState']['intent']['name'],
             {'contentType': 'PlainText',
              'content': 'I have received your request and will be sending the suggestions shortly. Have a Great Day !!'})

def dispatch(intentRequest):

    print("log in dispatch: ", intentRequest)
    intent_name = intentRequest['sessionState']['intent']['name']
    
    # In the dining suggestion intent, we will get the information from lex bot
    print('LOG: intent name: ', intent_name)
    if intent_name == 'PreviousSearch':
        return PreviousSuggestion(intentRequest)
        
    if intent_name == 'DiningSuggestionsIntent':
        return DiningSuggestion(intentRequest)
    print("Nothing happen")

    raise Exception('Intent with name ' + intent_name + ' not supported')


def lambda_handler(event, context):
    # TODO implement
    print('Receive request: ' + str(event))
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    
    return dispatch(event)

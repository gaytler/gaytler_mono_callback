import os
import json
import requests
import boto3

TELEGRAM_TOKEN = os.environ['gaytler_bot_token']

EXPECTED_ACCOUNT = os.environ['gaytler_bot_expected_account']

USERS_TABLE_NAME = os.environ['gaytler_bot_users_table_name']
TRANSACTIONS_TABLE_NAME = os.environ['gaytler_bot_transactions_table_name']

TARGET_STORAGE = os.environ['gaytler_bot_target_storage']

def send_message(chat_id, text):
    params = {
        "text": text,
        "chat_id": chat_id,
        "parse_mode": "MarkdownV2"
    }
    requests.get(
        "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage",
        params=params
    )
    
def format_raz_by_number(number):
    last_digit = int(number) % 10
    second_last_digit = second_last_digit = (int(number) // 10) % 10

    if (second_last_digit == 1) or (last_digit in (0,5,6,7,8,9)):
        return "%d разів"%number
    
    if last_digit in (2,3,4):
        return "%d рази"%number

    return "%d раз"%number
    
def format_zshk_by_number(number):
    last_digit = int(number) % 10
    second_last_digit = second_last_digit = (int(number) // 10) % 10

    if (second_last_digit == 1) or (last_digit in (0,5,6,7,8,9)):
        return "%d защекоінів"%number
    
    if last_digit in (2,3,4):
        return "%d защекоіна"%number

    return "%d защекоін"%number
    
def process_event(event):
    print(event)
    
    message = json.loads(event['body'])
    account = message['data']['account']

    # check if this is the expected account
    if account == EXPECTED_ACCOUNT:
        client = boto3.resource("dynamodb")
        
        # check if transaction was already processed
        transactions_table = client.Table(TRANSACTIONS_TABLE_NAME)
        
        try:
            transaction_id = message['data']['statementItem']['id']
            transactions_table.put_item(Item={'id': transaction_id}, ConditionExpression='attribute_not_exists(id)')
            
            # get additional data from transaction
            amount = message['data']['statementItem']['amount']/100
            balance = message['data']['statementItem']['balance']/100
    
            send_msg = "⚡ %s насипали за щеку %s\n⚡ За щекою %s 🍆💦😛"%(TARGET_STORAGE, format_raz_by_number(amount), format_zshk_by_number(balance))
        
            # notify all users
            users_table = client.Table(USERS_TABLE_NAME)
            users = users_table.scan()['Items']
    
            for user in users:
                send_message(user['id'], send_msg)
                
        except:
            print("Transaction %s was already processed"%(transaction_id))

def lambda_handler(event, context):
    try:
        process_event(event)
    finally:    
        return {
            'statusCode': 200
        }

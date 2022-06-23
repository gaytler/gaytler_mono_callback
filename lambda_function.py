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
    
            send_msg = "⚡ %s насипали за щеку %d раз\n⚡ За щекою %d защекоіна 🍆💦😛"%(TARGET_STORAGE, amount, balance)
        
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

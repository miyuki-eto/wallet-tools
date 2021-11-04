import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 2500)
pd.set_option("max_colwidth", 100)


def convert_values_decimal(x):
    """
    Convert large values to decimals
    """
    x['value'] = int(x['value']) / (10 ** int(x['tokenDecimal']))
    return x


def last_eth_block(api_key):
    return requests.get("https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey=" + api_key).json()['result']


def get_token_transactions(token_address, api):
    """
    Get all transactions for a given token using Etherscan and return a dataframe
    """
    start_block = 0
    end_block = last_eth_block(api)
    sort = 'asc'
    sleep_time = 0.25
    token_df = pd.DataFrame()
    cont = True
    while cont is True:
        print(start_block)
        url = 'https://api.etherscan.io/api?module=account&action=tokentx&contractaddress=' + token_address + \
              '&startblock=' + str(start_block) + '&endblock=' + str(end_block) + '&sort=' + sort + '&apikey=' + api
        json_data = requests.get(url).json()
        time.sleep(sleep_time)
        result_length = len(json_data['result'])
        if result_length > 0:
            last_block = json_data['result'][len(json_data['result']) - 1]['blockNumber']
            result_data = pd.DataFrame(json_data['result'])
            start_block = int(last_block) + 1
            token_df = result_data if token_df.empty else pd.concat([token_df, result_data], axis=0, sort=False)
        else:
            cont = False
    return token_df


def get_token_holder_data(token_address, api_key, output_filename):
    """
    Sort and rearrange the transaction data to get current holders and create output dataframe
    """
    print('--------------------------------------------------')
    print('Updating token data for {}...'.format(token_address))

    df = get_token_transactions(token_address, api_key)

    df.sort_values(['timeStamp'], inplace=True, ascending=[True])
    df.reset_index(drop=True, inplace=True)

    df['datetime'] = pd.to_datetime(df['timeStamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M')
    df['buydate'] = pd.to_datetime(df['timeStamp'], unit='s').dt.strftime('%Y-%m-%d')
    df['lastdate'] = pd.to_datetime(df['timeStamp'], unit='s').dt.strftime('%Y-%m-%d')

    df = df.apply(lambda x: convert_values_decimal(x), axis=1)

    df_1 = df.copy()
    df_1['address_1'] = df['to']
    df_1['address_2'] = df['from']
    df_1['token_value'] = df['value']
    df_1['sort'] = 2

    df_2 = df.copy()
    df_2['address_1'] = df['from']
    df_2['address_2'] = df['to']
    df_2['token_value'] = 0 - df['value']
    df_2['sort'] = 1

    data = pd.concat([df_1, df_2], axis=0, sort=False)
    data.sort_values(['datetime', 'sort'], inplace=True, ascending=[True, True])
    data.reset_index(drop=True, inplace=True)

    data.to_csv(output_filename)

    print(output_filename + ' updated')
    print('--------------------------------------------------')
    print('')

#
# # API keys
# etherscan_api_key = os.getenv('ETHERSCAN_KEY')
#
# floki = "0x43f11c02439e2736800433b4594994Bd43Cd066D"
# leash = "0x27C70Cd1946795B66be9d954418546998b546634"
# akitainu = "0x3301ee63fb29f863f2333bd4466acb46cd8323e6"
#
# get_token_holder_data(floki, etherscan_api_key)

import datetime
import pandas as pd
import json
import os
from dotenv import load_dotenv
from token_data import get_token_holder_data

load_dotenv()

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 2500)
pd.set_option("max_colwidth", 100)

etherscan_api_key = os.getenv('ETHERSCAN_KEY')


def create_directory(path):
    os.makedirs(path, exist_ok=True)


def file_exists(path):
    return os.path.isfile(path)


def check_data(token_dictionary, force_update):
    print('checking data...')
    create_directory('data')
    for k, v in token_dictionary.items():
        filename = 'data/{}_token_holders.csv'.format(k)
        if file_exists(filename):
            data = pd.read_csv(filename)
            last_timestamp = data['timeStamp'].tail(1).reset_index()['timeStamp'][0]
            end_time = int(datetime.datetime.strptime(v['end'], "%Y-%m-%d %H:%M").timestamp())
            if end_time > last_timestamp:
                get_token_holder_data(v['address'], etherscan_api_key, filename)
        else:
            get_token_holder_data(v['address'], etherscan_api_key, filename)
        if force_update:
            get_token_holder_data(v['address'], etherscan_api_key, filename)


def analyse_wallets(_dict, ignore_min):
    print('analysing wallets...')
    df = pd.DataFrame()
    for k, v in _dict.items():
        print(k, v)
        token_address = v['address']
        start_time = int(datetime.datetime.strptime(v['start'], "%Y-%m-%d %H:%M").timestamp())
        end_time = int(datetime.datetime.strptime(v['end'], "%Y-%m-%d %H:%M").timestamp())
        token_min = v['min']

        data = pd.read_csv('data/{}_token_holders.csv'.format(k))
        data['timeStamp'] = data['timeStamp'].astype(int)
        data = data[data['timeStamp'] >= start_time]
        data = data[data['timeStamp'] <= end_time]
        data['tokenBalance'] = data.groupby(['address_1'])['token_value'].apply(lambda x: x.cumsum())
        data = data.groupby(['address_1']).tail(1).reset_index()
        if not ignore_min:
            data = data[data['tokenBalance'] >= token_min]
        data = data[data['address_1'] != '0x000000000000000000000000000000000000dead']
        data.sort_values(['tokenBalance'], inplace=True, ascending=[False])
        data.reset_index(drop=True, inplace=True)
        cols = ['tokenSymbol', 'address_1', 'tokenBalance']
        data = data[cols]
        # print(data.head(25))
        df = data if df.empty else pd.concat([df, data], axis=0, sort=False)
    df = df.value_counts(subset=['address_1'])
    print(df)
    # print(df.columns)


with open('token_dict.json') as json_file:
    token_dict = json.load(json_file)

check_data(token_dict, force_update=False)
analyse_wallets(token_dict, ignore_min=True)



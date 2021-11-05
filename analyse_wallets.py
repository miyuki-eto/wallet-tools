import datetime
import pandas as pd
import json
import os
from dotenv import load_dotenv
from token_data import get_token_holder_data
from estimate_timestamp import estimate_block_height_by_timestamp
from pprint import pprint

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
                get_token_holder_data(v['address'], etherscan_api_key, filename, v['start_block'], v['end_block'])
        else:
            get_token_holder_data(v['address'], etherscan_api_key, filename, v['start_block'], v['end_block'])
        if force_update:
            get_token_holder_data(v['address'], etherscan_api_key, filename, v['start_block'], v['end_block'])


def analyse_wallets(_dict, ignore_min):
    print('analysing wallets...')
    df = pd.DataFrame()
    tokens = []
    for k, v in _dict.items():
        print(k, v)
        tokens.append(k)
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
        else:
            data = data[data['tokenBalance'] > 0]
        data = data[data['address_1'] != '0x000000000000000000000000000000000000dead']
        data.sort_values(['tokenBalance'], inplace=True, ascending=[False])
        data.reset_index(drop=True, inplace=True)
        cols = ['address_1', 'tokenBalance']
        data = data[cols]
        data.columns = ['address', k]
        # print(data.head(25))
        df = data if df.empty else pd.concat([df, data], axis=0, sort=False)
    # print(df.head(10))
    for token in tokens:
        df[token] = df.groupby(['address'])[token].transform('sum')
    df['count'] = df.groupby(['address'])['address'].transform('count')
    df.sort_values(['count'], inplace=True, ascending=[False])
    df = df[df['count'] > 1]
    print(df)


def check_block_timestamps(token_dictionary):
    print('checking block timestamps...')
    for k, v in token_dictionary.items():
        if v['start_block'] == "":
            start = estimate_block_height_by_timestamp(v['start']) - 10
            end = estimate_block_height_by_timestamp(v['end']) + 10
            token_dictionary[k]['start_block'] = start
            token_dictionary[k]['end_block'] = end
    with open('token_dict.json', 'w') as fp:
        json.dump(token_dictionary, fp)
    return token_dictionary


with open('token_dict.json') as json_file:
    token_dict = json.load(json_file)

token_dict = check_block_timestamps(token_dict)
check_data(token_dict, force_update=False)
analyse_wallets(token_dict, ignore_min=True)



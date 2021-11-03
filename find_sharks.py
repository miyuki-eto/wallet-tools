import datetime
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 2500)
pd.set_option("max_colwidth", 100)


etherscan_api_key = os.getenv('ETHERSCAN_KEY')
floki = "0x43f11c02439e2736800433b4594994Bd43Cd066D"
leash = "0x27C70Cd1946795B66be9d954418546998b546634"
akitainu = "0x3301ee63fb29f863f2333bd4466acb46cd8323e6"

token_dict = {
    'floki': {
        'address': '0x43f11c02439e2736800433b4594994Bd43Cd066D',
        'start': '2021-09-23 00:00',
        'end': '2021-09-28 03:00',
        'min': 1000000000
    },
    'leash': {
        'address': '0x27C70Cd1946795B66be9d954418546998b546634',
        'start': '2021-09-15 12:00',
        'end': '2021-10-05 08:00',
        'min': 100
    },
    'akitainu': {
        'address': '0x3301ee63fb29f863f2333bd4466acb46cd8323e6',
        'start': '2021-09-28 00:00',
        'end': '2021-10-05 01:00',
        'min': 1000000000
    },
}


def analyse_wallets(_dict):
    df = pd.DataFrame()
    for k, v in _dict.items():
        print(k)
        print(v)
        token_address = v['address']
        start_time = int(datetime.datetime.strptime(v['start'], "%Y-%m-%d %H:%M").timestamp())
        end_time = int(datetime.datetime.strptime(v['end'], "%Y-%m-%d %H:%M").timestamp())
        token_min = v['min']

        data = pd.read_csv('data/{}_token_holders.csv'.format(token_address))
        data['timeStamp'] = data['timeStamp'].astype(int)
        data = data[data['timeStamp'] >= start_time]
        data = data[data['timeStamp'] <= end_time]
        data['tokenBalance'] = data.groupby(['address_1'])['token_value'].apply(lambda x: x.cumsum())
        data = data.groupby(['address_1']).tail(1).reset_index()
        data = data[data['tokenBalance'] >= token_min]
        data = data[data['address_1'] != '0x000000000000000000000000000000000000dead']
        data.sort_values(['tokenBalance'], inplace=True, ascending=[False])
        data.reset_index(drop=True, inplace=True)
        cols = ['tokenSymbol', 'address_1', 'tokenBalance']
        data = data[cols]
        print(data.head(25))
        df = data if df.empty else pd.concat([df, data], axis=0, sort=False)
    df = df.value_counts(subset=['address_1'])
    print(df)
    print(df.columns)


analyse_wallets(token_dict)



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

start_time = int(datetime.datetime.strptime("2021-10-27 08:15", "%Y-%m-%d %H:%M").timestamp())
end_time = int(datetime.datetime.strptime("2021-10-27 18:30", "%Y-%m-%d %H:%M").timestamp())

token_min = 1000000000

df = pd.read_csv('data/FLOKI_token_holders.csv')
df['timeStamp'] = df['timeStamp'].astype(int)

# df = df.apply(lambda x: convert_values_decimal(x), axis=1)

df = df[df['timeStamp'] >= start_time]
df = df[df['timeStamp'] <= end_time]

df['tokenBalance'] = df.groupby(['address_1'])['token_value'].apply(lambda x: x.cumsum())

df = df.groupby(['address_1']).tail(1).reset_index()

df = df[df['tokenBalance'] >= token_min]

df = df[df['address_1'] != '0x000000000000000000000000000000000000dead']


df.sort_values(['tokenBalance'], inplace=True, ascending=[False])
df.reset_index(drop=True, inplace=True)

cols = ['tokenSymbol', 'address_1', 'tokenBalance']

print(df[cols])

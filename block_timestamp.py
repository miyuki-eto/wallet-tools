from web3 import Web3
import os
from dotenv import load_dotenv
import json

load_dotenv()

web3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_URL')))

latest_block = web3.eth.get_block('latest').number
timestamp_dict = {}

while latest_block > 1:
    timestamp_dict[latest_block] = web3.eth.get_block(latest_block).timestamp
    latest_block -= 1


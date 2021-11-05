from web3 import Web3
import os
from dotenv import load_dotenv
import json
import datetime
import math


load_dotenv()

web3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_URL')))

# a = 1484362538 # 01/14/2017 @ 2:55am (UTC)
a = "2021-09-28 15:00"


def estimate_block_height_by_timestamp(timestamp):
    timestamp = int(datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M").timestamp())
    block_found = False
    # last_block_number = web3.eth.get_block('latest').number
    latest_block = web3.eth.get_block('latest')
    close_in_seconds = 15

    while not block_found:
        block_number = latest_block.number
        block_timestamp = latest_block.timestamp

        diff = block_timestamp - timestamp
        diff_est = math.ceil(diff / close_in_seconds)

        block_est = block_number - diff_est

        test_block = web3.eth.get_block(block_est)
        time_est = test_block.timestamp
        time_diff = timestamp - time_est

        # print('block number    - {}'.format(block_number))
        # print('timestamp       - {}'.format(timestamp))
        # print('block timestamp - {}'.format(block_timestamp))
        # print('diff            - {}'.format(diff))
        # print('block diff est  - {}'.format(diff_est))
        # print('block est       - {}'.format(block_est))
        # print('time est        - {}'.format(time_est))
        # print('timestamp       - {}'.format(timestamp))
        #
        # print('time diff       - {}'.format(time_diff))
        #
        # print('----------------')

        if -100 <= time_diff <= 100:
            # print(block_est)
            # print('---------------------------------------')

            return block_est

        latest_block = test_block





    # while not block_found:
    #     print(last_block_number)
    #     block = web3.eth.getBlock(last_block_number)

        #
        # block_time = datetime.datetime.fromtimestamp(block.timestamp)
        # print(timestamp)
        # print(block_time)
        # difference_in_seconds = int((timestamp - block_time).total_seconds())
        #
        # block_found = abs(difference_in_seconds) < close_in_seconds
        #
        # if block_found:
        #     return last_block_number
        #
        # if difference_in_seconds < 0:
        #     last_block_number //= 2
        # else:
        #     last_block_number = int(last_block_number * 1.5) + 1


# block = estimate_block_height_by_timestamp(a)
#
# print(block)

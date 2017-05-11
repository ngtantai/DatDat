#!/usr/bin/env python3

from __future__ import print_function

import sys
import threading
import boto3
import json

from satori.rtm.client import make_client, SubscriptionMode
from satori.rtm.connection import Connection

channel = "transportation"
endpoint = "wss://open-data.api.satori.com"
appkey = "e41658E9b440EA66DAeA9dE21848C35f"
client1 = boto3.client('firehose', region_name = 'us-west-2')


def main():
    with make_client(endpoint=endpoint, appkey=appkey) as client:

        print('Connected!')

        mailbox = []
        got_message_event = threading.Event()

        class SubscriptionObserver(object):
            def on_subscription_data(self, data):
                for message in data['messages']:
                    mailbox.append(message)
                    #response = client1.put_record(DeliveryStreamName='traffic_stream',Record={'Data': json.dumps(message) + '\n'})
                got_message_event.set()

        subscription_observer = SubscriptionObserver()
        client.subscribe(channel,SubscriptionMode.SIMPLE,subscription_observer)

        if not got_message_event.wait(30):
            print("Timeout while waiting for a message")
            sys.exit(1)

        for message in mailbox:
            response = client1.put_record(DeliveryStreamName='traffic_stream',Record={'Data': json.dumps(message) + '\n'})
            print(message)

#for message in mailbox:
    #print(response)
if __name__ == '__main__':
    while True:
        main()

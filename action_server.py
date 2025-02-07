import datetime

from flask import Flask, render_template, url_for, request, redirect
from kafka import KafkaProducer, KafkaConsumer
import os
import time
import json
import pymongo
import threading
from json import loads, dumps

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

def handle_dict(rec_dict):
    pass

def consume_message(topic):
    global producer
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: loads(x.decode('utf-8')))

    for msg in consumer:
        print(msg.value)
        rec_dict = msg.value

        if rec_dict["op_type"] == "send":
            # Sending the message to uid2 with topic as his username.
            producer.send(rec_dict["uid2"], json.dumps(rec_dict).encode('utf-8'))
            handle_dict(rec_dict)


def main():
    # Logic for action server
    topic = "ActionServer"
    t1 = threading.Thread(target=consume_message, args=(topic,))
    t1.start()
    t1.join()
    print("Done")


if __name__ == "__main__":
    main()
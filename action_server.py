import json
import threading
from json import loads

import pymongo
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

# Storing message in DB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["GlobalDB"]


def update_db(rec_dict, collection_name):
    """
    Method to save the message in DB.
    There are two collection(table) to store messages(rows)
    1. user1_and_user2 (There is no user2_and_user1)
    2. group 1

    :param rec_dict:
    :param collection_name: user1_and_user2
    :return:
    """
    mycol = mydb[collection_name]
    dict_msg = {
        "msg_id": rec_dict['msg_id'],
        "send_uid": rec_dict['uid1'],
        "timestamp": rec_dict['timestamp'],
        "text": rec_dict['text']
    }

    mycol.insert_one(dict_msg)


def isGroup(name):
    """
    Return if the message is from a group.
    :param name:
    :return:
    """
    return name.startswith("group")


def get_group_info():
    """
    Loads and returns group_mapping.txt file in appropriate structure.
    File content:
    group1-user1-user2
    group2-user1-user3
    group3-user1-user2-user3
    group4-user2-user3
    :return:
    """
    file = open("group_mapping.txt", "r")
    data = file.readlines()
    dict_groups = {}
    for line in data:
        tokens = line.strip().split("-")
        dict_groups[tokens[0]] = []
        for i in range(1, len(tokens)):
            dict_groups[tokens[0]].append(tokens[i])

    print(dict_groups)
    return dict_groups


def handle_send(rec_dict):
    """
    Method to handle Send Request from Flask Server.
    Two things here.
    1. Emit message back to over the topic = receiver of the message over Kafka.
    2. Save the message to DB.
    :param rec_dict:
    :return:
    """
    uid1 = rec_dict["uid1"]  # Sender
    uid2 = rec_dict["uid2"]  # Receiver

    if isGroup(uid2):
        collection_name = uid2
        group_info = get_group_info()
        if uid2 in group_info:
            for member in group_info[uid2]:
                # Send the message to all the group members.
                rec_dict["op_type"] = "grp_send"
                producer.send(member, json.dumps(rec_dict).encode('utf-8'))

    else:
        temp_list = [uid1, uid2]
        temp_list.sort()
        collection_name = str(temp_list[0]) + "_and_" + str(temp_list[1])
        # Sending the message to uid2 with topic as his username.
        producer.send(rec_dict["uid2"], json.dumps(rec_dict).encode('utf-8'))

    update_db(rec_dict, collection_name)

def getMessages(collection_name):
    """
    Returns all the messages in collection = collection_name (user1_and_user2)
    :param collection_name:
    :return:
    """
    mycol = mydb[collection_name]
    temp = mycol.find()
    messages = []
    for x in temp:
        msg = {
            "msg_id" : x["msg_id"],
            "send_uid" : x["send_uid"],
            "text" : x["text"],
            "timestamp" : x["timestamp"]
        }
        messages.append(msg)
        print(msg)

    return messages

def handle_fetch_msgs(rec_dict):
    """
    Method to handle Fetch Messages request from flask server.
    Fetches all the requested message and send it back to Flask over the topic = user who requested it.
    :param rec_dict: Request info.
    :return:
    """
    uid1 = rec_dict["uid1"]
    uid2 = rec_dict["uid2"]
    # We need collection name where the message is stored first.

    if isGroup(uid2):
        collection_name = uid2
    else:
        temp_list = [uid1, uid2]
        temp_list.sort()
        collection_name = str(temp_list[0]) + "_and_" + str(temp_list[1])

    messages = getMessages(collection_name)

    dict_msg = {
        "op_type" : "fetch_msgs",
        "uid1" : uid1,
        "uid2" : uid2,
        "messages" : messages
    }

    # Send all the messages to user 1 back, the logged-in user.
    producer.send(rec_dict['uid1'], json.dumps(dict_msg).encode('utf-8'))

def handle_update_msg(rec_dict):
    """
    Method to handle Update Messages request from flask server.
    Updates the DB and sends message back to flask server over the topic = receiver
    :param rec_dict:
    :return:
    """
    uid1 = rec_dict["uid1"]
    uid2 = rec_dict["uid2"]
    msg_id = rec_dict['msg_id']
    text = rec_dict['text']
    collection_name = None
    if isGroup(uid2):
        collection_name = uid2
        group_info = get_group_info()
        if uid2 in group_info:
            for member in group_info[uid2]:
                rec_dict["op_type"] = "grp_update"
                producer.send(member, json.dumps(rec_dict).encode('utf-8'))

    else:
        temp_list = [uid1, uid2]
        temp_list.sort()
        collection_name = str(temp_list[0]) + "_and_" + str(temp_list[1])
        producer.send(rec_dict['uid2'], json.dumps(rec_dict).encode('utf-8'))

    timestamp = rec_dict['timestamp']
    mycol = mydb[collection_name]
    myquery = { "msg_id": msg_id }
    mydoc = mycol.find(myquery)
    newvalues = { "$set": { "text": text } }
    mycol.update_one(myquery, newvalues)
    newvalues = { "$set": { "timestamp": timestamp } }
    mycol.update_one(myquery, newvalues)


def handle_delete_msg(rec_dict):
    """
    Method to handle Delete Messages request from flask server.
    Deletes from the DB and sends message back to flask server over the topic = receiver
    :param rec_dict:
    :return:
    """
    uid1 = rec_dict["uid1"]
    uid2 = rec_dict["uid2"]
    msg_id = rec_dict['msg_id']

    collection_name = None
    if isGroup(uid2):
        collection_name = uid2
        group_info = get_group_info()
        if uid2 in group_info:
            for member in group_info[uid2]:
                rec_dict["op_type"] = "grp_delete"
                producer.send(member, json.dumps(rec_dict).encode('utf-8'))

    else:
        temp_list = [uid1, uid2]
        temp_list.sort()
        collection_name = str(temp_list[0]) + "_and_" + str(temp_list[1])
        producer.send(rec_dict['uid2'], json.dumps(rec_dict).encode('utf-8'))

    mycol = mydb[collection_name]
    myquery = { "msg_id": msg_id }
    mydoc = mycol.find(myquery)
    for x in mydoc:
        mycol.delete_one(x)

def consume_message(topic):
    """
    Method to consume and store messages received from Flask Server via Kafka over topic 'ActionServer'.
    Flask server receives the request from UI and sends the request information to Action Server in some defined format
    to store in DB and then action server sends back another Kafka message to user2 (receiver) over the topic (receiver's id = uid2)
    :param topic:
    :return:
    """
    global producer
    consumer = KafkaConsumer(topic,  # ActionServer
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: loads(x.decode('utf-8')))

    for msg in consumer:
        print(f"Message from Flask Server to Action Server -> {msg.value}")
        rec_dict = msg.value

        if rec_dict["op_type"] == "send":
            handle_send(rec_dict)

        elif rec_dict["op_type"] == "fetch_msgs":
            handle_fetch_msgs(rec_dict)

        elif rec_dict["op_type"] == "update_msg":
            handle_update_msg(rec_dict)

        elif rec_dict["op_type"] == "delete_msg":
            handle_delete_msg(rec_dict)


def main():
    # Logic for action server
    topic = "ActionServer"
    print("Started Action Sever")
    t1 = threading.Thread(target=consume_message, args=(topic,))
    t1.start()
    t1.join()
    print("Done")


if __name__ == "__main__":
    main()
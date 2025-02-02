# from Tools.scripts.make_ctype import method
import datetime

from flask import Flask, render_template, url_for, request, redirect
from kafka import KafkaProducer, KafkaConsumer
import os
import time
import json
import pymongo

app = Flask(__name__)
app.secret_key = 'any_random_string'

# Database Connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
user_db = myclient["authentication"]  # authentication is db, user_info is the table.
user_table = user_db["user_info"]

# Some global variables
users_data = {}  # This is the only structure that will have everything
msg_count = 0

@app.route("/")
@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/register", methods=['GET','POST'])
def register():
    return render_template("register.html")

@app.route("/register_check", methods=['GET','POST'])
def register_check():
    global users_data
    if request.method == 'POST':
        req = dict(request.form)
        print(req)
        query = user_table.find({'uid': req['uid']})
        found = False
        for q in query:
            if q['uid'] == req['uid']:
                found = True
                break
        req_dict = {
            "uid": req["uid"],
            "email": req["email"],
            "password": req["password"]
        }
        if not found:
            temp = user_table.insert_one(req_dict)
            uid = req["uid"]
            users_data[uid] = {}
            # See message Structure in notes file.
            users_data[uid]["cid"] = None  # Current user with him uid is talking to.
            users_data[uid]["user_list"] = []
            users_data[uid]["group_list"] = []
            users_data[uid]["msg_list"] = {}

            return redirect("/dashboard/" + str(uid))
        else:
            return render_template("invalid.html", message = "User already registered.")
    return render_template("register.html")

@app.route("/login", methods=['GET','POST'])
def login():
    return render_template("login.html")

@app.route("/login_check", methods=['GET','POST'])
def login_check():
    global users_data
    if request.method == 'POST':
        req = dict(request.form)
        print(req)
        query = user_table.find({'uid': req['uid']})
        found = False
        user = None
        for q in query:
            if q['uid'] == req['uid']:
                found = True
                user = q
                break
        if not found:
            return render_template("invalid.html", message="User not registered.")
        else:
            if user["password"] == req["password"]:
                uid = req["uid"]  # current user
                users_data[uid] = {}
                users_data[uid]["cid"] = None  # Current user with him uid is talking to.
                users_data[uid]["user_list"] = []
                users_data[uid]["group_list"] = []
                users_data[uid]["msg_list"] = {}

                return redirect("/dashboard/" + str(uid))
            else:
                return render_template("invalid.html", message="Incorrect password")

    return render_template("login.html")

@app.route("/fetch_user/<string:user_id>", methods=['GET', 'POST'])
def fetch_user(user_id):
    global users_data
    file = open("users.txt", "r")
    data = file.readlines()
    users_data[user_id]["user_list"] = data
    return redirect("/dashboard/" + str(user_id))

@app.route("/fetch_group/<string:user_id>", methods=['GET', 'POST'])
def fetch_group(user_id):
    global users_data
    file = open("groups.txt", "r")
    data = file.readlines()
    users_data[user_id]["group_list"] = data
    return redirect("/dashboard/" + str(user_id))


@app.route("/update_cid/<string:user_id>/<string:chat_id>", methods=['GET', 'POST'])
def update_cid(user_id, chat_id):
    global users_data
    users_data[user_id]["cid"] = chat_id
    return redirect("/dashboard/" + str(user_id))

@app.route("/dashboard/<string:user_id>", methods=['GET','POST'])
def dashboard(user_id):
    global users_data

    if user_id in users_data:
        return render_template("dashboard.html",
                               uid=user_id,
                               cid=users_data[user_id]['cid'],
                               user_list=users_data[user_id]['user_list'],
                               group_list=users_data[user_id]['group_list'],
                               msg_list=users_data[user_id]['msg_list'])
    else:
        redirect("/home")

@app.route("/send_msg/<string:user_id>", methods=['GET', 'POST'])
def send_msg(user_id):
    global users_data, msg_count
    if request.method == 'POST':
        req = request.form
        req = dict(req)
        print(req)
        text = req['typed_msg']
        chat_id = users_data[user_id]['cid']  # To whom message is send
        if chat_id is not None:
            chat_id = chat_id.strip()
        msg_count +=1
        # file = open("msg_id.txt", "w")
        # file.write(str(msg_count))
        # file.close()
        msg_id = str(msg_count)
        timestamp = str(datetime.datetime.now())

        # dict_msg = {
        #     "op_type":"send",
        #     "uid1":user_id,
        #     "uid2":chat_id,
        #     "text":text,
        #     "timestamp":timestamp,
        #     "msg_id":msg_id
        # }
        #
        # topic = "ActionServer"
        # producer.send(topic, json.dumps(dict_msg).encode('utf-8'))

        if chat_id in users_data[user_id]['msg_list']:
            # User has already talked to this user
            users_data[user_id]['msg_list'][chat_id][msg_id] = {}

            users_data[user_id]['msg_list'][chat_id][msg_id]['text'] = text
            users_data[user_id]['msg_list'][chat_id][msg_id]['send_uid'] = user_id
            users_data[user_id]['msg_list'][chat_id][msg_id]['timestamp'] = timestamp

        else:
            users_data[user_id]['msg_list'][chat_id] = {}

            users_data[user_id]['msg_list'][chat_id][msg_id] = {}

            users_data[user_id]['msg_list'][chat_id][msg_id]['text'] = text
            users_data[user_id]['msg_list'][chat_id][msg_id]['send_uid'] = user_id
            users_data[user_id]['msg_list'][chat_id][msg_id]['timestamp'] = timestamp

    return redirect('/dashboard/'+str(user_id))


@app.route("/logout/<string:user_id>", methods=['GET', 'POST'])
def logout(user_id):
    global users_data
    print("logout ", user_id)
    users_data.pop(user_id)
    return redirect('/home')
# ip: localhost (127.0.0.1) and port 5000
if __name__ == "__main__":
    app.run(debug=True, threaded=True)
    # threaded=True represents that a new request is processed by separate thread

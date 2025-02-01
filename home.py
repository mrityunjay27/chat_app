from Tools.scripts.make_ctype import method
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
current_user = None
user_list = []
group_list = []
cid = None

@app.route("/")
@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/register", methods=['GET','POST'])
def register():
    return render_template("register.html")

@app.route("/register_check", methods=['GET','POST'])
def register_check():
    global current_user
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
            current_user = req["uid"]
            return redirect("dashboard")
        else:
            return render_template("invalid.html", message = "User already registered.")
    return render_template("register.html")

@app.route("/login", methods=['GET','POST'])
def login():
    return render_template("login.html")

@app.route("/login_check", methods=['GET','POST'])
def login_check():
    global current_user
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
                current_user = req["uid"]
                return redirect("dashboard")
            else:
                return render_template("invalid.html", message="Incorrect password")

    return render_template("login.html")

@app.route("/fetch_user", methods=['GET', 'POST'])
def fetch_user():
    global user_list
    file = open("users.txt", "r")
    data = file.readlines()
    user_list = data
    return redirect('dashboard')

@app.route("/fetch_group", methods=['GET', 'POST'])
def fetch_group():
    global group_list
    file = open("groups.txt", "r")
    data = file.readlines()
    group_list = data
    return redirect('dashboard')


@app.route("/update_cid/<string:chat_id>", methods=['GET', 'POST'])
def update_cid(chat_id):
    global cid
    cid = chat_id
    return redirect('/dashboard')

@app.route("/dashboard", methods=['GET','POST'])
def dashboard():
    global current_user, user_list, group_list, cid
    return render_template("dashboard.html", uid=current_user, user_list=user_list, group_list=group_list, cid=cid)

# ip: localhost (127.0.0.1) and port 5000
if __name__ == "__main__":
    app.run(debug=True, threaded=True)
    # threaded=True represents that a new request is processed by separate thread

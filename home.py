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

@app.route("/")
@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/register", methods=['GET','POST'])
def register():
    return render_template("register.html")

@app.route("/register_check", methods=['GET','POST'])
def register_check():
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
            return render_template("dashboard.html", uid = req["uid"])
        else:
            return render_template("invalid.html", message = "User already registered.")
    return render_template("register.html")

@app.route("/login", methods=['GET','POST'])
def login():
    return render_template("login.html")

@app.route("/login_check", methods=['GET','POST'])
def login_check():
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
                return render_template("dashboard.html", uid=req["uid"])
            else:
                return render_template("invalid.html", message="Incorrect password")

    return render_template("login.html")

@app.route("/dashboard", methods=['GET','POST'])
def dashboard():
    return render_template("dashboard.html")

# ip: localhost (127.0.0.1) and port 5000
if __name__ == "__main__":
    app.run(debug=True, threaded=True)
    # threaded=True represents that a new request is processed by separate thread

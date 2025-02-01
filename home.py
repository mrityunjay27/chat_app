from flask import Flask, render_template, url_for, request, redirect
from kafka import KafkaProducer, KafkaConsumer
import os
import time
import json
import pymongo

app = Flask(__name__)
app.secret_key = 'any_random_string'

@app.route("/")
@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/register")
def register():
    return render_template("register.html")

@app.route("/login")
def login():
    return render_template("login.html")

# ip: localhost (127.0.0.1) and port 5000
if __name__ == "__main__":
    app.run(debug=True, threaded=True)  # threaded=True represents that a new request is processed by seperate thread
import json
import time
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    time_stamp = time.time()
    purchase_sword_event = {'event_type': 'purchase_sword', 'event_ts': time_stamp}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/purchase_a_shield")
def purchase_a_shield():
    time_stamp = time.time()
    purchase_shield_event = {'event_type': 'purchase_shield', 'event_ts': time_stamp}
    log_to_kafka('events', purchase_shield_event)
    return "\nShield Purchased!\n"

@app.route("/purchase_a_spear")
def purchase_a_spear():
    time_stamp = time.time()
    purchase_spear_event = {'event_type': 'purchase_spear', 'event_ts': time_stamp}
    log_to_kafka('events', purchase_spear_event)
    return "\nSpear Purchased!\n"

@app.route("/purchase_a_potion")
def purchase_a_potion():
    time_stamp = time.time()
    purchase_potion_event = {'event_type': 'purchase_potion', 'event_ts': time_stamp, 'description': 'heals you for 50 hp'}
    log_to_kafka('events', purchase_potion_event)
    return "\nPotion Purchased!\n"




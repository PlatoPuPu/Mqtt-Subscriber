# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 10:57:05 2022

@author: Martin Luo
"""
import random
import paho.mqtt.client as mqtt
import mqtt_gw.data_processor as zwei
import mqtt_gw.iotdb_insert as drei
import json

target_data = ''

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with GW-EMS, result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("controller/push/real")  # Subscribe to the topic “digitest/test1”, receive any messages published on it

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("New Message received under topic-> " + msg.topic)  # Print a received msg

    global target_data

    # cut the data part
    raw_data = str(msg.payload)[2:-1]
    # form the str to dict use json
    dict_data = json.loads(raw_data)
    
    # call the processor
    target_data = zwei.process(dict_data)


    # insert data to iotDB. COMMENTED for homeoffice
    drei.insert_data(target_data)

def core():
    
    client_id = f'QACN-{random.randint(0, 100)}'
    client = mqtt.Client(client_id)  # Create instance of client with client ID “digi_mqtt_test”
    client.on_connect = on_connect  # Define callback function for successful connection
    client.on_message = on_message  # Define callback function for receipt of a message
    client.connect("free.svipss.top", 55107, 60)  # Connect to (broker, port, keepalive-time)
    
    client.loop_forever()  # Start networking daemon

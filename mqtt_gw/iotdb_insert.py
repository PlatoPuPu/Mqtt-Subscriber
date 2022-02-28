# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 10:57:05 2022

@author: Martin Luo
"""

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
# from iotdb.utils.Tablet import Tablet

# creating session connection.
ip = "192.168.2.132"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)


def insert_data(target_data):
    # given values
    print("Start Data Inserting")
    measurements_ = target_data["measurements"]
    values_ = target_data["values"]
    timestampe_ = target_data["timestampe"]
    device_ = target_data["device"]
    data_types_ = [TSDataType.FLOAT for i in range(len(values_))] # lenth of data_types must be the same as lenth of values!
    # insert one record into the database.
    session.insert_record("root.martinluo", timestampe_, measurements_, data_types_, values_)
    print("End Data Inserting at"+" "+target_data["Zeitpunkt"])
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 16:58:19 2022

@author: luoyi
"""

from datetime import datetime
import pandas as pd

# Get data from MQTT


def process (raw_data):
    
    print("Start Data Processing")
    
    # process raw data
    data = pd.DataFrame(raw_data['data'])
    x = data['id'].astype('string')
    y = data['value'].astype('float')
    
    measurements = pd.Series.to_list(x)
    values = pd.Series.to_list(y)
    device = "root.qa.gw."+raw_data['sn']
    Zeitpunkt = str(datetime.strptime(raw_data['date'],'%Y-%m-%d %H:%M:%S'))
    time = int(datetime.strptime(raw_data['date'],'%Y-%m-%d %H:%M:%S').timestamp())*1000
    
    # combine processed data
    target_value = {"device":device,"timestampe":time,"measurements":measurements, "values":values, "Zeitpunkt":Zeitpunkt}
    print("End Data Processing at"+" "+Zeitpunkt)

    return target_value


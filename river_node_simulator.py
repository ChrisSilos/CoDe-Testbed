# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 17:41:00 2020

@author: StevensUser
"""

import json
import requests
import pandas as pd
import datetime
import time as t
from datetime import timedelta
import ntplib
import paho.mqtt.client as mqtt

response = ntplib.NTPClient().request('time.nist.gov', version=3)
global offset
offset = timedelta(seconds = response.offset)

global trigger
trigger = 0

def retreive_data(start_date, end_date):
    global data
    global interp_water_level
    global interp_flow_rate
    print(">>> Retrieving streamgage data")
    page = requests.get(
            "https://waterservices.usgs.gov/nwis/iv/?format=json&sites="
            + '04156000' 
            + "&startDT=" + start_date[:10]  
            + "&endDT=" + end_date[:10]  
            + "&parameterCd=00060,00065"
            + "&siteStatus=all")
    data = json.loads(page.text)
   
    
    #flowrate
    flow_rate_data = data['value']['timeSeries'][0]['values'][0]['value']
    
    #water level
    water_level_data = data['value']['timeSeries'][1]['values'][0]['value']
    
    
    #convert each date to more readable format
    for i in range(len(flow_rate_data)):
        flow_rate_data[i]['dateTime'] = niceDate(flow_rate_data[i]['dateTime'])
        water_level_data[i]['dateTime'] = niceDate(water_level_data[i]['dateTime'])
        
    #create dataframes with flow_rate_data and water_level_data
    flow_rate_df = pd.DataFrame(flow_rate_data)
    water_level_df = pd.DataFrame(water_level_data)
    
    #combine dfs
    df = pd.merge(flow_rate_df, water_level_df, how = 'outer', on='dateTime')
    
    #rename columns
    df.columns = ['flow_rate', 'FR_qualifiers', 'date_time', 'water_level', 'WL_qualifiers']
    
    #convert water levels and flow rates to floats
    df['water_level']=pd.to_numeric(df['water_level'],downcast='float')
    df['flow_rate']=pd.to_numeric(df['flow_rate'],downcast='float')
    
    #change index to DateTime
    df.set_index('date_time', inplace=True, drop=True)
    df.index = pd.to_datetime(df.index)
    
    interp_dates = pd.date_range(
        start = df.index.min(),
        end = df.index.max(),
        freq = datetime.timedelta(minutes=5)
    )
    
    df2 = df.merge(pd.DataFrame(index=interp_dates), how='outer', left_index=True, right_index=True, sort=True)
    interp_water_level = df2['water_level'].interpolate(method='polynomial', order=2)
    interp_flow_rate = df2['flow_rate'].interpolate(method='polynomial', order=2)
    
    
def niceDate(date):
    dateTime = date.split('T')
    time = dateTime[1][:5] + ':00'
    date = dateTime[0]
    return (date+' '+time)

def msg_format(time, state, flow_state, alert):
    message = {
        "name": "River Node",
        "alert": alert,
        "properties": {
            "stationID": "0415000",
            "Time": time,
            "Water_level_units": "ft",
            "Water_level": state,
            "Flow_rate_units":"ft^3/sec",
            "Flow_rate": flow_state
            }
        } 
    return message
    
def init(init_t):
    global time
    global state
    global flow_state
    time = init_t
    
    if str(time)[14:16] == '00':
        state = interp_water_level[str(time)[:13]][0]
        flow_state = interp_flow_rate[str(time)[:13]][0]
    elif str(time)[14:16] == '05':
        state = interp_water_level[str(time)[:13]][1]
        flow_state = interp_flow_rate[str(time)[:13]][1]
    elif str(time)[14:16] == '10':
        state = interp_water_level[str(time)[:13]][2]
        flow_state = interp_flow_rate[str(time)[:13]][2]
    elif str(time)[14:16] == '15':
        state = interp_water_level[str(time)[:13]][3]
        flow_state = interp_flow_rate[str(time)[:13]][3]
    elif str(time)[14:16] == '20':
        state = interp_water_level[str(time)[:13]][4]
        flow_state = interp_flow_rate[str(time)[:13]][4]
    elif str(time)[14:16] == '25':
        state = interp_water_level[str(time)[:13]][5]
        flow_state = interp_flow_rate[str(time)[:13]][5]
    elif str(time)[14:16] == '30':
        state = interp_water_level[str(time)[:13]][6]
        flow_state = interp_flow_rate[str(time)[:13]][6]
    elif str(time)[14:16] == '35':
        state = interp_water_level[str(time)[:13]][7]
        flow_state = interp_flow_rate[str(time)[:13]][7]
    elif str(time)[14:16] == '40':
        state = interp_water_level[str(time)[:13]][8]
        flow_state = interp_flow_rate[str(time)[:13]][8]
    elif str(time)[14:16] == '45':
        state = interp_water_level[str(time)[:13]][9]
        flow_state = interp_flow_rate[str(time)[:13]][9]
    elif str(time)[14:16] == '50':
        state = interp_water_level[str(time)[:13]][10]
        flow_state = interp_flow_rate[str(time)[:13]][10]
    elif str(time)[14:16] == '55':
        state = interp_water_level[str(time)[:13]][11]
        flow_state = interp_flow_rate[str(time)[:13]][11]
    else:
        state = interp_water_level[str(time)[:13]][12]
        flow_state = interp_flow_rate[str(time)[:13]][12]
    
def tick(delta_t):
    global next_time
    global next_state
    global next_flow_state
    next_time = time + timedelta(minutes = delta_t)
    
    if str(time)[14:16] == '00':
        next_state = interp_water_level[str(next_time)[:13]][0]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][0]
    elif str(time)[14:16] == '05':
        next_state = interp_water_level[str(next_time)[:13]][1]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][1]
    elif str(time)[14:16] == '10':
        next_state = interp_water_level[str(next_time)[:13]][2]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][2]
    elif str(time)[14:16] == '15':
        next_state = interp_water_level[str(next_time)[:13]][3]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][3]
    elif str(time)[14:16] == '20':
        next_state = interp_water_level[str(next_time)[:13]][4]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][4]
    elif str(time)[14:16] == '25':
        next_state = interp_water_level[str(next_time)[:13]][5]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][5]
    elif str(time)[14:16] == '30':
        next_state = interp_water_level[str(next_time)[:13]][6]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][6]
    elif str(time)[14:16] == '35':
        next_state = interp_water_level[str(next_time)[:13]][7]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][7]
    elif str(time)[14:16] == '40':
        next_state = interp_water_level[str(next_time)[:13]][8]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][8]
    elif str(time)[14:16] == '45':
        next_state = interp_water_level[str(next_time)[:13]][9]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][9]
    elif str(time)[14:16] == '50':
        next_state = interp_water_level[str(next_time)[:13]][10]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][10]
    elif str(time)[14:16] == '55':
        next_state = interp_water_level[str(next_time)[:13]][11]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][11]
    else:
        next_state = interp_water_level[str(next_time)[:13]][12]
        next_flow_state = interp_flow_rate[str(next_time)[:13]][12]
    
def tock():
    global time
    time = next_time
    global state
    state = next_state
    global flow_state
    flow_state = next_flow_state

def execute (init_t, end_time, delta_t, threshold, wallclock_epoch):        

        global weather_msg
        global wave_msg
        init(init_t) 
        start_time = init_t
        print('>>> Initializing simulator time to', init_t)
        wait_for_wallclock_epoch(wallclock_epoch)
        print('>>> Starting main simulation loop')
        
        while (time < end_time):
            
            global trigger
            if trigger == 2:
                print(">>> TERMINATED")
                break
            
            if(state >= threshold):
                tick(5)
                print(time, ":*FLOOD WATCH*: ", round(state,2), "ft")
                print("                    :*FLOOD WATCH*: ", round(flow_state,0), "ft^3/sec")
                client.publish("topic/logs", payload=json.dumps(msg_format(str(time), str(round(state,2)), str(round(flow_state,2)), True)))
                wait_for_tock(start_time,wallclock_epoch)
                tock()
                
            else:
                try:
                    if wave_msg['alert']:
                        for i in range(18):
                            if trigger == 2:
                                break
                            if state >= threshold:
                                break
                            tick(10)
                            print(time, ":*Possible Flooding*: ", round(state,2), "ft")
                            print("                    :*Possible Flooding*: ", round(flow_state,0), "ft^3/sec")
                            client.publish("topic/logs", payload=json.dumps(msg_format(str(time), str(round(state,2)), str(round(flow_state,2)), False)))
                            wait_for_tock(start_time,wallclock_epoch)
                            tock()
                             
                        wave_msg = 'none'
                    else:
                        tick(delta_t)
                        print(time, ":" ,round(state,2), "ft")
                        print("                    :", round(flow_state,2), "ft^3/sec")
                        client.publish("topic/logs", payload=json.dumps(msg_format(str(time), str(round(state,2)), str(round(flow_state,2)), False)))
                        wait_for_tock(start_time,wallclock_epoch)
                        tock()
                                   
                except: 
                    try:
                        if weather_msg['alert']:
                            for i in range(18):
                                if trigger == 2:
                                    break
                                if state >= threshold:
                                    break
                                tick(10)
                                print(time, ":*Possible Flooding*: ", round(state,2), "ft")
                                print("                    :*Possible Flooding*: ", round(flow_state,0), "ft^3/sec")
                                client.publish("chris/testing", payload=json.dumps(msg_format(str(time), str(round(state,2)), str(round(flow_state,2)), False)))
                                wait_for_tock(start_time,wallclock_epoch)
                                tock()      
                        weather_msg = 'none'
                    except:
                        tick(delta_t)
                        print(time, ":" ,round(state,2), "ft")
                        print("                    :", round(flow_state,2), "ft^3/sec")
                        client.publish("topic/logs", payload=json.dumps(msg_format(str(time),str(round(state,2)), str(round(flow_state,2)), False)))
                        wait_for_tock(start_time,wallclock_epoch)
                        tock()    

                

def reduce_intervals(sleep_time):
    tick(5)
    tock()
    print(time, ":*FLOOD WATCH*: ", round(state,2), "ft")
    print("                    :*FLOOD WATCH*: ", round(flow_state,0), "ft^3/sec")
    t.sleep(sleep_time)
    
    
def wait_for_wallclock_epoch(epoch):
    epoch_diff = epoch - (datetime.datetime.now() + offset)
    print(">>> Waiting for", epoch_diff, 'to synchronize simulation start')
    t.sleep(epoch_diff/timedelta(seconds=1))
    
    
def wait_for_tock(start_time,epoch):
    next_wallclock_time = epoch + (next_time - start_time)/activate_info['properties']['timeScalingFactor']
    time_diff = next_wallclock_time - datetime.datetime.now()
    t.sleep(time_diff/timedelta(seconds=1))
   
def on_message(mqttc, obj, msg):
    """ Callback to process an incoming message."""
    global weather_msg
    global wave_msg
    global activate_info
    global trigger

    try:
        # load the JSON-formatted message into a dictionary
        data = json.loads(msg.payload)
        
        if data['name'] == 'WeatherNode IKW':
            weather_msg = data
            
        if data['name'] == 'ms-wl':
            wave_msg = data
        
        if 'name' in data and data['name'] == 'Control':
            if data['properties']['type'] == 'start':
                retreive_data(data['properties']['simStartTime'], data['properties']['simStopTime'])
                activate_info = data
                trigger = 1
            if data['properties']['type'] == 'stop':
                trigger = 2 
        
    except Exception as e:
        print(e)
        print("Error parsing message ({:})".format(msg.payload))
        

def initialize():
    message = {
        'name':'river node',
        'description':'Model simulating water level and flow rate at USGS site 04156000',
        'properties':{
            'resources':{},
            'subscriptions': [
                'topic/control',
                'topic/wn/output/main'
            ],
            'type':'groundstation'
            }
        }
    
    client.publish('topic/init',json.dumps(message))    

               
#%%


client = mqtt.Client()
client.connect("testbed.code-lab.org", 1883, 60)
client.subscribe("topic/wn/output/main")
client.subscribe('topic/ms/wl/alert')
client.subscribe("topic/control")
client.on_message = on_message
initialize()

client.loop_start()
print(">>> Waiting for activation message")

while trigger != 1:
    t.sleep(.25)
    continue

execute(datetime.datetime.strptime(activate_info['properties']['simStartTime'],'%Y-%m-%dT%H:%M:%S'),
        datetime.datetime.strptime(activate_info['properties']['simStopTime'],'%Y-%m-%dT%H:%M:%S'),
        15,
        18,
        datetime.datetime.strptime(activate_info['properties']['startTime'],'%Y-%m-%dT%H:%M:%S')
        )
client.loop_stop()

from confluent_kafka import Producer
import socket 
import pandas as pd 
import json
import time


#method to be called as a callback to ensure message is delivered or failed 
# def delivered_msg(err,msg): 
#     if err is not None: 
#         print(f"failed to deliver {msg} due to {err}")
#     else : 
#         print(f"message : {msg} is delivered")

conf = {
    'bootstrap.servers':'localhost:9092',
    'client.id':socket.gethostname()
}

#initialize the producer
producer = Producer(conf)

topic = 'dataviz'

df = pd.read_csv('earthquake_data.csv',parse_dates=True)

test_data = df.loc[:20,['Date','Longitude','Latitude','Magnitude']]

for data in test_data.itertuples() : 
    data_obj = {}
    data_obj['date'] = data.Date
    data_obj['longitude']=data.Longitude
    data_obj['latitude']=data.Latitude
    data_obj['magnitude']=data.Magnitude

    data_obj_json = json.dumps(data_obj)
    producer.produce(topic,value=data_obj_json)
    producer.poll(1)
    time.sleep(0.1)
producer.flush()

#!/usr/bin/env python
# The purpose of this script is to broadcast a time signal so that individual sensors
# have access to the time of day.
import paho.mqtt.client as mqtt
import time
import datetime

TIME_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# mosquitto broker config
mqtt_broker = 'localhost'
mqtt_broker_port = 1883
# subscribe to all topics
mqtt_broker_topic = 'meta/time'

def on_publish(client, userdata, mid):
    print("mid: {0} {1}".format(mid, userdata))

client = mqtt.Client()
client.on_publish = on_publish
client.connect(mqtt_broker, mqtt_broker_port, 60)
client.loop_start()

while True:
    current_time_str = datetime.datetime.now().strftime(TIME_STAMP_FORMAT)
    print(current_time_str)
    (rc, mid) = client.publish(mqtt_broker_topic, current_time_str, qos=1)
    time.sleep(30)

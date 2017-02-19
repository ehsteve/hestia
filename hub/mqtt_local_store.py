#!/usr/bin/env python
#
# Simple script to backup up all incoming data from MQTT

import paho.mqtt.client as mqtt
import datetime
import logging
from logging.handlers import RotatingFileHandler
import sys
import glob
import os.path

TIME_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_TIME_STAMP_FORMAT = "%Y%m%d_%H%M%S"

# define the name of the log file
current_time = datetime.datetime.now()
LOGFILE = "{0}_mqtt_subscribe_and_save.log".format(current_time.strftime(FILE_TIME_STAMP_FORMAT))


# create the logger which will output both to a file and to the console
format_str = '%(asctime)s - %(name)s - %(levelname)s:%(message)s'
logFormatter = logging.Formatter(format_str, datefmt=TIME_STAMP_FORMAT)
rootLogger = logging.getLogger('')
rootLogger.setLevel(logging.DEBUG)

fileHandler = RotatingFileHandler(LOGFILE, maxBytes=(1048576*5), backupCount=7)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

def learn_known_topics():
    """Read what feeds have been known in the past"""
    feed_data_files = glob.glob("feed-data/*.txt")
    result = {}
    if len(feed_data_files) != 0:
        for f in feed_data_files:
            feed_name = os.path.splitext(os.path.basename(f))[0].replace("_", "/")
            result.update({feed_name:None})
    return result

list_of_known_topics = learn_known_topics()

def write_data_to_file(fhandler, data):
    """Define a standard way to log to a file"""
    current_time_str = datetime.datetime.now().strftime(TIME_STAMP_FORMAT)
    fhandler.write("{0}, {1}\n".format(current_time_str, data))


rootLogger.info("Existing topics found {0}".format(list_of_known_topics))

# mosquitto broker config
mqtt_broker = 'localhost'
mqtt_broker_port = 1883
# subscribe to all topics
mqtt_broker_topic = '#'

def on_subscribe(client, userdata, mid, granted_qos):
    rootLogger.info("Subscribed: {0} {1}".format(mid, granted_qos))

def on_message(client, userdata, msg):
    rootLogger.info(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
    # check if this is a new topic
    if list(list_of_known_topics.keys()).count(msg.topic) == 0:
        rootLogger.info("New topic discovered. Adding {0} to list of known topics".format(msg.topic))
        # create and open a new file to store data
        f = open("feed-data/" + msg.topic.replace("/", "_") + ".txt", "a", 1)
        write_data_to_file(f, msg.payload)
        # add it to the list
        list_of_known_topics.update({msg.topic:f})
    else:
        f = list_of_known_topics.get(msg.topic)
        # check to see if the file is already open
        if f is not None:
            write_data_to_file(f, msg.payload)
        else:
            f = open("feed-data/" + msg.topic.replace("/", "_") + ".txt", "a", 1)
            list_of_known_topics[msg.topic] = f
            write_data_to_file(f, msg.payload)


def on_connect(mosq, obj, rc):
    rootLogger.info("rc: "+str(rc))

client = mqtt.Client()
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_connect = on_connect
client.connect(mqtt_broker, mqtt_broker_port, 60)
client.subscribe(mqtt_broker_topic, qos=0)
client.loop_forever()


# disconnect from server
rootLogger.info('Disconnected, done.')

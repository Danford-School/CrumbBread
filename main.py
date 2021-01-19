# Danford Compton
# CS410 Data Engineering
# Data Gathering program for Breadcrumbs project

# need these
import json

import requests
import time
import kafka

# this is the url to get the huge JSON file
from kafka import KafkaConsumer, SimpleProducer


url = 'http://rbi.ddns.net/getBreadCrumbData'
# this is to name the file
today = time.strftime("%Y%m%d")
# this gets the file from the URL
# This operation takes a while to download, so be patient if it seems to be hung up
response = requests.get(url)
# I am absolutely not sure this is all correct.
for records in response:
    kafka = kafka.SimpleClient('10.20.30.12:9092')
    producer = SimpleProducer(kafka)
    jd = json.dumps(records)
    producer.send_messages('sensor-data', jd)

# Creates new file with today's date as the name, writes JSON to the file
#with open(today, 'w') as fw:
#    fw.write(response.text)

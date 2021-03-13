#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import requests
import datetime

if __name__ == '__main__':

    url = 'http://rbi.ddns.net/getBreadCrumbData'
    # this gets the file from the URL
    # This operation takes a while to download, so be patient if it seems to be hung up
    data = requests.get(url)
    today = datetime.date.today().strftime("%Y%m%d")
    with open(today, 'w') as fw:
        # get the correct path either here or precede today 
        fw.write(data.text)

    # Read arguments and configurations and initialize

    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))

    theRecords = json.loads(data.text)
    count = 0
    for records in theRecords:
        count += 1
        record_key = "test"
        record_value = json.dumps(records)
        print(records)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce('sensor-data1', key=record_key, value=record_value, on_delivery=acked)
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
        producer.poll(0)

    #Might need to flush more often?
    #catch the error and flush 
    producer.flush()

    # Added this to catch the count of files sent. I should be able to get this data elsewhere,
    # but I don't know where yet. 
    file = open("count_counter.txt", "a")
    file.write(str(today) + " " + str(count) + "\n")
    file.close


    print("{} messages were produced to topic {}!".format(delivered_records, topic))

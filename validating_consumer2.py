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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import psycopg2
import datetime
from datetime import timedelta, date
from testing import check_null, check_range


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'latest',
        })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    # This is the workaround for not having all the values for the following variables
    total_count = 0
    temp_lat = 0
    temp_long = 0
    temp_dir = 0
    temp_speed = 0
    storedID = 0
    storedVehicleNum = 0
    storedDate = 0
    repeat = 0
    print("Consumer is running! Woohoo!")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                #with open('something.txt', "a") as file:
                #    json.dump(data, file)
                #'count' will be updated to dictionary name 
                #count = data['count']
                total_count += 1
                conn = psycopg2.connect(
                        database="crumblydata", user='breadboy', password='leaven', host='127.0.0.1', port= '5432'
                        )
                conn.autocommit = False
                cursor = conn.cursor()

                count = 0
                date = 0
                newtime = 0
                if int(data["ACT_TIME"]) >= 86400:
                    date = (datetime.datetime.strptime(data["OPD_DATE"], '%d-%b-%y'))
                    date = date + datetime.timedelta(days = 1)
                    date = date.strftime('%Y%m%d')


                   # date = (datetime.datetime.strptime(data["OPD_DATE"], '%d-%b-%y').strftime('%Y%m%d')) + datetime.timedelta(days = 1)
                    newtime = datetime.timedelta(seconds = (int(data["ACT_TIME"]) % 86400))
                    
                else:
                    date = datetime.datetime.strptime(data["OPD_DATE"], '%d-%b-%y').strftime('%Y%m%d')
                    newtime = datetime.timedelta(seconds = (int(data["ACT_TIME"])))
                    newtime = '0' + str(newtime)
                lat = data["GPS_LATITUDE"]
                longitude = data["GPS_LONGITUDE"]
                direction = data["DIRECTION"]
                speed = data["VELOCITY"]
               # if not check_range(0, 359, "DIRECTION", data): 
                #    file = open("errors.txt", "a")
                #    file.write("Trip " + data["EVENT_NO_TRIP"] + " at " + data["ACT_TIME"] + " has odd data!")
                #    file.close
               # if not check_null(data, "VELOCITY") and not check_null(data, "DIRECTION"):
                #    file = open("errors.txt", "a")
                #    file.write("Trip " + data["EVENT_NO_TRIP"] + " at " + data["ACT_TIME"] + " has odd data!")
                #    file.close
               # if check_null(data, "EVENT_NO_TRIP"):
                #    file = open("errors.txt", "a")
                #    file.write("Null trip_id discovered!")
                #    file.close
               # if not check_null(data, "RADIO_QUALITY"):
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close
                actual_date = str(date) + " " + str(newtime)
                routeID = data["EVENT_NO_STOP"]
                serviceKey = None
                vehicleID = data["VEHICLE_ID"]
                tripDir = None

                # if check_null(data, "EVENT_NO_STOP"):
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close
                #    continue

                # if check_null(data, "VEHICLE_ID"): 
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close
                 #   continue

               # if check_null(data, "METERS"):
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close
                
               # if check_null(data, "ACT_TIME"): 
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close
                #    continue

               # if not check_range(40, 0, "VELOCITY", data):
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close

               # if not check_range(93000, 15000, "ACT_TIME", data):
                #    file = open("errors.txt", "a")
                #    file.write("Trip number " + str(data["EVENT_NO_TRIP"]) + " has radio data!")
                #    file.close

                if lat == '':
                    lat = temp_lat
                temp_lat = lat

                if longitude == '':
                    longitude = temp_long
                temp_long = longitude

                if direction == '':
                    direction = temp_dir
                temp_dir = direction 


                if speed == '':
                    speed = temp_speed
                temp_speed = speed 
                trip = data["EVENT_NO_TRIP"]
                 
                if trip != storedID and vehicleID != storedVehicleNum and actual_date != storedDate: 

                    try:
                        cursor.execute("INSERT INTO TRIP(TRIP_ID, ROUTE_ID, VEHICLE_ID, SERVICE_KEY, DIRECTION) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (trip, routeID, vehicleID, serviceKey, tripDir))
                        print("Trip " + trip + " completed!")
                        storedID = trip
                        vehicleID = storedVehicleNum
                        storedDate = actual_date
                        try:
                            conn.commit()
                        except psycopg2.errors.IntegrityError as bummer:
                            continue
                        
                    except psycopg2.errors.IntegrityError as bummer:
                        print(trip + " " + actual_date + " record skipped")
                        continue
                
                cursor.execute("INSERT INTO BREADCRUMB(TSTAMP, LATITUDE, LONGITUDE, DIRECTION, SPEED, TRIP_ID) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (actual_date, lat, longitude, direction, speed, trip))
                try:
                    conn.commit()
                except psycopg2.errors.IntegrityError as bummer:
                    continue
    except KeyboardInterrupt:
            pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

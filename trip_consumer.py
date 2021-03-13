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
                storedID = 0
                vehicleID = 0
                storedDate

                conn = psycopg2.connect(
                        database="crumblydata", user='breadboy', password='leaven', host='127.0.0.1', port= '5432'
                        )
                conn.autocommit = False
                cursor = conn.cursor()

                count = 0
                leave_time = data["leave_time"]
                stop_time = data["stop_time"]
                arrive_time = data["arrive_time"]

                x_coordinate = data["x_coordinate"]
                y_coordinate = data["y_coordinate"]
                direction = data["direction"]
                train = data["train"]

                route_number = data["route_number"]
                service_key = data["service_key"]
                dwell = data["dwell"]
                location_id = data["location_id"]
                door = data["door"]
                lift = data["lift"] 
                ons = data["ons"]
                offs = data["offs"]
                estimated_load = data["estimated_load"]
                train_mileage = data["train_mileage"]
                pattern_distance = data["pattern_distance"]
                data_source = data["data_source"]
                schedule_status = data["schedule_status"]
                trip_id = data["trip_id"]
                date = data["date"] 
                vehicle_number = data["vehicle_number"]
                location_distance = data["location_distance"] 
                if route_number == '':
                    route_number = None

                if trip_id != storedID or vehicleID != storedVehicleNum: 

                    try:
                        cursor.execute("UPDATE trip SET ROUTE_ID = %s, SERVICE_KEY = %s WHERE TRIP_ID = %s ON CONFLICT DO NOTHING", (route_number, service_key, trip_id))
                        storedID = trip_id
                        vehicleID = vehicle_number

                        print("Trip " + trip_id + " completed!")
                        try:
                            conn.commit()
                        except psycopg2.errors.IntegrityError as bummer:
                        
                        print(trip + " " + actual_date + " record skipped")
                
                cursor.execute("INSERT INTO stops(leave_time, stop_time, arrive_time, x_coordinate, y_coordinate, direction, service_key, dwell, location_id, door, lift, ons, offs, estimated_load, train_mileage, pattern_distance, location_distance, data_source, schedule_status, trip_id, date, vehicle_number, train, route_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (leave_time, stop_time, arrive_time, x_coordinate, y_coordinate, direction, service_key, dwell, location_id, door, lift, ons, offs, estimated_load, train_mileage, pattern_distance, location_distance, data_source, schedule_status, trip_id, date, vehicle_number, train, route_number))
                try:
                    conn.commit()
                except psycopg2.errors.IntegrityError as bummer:
                    continue
    except KeyboardInterrupt:
            pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

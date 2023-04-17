#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List
import mysql.connector
import datetime

#FILE_PATH = "/Users/shashankmishra/Desktop/Batch-1/Kafka Classes/Confluen Kafka Setup/Confluent-Kafka-Setup/cardekho_dataset.csv"
columns=['emp_id','emp_name','salary','dept_id','updated_at']

API_KEY = '7NHHJTJTIDMEBRLF'
ENDPOINT_SCHEMA_URL  = 'https://psrc-5mn3g.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = 'ceVKxuFyR7C/R45rD/w5mH+T/8uH6PCy06uTnYIr966V7X6MmoqRYAE1IqE3b4i/'
BOOTSTRAP_SERVER = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'SZN3YW7L7TGY4Z3K'
SCHEMA_REGISTRY_API_SECRET = '5Q4m7LTUovehpwYxVG/Rsu5Sre5jNHPiClTWqfDyRHFspZbHgtG/3iKyqyFZtRcS'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Emp:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_emp(data:dict,ctx):
        return emp(record=data)

    def __str__(self):
        return f"{self.record}"


def get_emp_instance(cursor):
    
    cursor.execute("select * from emp where TIMESTAMPDIFF(SECOND,updated_at,NOW())<=300;")
    rows=cursor.fetchall()
    #print(cursor.rowcount)
    emps=[]
    for row in rows:
        data=[]
        data.append(row[0])
        data.append(row[1])
        data.append(row[2])
        data.append(row[3])
        data.append(row[4].strftime('%Y-%m-%d %H:%M:%S'))
        #print(row)
        emp=Emp(dict(zip(columns,data)))
        emps.append(emp)
        yield emp

def emp_to_dict(emp:Emp, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return emp.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, emp_to_dict)

    producer = Producer(sasl_conf())
    max_time=datetime.datetime(1900,1,1)
    print("Producing user records to topic {}. ^C to exit.".format(topic))
    
    try:    
        mydb = mysql.connector.connect(
        host="localhost",
        user="abc",
        password="password",
        database="newdb")

        mycursor = mydb.cursor()
       
    except:
        print("connection failed to mysql database")
    
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    
    try:
        for emp in get_emp_instance(mycursor):
            print(emp)
            
            
            curr_time=datetime.datetime.strptime(emp.record['updated_at'],'%Y-%m-%d %H:%M:%S')
            
            if ((curr_time-max_time)>=datetime.timedelta(seconds=0)):
                
                producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=json_serializer(emp, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            

            #mycursor.execute('SELECT max(updated_at) from emp;')
            #row=mycursor.fetchone()
            #max_time=row[0]  

         
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("emp_topic")
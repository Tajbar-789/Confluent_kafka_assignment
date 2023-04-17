import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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
        return Emp(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Emp.dict_to_emp)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "latest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        cloud_config= {'secure_connect_bundle': '/config/workspace/secure-connect-cassandra-basic.zip'}

        auth_provider = PlainTextAuthProvider('NPSZYNNiDNxwxHLADujEXTcY', 'uaeYSqdbLvU8HMG8NLTupLJ06BegYSO6Ekb.5LBRTEqezg4a+G9LwoAiSuh.tHZ62e.cQErS4t,6taD5B,Ad,r8LmJwf-GaQRSviK8un1Pl6EEqC5C9kJf-49GPAQ7T8')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        #session.execute('create keyspace new;')
        session.execute('use new;')
        session.execute("create table if not exists emp (emp_id int,emp_name text,salary int,dept_id int ,updated_at TIMESTAMP ,PRIMARY KEY ((dept_id) ,emp_id));")
    except:

        print("connection to cassandra not established")

    
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            emp = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if emp is not None:
                print("User record {}: emp: {}\n"
                      .format(msg.key(), emp))
                
                #print(emp.record['emp_name'])
                
                query="INSERT INTO emp (emp_id,emp_name,salary,dept_id,updated_at) VALUES (%s,%s,%s,%s,%s);"
                session.execute(query,(emp.record['emp_id'],emp.record['emp_name'],emp.record['salary'],emp.record['dept_id'],emp.record['updated_at']))
                

        except KeyboardInterrupt:
            break

    consumer.close()

main("emp_topic")
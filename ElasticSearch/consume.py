# consume.py
import json
import pika, os
from os import read
from elasticsearch import Elasticsearch, helpers
import configparser

# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqps://webstepy:inie8RuNuFYfFxFdnSvXfIjDkBCmtEd7@gull.rmq.cloudamqp.com/webstepy')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel
channel.queue_declare(queue='hello') # Declare a queue

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)
    
def callback(ch, method, properties, body):
    print(" [x] Received " + str(body))

channel.basic_consume('HF',
                      callback,
                      auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()
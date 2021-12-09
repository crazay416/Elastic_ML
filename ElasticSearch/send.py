import pika, os, json
import csv
from elasticsearch import Elasticsearch, helpers
import configparser

url = os.environ.get('CLOUDAMQP_URL', 'amqps://webstepy:inie8RuNuFYfFxFdnSvXfIjDkBCmtEd7@gull.rmq.cloudamqp.com/webstepy')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

    
 
def send():
    x = '{  "message": "sellProduct", "productID": 0, "quantity": 10,"buyer": "JetBlue"} '


    channel.basic_publish(exchange='',
                          routing_key='HF',
                          body=x)

    print("sent: " + x)
    connection.close()

send()
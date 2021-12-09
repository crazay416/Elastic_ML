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
    count = 0 #Delete 
    with open("C:\\Users\\Admin\\Downloads\\product_sales_transpose.csv", "r") as f:
        reader = csv.reader(f)
        for row in reader:
            jsonString = json.dumps(row)
            channel.basic_publish(exchange='',
                                    routing_key='HF',
                                    body=jsonString)

            print("sent: " + jsonString)
            
            
            count += 1 #Delete this after debug
            if count ==2: break # Delete this after debug
    connection.close()

send()
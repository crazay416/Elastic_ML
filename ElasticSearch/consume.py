# consume.py
import json
import pika
import os
from os import read
from elasticsearch import Elasticsearch, helpers
import configparser

# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get(
    'CLOUDAMQP_URL', 'amqps://webstepy:inie8RuNuFYfFxFdnSvXfIjDkBCmtEd7@gull.rmq.cloudamqp.com/webstepy')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()  # start a channel
channel.queue_declare(queue='hello')  # Declare a queue

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

# productID is json file
# product ID is ElasticSearch csv


def callback(ch, method, properties, body):
    new_body = str(body).replace("b'", "").replace("} '", "}")
    json_file = json.loads(new_body)
    # print(json_file["productID"])

    res = es.search(index="ml-product-info", filter_path=[
                    "hits.hits._source"], size=10000)

    counter = (len(res["hits"]["hits"]))
    # print(counter)
    for x in range(counter):
        result = res["hits"]["hits"][x]["_source"]["product ID"]
        data = res["hits"]["hits"][x]["_source"]
        if result == json_file["productID"]:
            print(data)

    # print(res)

    # find_data = {
    #     "id": json_file["productID"]
    # }

    data_update = {
        "doc": {
            "Sold": "TRUE",
        }
    }

    # es.update(index="mock_data",
    #           id=json_fi, body=data_update)


channel.basic_consume('HF',
                      callback,
                      auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()

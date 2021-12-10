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


def callback(ch, method, properties, body):
    # productID is json file
    # product ID is ElasticSearch csv

    new_body = str(body).replace("b'", "").replace("}'", "}")
    json_file = json.loads(new_body)

    if json_file["message"] == "ML":
        MLcallback(json_file)
        return
    #print(json_file["productID"])

    check_exists = {
        "match": {
            "product ID": json_file["productID"]
        }
    }

    query_exists = es.search(
        index="ml-product-info", query=check_exists, size=1)

    value = query_exists["hits"]["total"]["value"]



    if(value == 0 and json_file["message"] == "addProduct"):
        doc = {
            "Price": 0,
            "Current Inventory": json_file["quantity"],
            "product ID": json_file["productID"],
            "Product Name": "Product_Test_Name",
            "Manufacturer": json_file["buyer"]
        }

        es.index(index="ml-product-info", body=doc)

    # if(not query_exists["hits"]["hits"][0]):
    #     print("Data does not exist")
    # else:
    #     print("Data Exists")
    # h

    elif value != 0:
        res = es.search(index="ml-product-info", filter_path=[
                        "hits.hits._source"], size=10000)

        counter = (len(res["hits"]["hits"]))
        # print(counter)
        for x in range(counter):
            result = res["hits"]["hits"][x]["_source"]["product ID"]
            data = res["hits"]["hits"][x]["_source"]

            if result == json_file["productID"]:
                print(data)

                find_id = {
                    "match": {
                        "product ID": json_file["productID"]
                    }
                }

                query_data = es.search(
                    index="ml-product-info", query=find_id, size=1)

                index_id = query_data["hits"]["hits"][0]["_id"]

                if(json_file["message"] == "sellProduct"):
                    updateInventory = data["Current Inventory"] - \
                        json_file["quantity"]

                if(json_file["message"] == "addProduct"):
                    updateInventory = data["Current Inventory"] + \
                        json_file["quantity"]

                data_update = {
                    "doc": {
                        "Current Inventory": updateInventory,
                        "Manufacturer": json_file["buyer"]
                    }
                }

                es.update(index="ml-product-info",
                          id=index_id, body=data_update)

def MLcallback(json_file):
    res = es.search(index="ml-product-info", filter_path=[
                        "hits.hits._source"], size=10000)


    for x in list(json_file['body']):
        find_id = {
                        "match": {
                            "product ID": x
                        }
                    }

        query_data = es.search(index="ml-product-info", query=find_id, size=1) #query data with id x

        index_id = query_data["hits"]["hits"][0]["_id"] #doc ID

        data_update = {
                    "doc": {
                        "forcastedSale": json_file["body"][str(x)]
                    }
                }

        es.update(index="ml-product-info", id=index_id, body=data_update)
        print("Update forcasted sale Product ID ", x , ": ", json_file["body"][str(x)])

channel.basic_consume('HF',
                      callback,
                      auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()

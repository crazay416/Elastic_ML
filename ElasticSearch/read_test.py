import csv
from os import read
from elasticsearch import Elasticsearch, helpers
import configparser
import json


config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

def insert():
    with open("C:\\Users\\Admin\\Downloads\\product_sales_transpose.csv", "r") as f:
        reader = csv.reader(f)
        for row in reader:
            jsonString = json.dumps(row)
            print(jsonString)
            # yield {
            #     "_index": "ml_mock_data",
            #     "sales": row
            # }   
            break
            
def main():
    helpers.bulk(es, insert())

insert()
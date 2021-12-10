from elasticsearch import Elasticsearch
import configparser

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

data_update = {
    "doc": {
        "Current Inventory": 15,
        "forcastedSale": 50,

    }
}

es.update(index="ml-product-info",
          id="U_Ongn0BZvKVfYIed2lu", body=data_update)


# id 100
# forecasted 50
# current inverntory 15

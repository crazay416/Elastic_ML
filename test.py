from elasticsearch import Elasticsearch

"""
Each Elasticsearch shard is a Lucene index. 
The maximum number of documents you can have in a Lucene index is 2,147,483,519
"""

es = Elasticsearch(HOST="http://localhost", PORT=9200)
#es = Elasticsearch()

# THIS IS TO CREATE AN INDEX
# es.indices.create(index="isaiah_index")


#index_name = input("Enter the name of the index you would like to create")


# doc_1 = {"city": "Paris", "country": "France"}
# es.index(index="cities", document=doc_1)

# doc_2 = {"city": "Los Angeles", "country": "USA"}
# es.index(index="cities", document=doc_2)


# es.index(index="cities", doc_type="places", id=1, body=doc_1) OLD WAY


# Random Query
"""
body = {
    "from": 0,
    "size": 1,
    "query": {
        "match": {
            "city": "Paris"
        }
    }
}
"""

# res = es.get(index="cities", id = 1)
# res = (es.search(index="cities", body=body))
# res = es.search(index="cities", filter_path=['hits.hits._*'])
# res = es.search(index="cities", filter_path=['hits.hits._source.*'])

# city = res["hits"]["hits"][0]["_source"]["city"]
# country = res["hits"]["hits"][0]["_source"]["country"]
# print("City: " + city + " \nCountry: " + country)

# city = res["hits"]["hits"][1]["_source"]["city"]
# country = res["hits"]["hits"][1]["_source"]["country"]
# print("City: " + city + " \nCountry: " + country)


# This only works up to 10,000
# Must use Scroll API to do so

res = es.search(index="python_airplane",
                filter_path=['hits.hits._source.*'], size=10000)

counter = (len(res["hits"]["hits"]))
print(counter)
for x in range(counter):
    quantity_sold = res["hits"]["hits"][x]["_source"]["Quantity Sold"]
    print(quantity_sold)


"""
res = es.search(index=["python_airplane"])

counter = res["hits"]["total"]

res = es.search(index="cities", filter_path=['hits.hits._source.*'])
res = es.search(index="python_airplane",
                filter_path=['hits.hits._source.*'])

print(counter)
for x in range(15):
    serial_number = res["hits"]["hits"][x]["_source"]["Serial Number"]
    print(serial_number)
"""


"""
for x in range(counter):
    city = res["hits"]["hits"][x]["_source"]["city"]
    country = res["hits"]["hits"][x]["_source"]["country"]
    print("City: " + city + " \nCountry: " + country)
"""

# Since the dictionary is already in a JSON format,
# we already do not need to do it all over again
"""
# print(type(res.hits))
# print(type(res['hits']))

# cities_data = str(res).replace("\'", "\"")
# data = json.loads(cities_data)
# print(type(data))

"""

# This checks if an index exists
"""
if(es.indices.exists(index=index_name) == False):
    es.indices.create(index=index_name)
    print(index_name, " has been created")

print(es.indices.exists(index="isaiah_index"))
"""

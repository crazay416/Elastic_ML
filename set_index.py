from elasticsearch import Elasticsearch
import csv

es = Elasticsearch(
    cloud_id="CSULB_406:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDE0MTdjZWZiNmM0NTRhMzE4ZWE0YmZjMzUwNDZjZDdhJGJmY2VlYmZkZDM1MTRkNTRhNzkwOWQ1ZWNmYWJhZDZj",
    http_auth=("dwizard", "@BeachVibesAlumni2021")
)

with open("C:\\Users\\gisai\\Downloads\\mock3.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        doc = {"Serial Number": row[0], "Date Manufactured": row[1],
               "Expiration Date": row[2], "Name of Product": row[3],
               "Location Manufactured": row[4], "Total Quantity": row[5],
               "Selling Price": row[6], "Initial Price": row[7],
               "Brand": row[8], "Quantity Sold": row[9], "Date Sold": row[10]}
        es.index(index="testingtesting", document=doc)


# with open("C:\\Users\\gisai\\Downloads\\Mock_Data", "r") as f:
#     reader = csv.reader(f)
#     next(reader)
#     for row in reader:
#         doc = {"Serial Number": row[0], "Date Manufactured": row[1],
#                "Expiration Date": row[2], "Name of Product": row[3],
#                "Location Manufactured": row[4], "Total Quantity": row[5],
#                "Selling Price": row[6], "Initial Price": row[7],
#                "Brand": row[8], "Quantity Sold": row[9], "Date Sold": row[10]}
#         es.index(index="test123456", document=doc)


# res = es.search(index="python_airplane",
#                 filter_path=['hits.hits._source.*'], size=10000)

# counter = (len(res["hits"]["hits"]))
# print(counter)
# for x in range(counter):
#     quantity_sold = res["hits"]["hits"][x]["_source"]["Location Manufactured"]
#     print(quantity_sold)

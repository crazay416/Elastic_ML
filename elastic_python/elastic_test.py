from elasticsearch import Elasticsearch
import configparser
import csv

config = configparser.ConfigParser()
config.read('example.ini')

columns = ["Serial Number", "Date Manufactured", "Expiration Date", "Name of Product", "Location Manufactured",
           "Total Quantity", "Selling Price", "Initial Price", "Brand", "Quantity Sold", "Date Sold"]

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)


def main():
    num = ""
    while(num != "0"):
        num = input("Please enter a number: ")
        if(num == "1"):
            insertData()
        elif(num == "2"):
            searchData()
        elif(num == "3"):
            deleteData()
        elif(num == "4"):
            listColumns()


def listColumns():
    for x in range(len(columns)):
        print(x, ".)", columns[x])
    choice = input("Select which you would like to see")
    int_choice = int(choice)
    print(columns[int_choice])
    res = es.search(index="mock3", filter_path=[
                    "hits.hits._source"], size=10000)
    counter = (len(res["hits"]["hits"]))
    for x in range(counter):
        result = res["hits"]["hits"][x]["_source"][columns[int_choice]]
        print(result)


def searchData():
    res = es.search(index="mock3",
                    filter_path=['hits.hits._source.*'], size=1)
    counter = (len(res["hits"]["hits"]))
    print(counter)
    for x in range(counter):
        quantity_sold = res["hits"]["hits"][x]["_source"]["Quantity Sold"]
        print(quantity_sold)


def insertData():
    with open("C:\\Users\\gisai\\Downloads\\mock3.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            doc = {"Serial Number": row[0], "Date Manufactured": row[1],
                   "Expiration Date": row[2], "Name of Product": row[3],
                   "Location Manufactured": row[4], "Total Quantity": row[5],
                   "Selling Price": row[6], "Initial Price": row[7],
                   "Brand": row[8], "Quantity Sold": row[9], "Date Sold": row[10]}
            es.index(index="mock3", document=doc)


def deleteData():
    index_name = input("Choose an index to delete")
    es.indices.delete(index=index_name, ignore=[400, 404])
    print("Index is now deleted")


# print(es.info())

# es.indices.delete(index='406mbe', ignore=[400, 404])

main()

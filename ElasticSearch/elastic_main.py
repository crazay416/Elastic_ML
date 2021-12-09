from elasticsearch import Elasticsearch
import configparser
import csv
from datetime import date

config = configparser.ConfigParser()
config.read('example.ini')

columns = ["Serial Number", "Date Manufactured", "Expiration Date", "Name of Product", "Location Manufactured",
           "Selling Price", "Initial Price", "Brand", "Date Sold"]

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)


def main():
    today = date.today().strftime("%m/%d/%Y")
    print("Today's Date: ", today)
    options()
    selection()


def options():
    print("1.)Insert Data \n2.)Search Data \n3.)Delete Data \n4.)List categories \n5.)Buy a Product")


def selection():
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
        elif(num == "5"):
            queryProduct()


def listColumns():
    for x in range(len(columns)):
        print(x, ".)", columns[x])

    choice = input("Select which you would like to see")
    int_choice = int(choice)
    print(columns[int_choice])

    res = es.search(index="mock_data", filter_path=[
                    "hits.hits._source"], size=10000)
    counter = (len(res["hits"]["hits"]))
    for x in range(counter):
        result = res["hits"]["hits"][x]["_source"][columns[int_choice]]
        print(result)


def searchData():
    res = es.search(index="mock_data",
                    filter_path=['hits.hits._source.*'], size=100000)
    counter = (len(res["hits"]["hits"]))
    print(counter)
    for x in range(counter):
        quantity_sold = res["hits"]["hits"][x]["_source"]["Quantity Sold"]
        print(quantity_sold)


def insertData():
    with open("C:\\Users\\gisai\\Downloads\\mock_data.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            doc = {"Serial Number": row[0], "Date Manufactured": row[1],
                   "Expiration Date": row[2], "Name of Product": row[3],
                   "Location Manufactured": row[4], "Selling Price": row[5],
                   "Initial Price": row[6], "Brand": row[7], "Date Sold": row[8],
                   "Sold": row[9]}
            es.index(index="mock_data", document=doc)

            # doc = {"Serial Number": row[0], "Date Manufactured": row[1],
            #        "Expiration Date": row[2], "Name of Product": row[3],
            #        "Location Manufactured": row[4], "Total Quantity": row[5],
            #        "Selling Price": row[6], "Initial Price": row[7],
            #        "Brand": row[8], "Quantity Sold": row[9], "Date Sold": row[10]}
            # es.index(index="mock3", document=doc)


def deleteData():
    index_name = input("Choose an index to delete")
    es.indices.delete(index=index_name, ignore=[400, 404])
    print("Index is now deleted")


def queryProduct():
    for x in range(len(columns)):
        print(x, ".)", columns[x])

    column_choice = int(input("Select which you would like to Search for"))
    name_choice = input("What is the name of the you are looking for?")

    # query_body = {
    #     "match": {
    #         columns[column_choice]: str(name_choice)
    #     }
    # }

    query_body = {
        "match": {
            columns[column_choice]: str(name_choice)
        },
        "match": {
            "Sold": "FALSE"
        }
    }

    query_data = es.search(index="mock_data", query=query_body, size=10000)

    buy_Product(query_data, column_choice, name_choice)


def buy_Product(query_data, column_choice, name_choice):
    counter = len(query_data["hits"]["hits"])

    print(query_data, "\n\n\n")

    for x in range(counter):
        result = query_data["hits"]["hits"][x]["_source"][columns[column_choice]]
        print(x, ".)", result)

    buy_option = ""
    while(buy_option != "-1"):
        buy_option = input("Would you like to buy a product?: ")
        if(buy_option != "-1"):
            buy_option = int(buy_option)
            data_id = query_data["hits"]["hits"][buy_option]["_id"]
            print(data_id)
            data_update = {
                "doc": {
                    "Sold": "TRUE",
                    "Date Sold": date.today().strftime("%m/%d/%Y")
                }
            }
            es.update(index="mock_data",
                      id=data_id, body=data_update)

    # if(result == name_choice):
    #     print(result)

    # print(es.info())
    # es.indices.delete(index='406mbe', ignore=[400, 404])
main()
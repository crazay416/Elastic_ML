import pika, os, json
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from datetime import date

# TODO 2 messages for all prediction and low sales??, psql integration


################# GLOBAL #################
df_sales = pd.read_csv("product_sales.csv")
df_inventory = pd.read_csv("product_info.csv")
mean = 49.31343913738019
std = 26.957462987074493
model = keras.models.load_model(os.path.join(os.getcwd(), 'in20_out7_20epoch_02dropout_units20_lr0001.h5'))
output_window = 7
product_count = 500


################# INVENTORY #################
def edit_inventory(product_id, quantity):
    df_inventory.loc[product_id, "Current Inventory"] = df_inventory.iloc[product_id][-1] + quantity
    df_inventory.to_csv("product_info.csv", index=False)

def add_sale(product_id, quantity):
    edit_inventory(product_id, -(quantity))
    today = date.today().strftime("%m/%d/%Y")
    if df_sales.columns.values[-1] != today:
        df_sales[today] = [0] * 500
    df_sales.loc[product_id, today] += quantity
    df_sales.to_csv("product_sales.csv", index=False)


################# PREDICTION #################
def recommendation():
    return deploy_predict(get_data_deployed())


def normalize(data):
    data = data - mean
    data = data / std
    return data


def reverse_normalize(data):
    data = data * std
    data = data + mean
    return data


def deploy_predict(data):
    Y_pred = model.predict(data)
    x = 0
    new_pred = np.zeros((output_window, product_count))
    for i in Y_pred:
        temp = i.reshape((output_window, product_count))
        for j in temp:
            new_pred[x] = j
            x += 1
    return reverse_normalize(np.transpose(new_pred))


def get_data_deployed():
    #TODO change below to get previous 20 days instead of last 20 from csv after elastic search is set up
    data = np.asarray([np.transpose(df_sales.iloc[:,-21:-1].to_numpy())])
    data = normalize(data)
    return data


def is_low_product(sales, product_id):
    #TODO query elastic for current inventory
    # gets current inventory for all products
    df_inventory.iloc[:, -1]
    # if sales > inventory[product_id]:
    #     return True
    # return False


################# MESSAGES #################
def create_output_ML_json(forecast):
    pass

def send(forecast):

    x = {
        "message": "ML",
        "body": {

        }
    }

    for index, i in enumerate(forecast):
        x["body"]["{}".format(index)] = int(np.sum(i))

    file = json.dumps(x)

    channel.basic_publish(exchange='',
                          routing_key='HF_ML',
                          body=str(file))

    print("sent: " + str(file))


################# RECEIVE #################

url = os.environ.get('CLOUDAMQP_URL', '') # <- PUT SERVER LINK HERE
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

def callback(ch, method, properties, body):
    print(" [x] Received " + body.decode("utf-8"))

    jsonObj = json.loads(body.decode("utf-8"))
    if jsonObj["message"] == "getInventoryRecommendations":
        send(recommendation())
    elif (jsonObj["message"] == "sellProduct"):
        add_sale(jsonObj["productID"], jsonObj["quantity"])
    elif (jsonObj["message"]== "addProduct"):
        edit_inventory(jsonObj["productID"], jsonObj["quantity"])


channel.basic_consume('HF_ML',
                      callback,
                      auto_ack=True)

# while (True):
print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()

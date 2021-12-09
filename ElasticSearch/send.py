import pika, os, json
url = os.environ.get('CLOUDAMQP_URL', 'amqps://wfsdzxpt:UdYJ3pVxVAEEtnP6RYBzs1fnvbTaocKb@gull.rmq.cloudamqp.com/wfsdzxpt') # <- PUT SERVER LINK HERE
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

def send():

    # x = '{ "message": "getInventoryRecommendations" }'
    x = '{  "message": "sellProduct", "productID": 0, "quantity": 10,"buyer": "JetRed"} '
    # x = '{ "message": "addProduct","productID": 0,"quantity": 10}'


    channel.basic_publish(exchange='',
                          routing_key='HF_ML',
                          body=x)

    print("sent: " + x)
    connection.close()

send()
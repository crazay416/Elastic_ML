# consume.py
import pika, os

# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqps://wfsdzxpt:UdYJ3pVxVAEEtnP6RYBzs1fnvbTaocKb@gull.rmq.cloudamqp.com/wfsdzxpt')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel
channel.queue_declare(queue='hello') # Declare a queue
def callback(ch, method, properties, body):
  print(" [x] Received " + str(body))

channel.basic_consume('HF_ML',
                      callback,
                      auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()
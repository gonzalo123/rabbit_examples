import pika
import logging
import json
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

queue_name = 'dlx'

logging.basicConfig(level=logging.WARNING)


def callback(ch, method, properties, body):
    logging.warning("body".format(body))
    logging.warning("headers: {}".format(json.dumps(properties.headers)))


# Connect to Rabbit using credentials
broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

# create a new channel
channel = broker_connection.channel()
# create the queue if doesn't exits
channel.queue_declare(queue=queue_name)

# register callback to the queue
channel.basic_consume(consumer_callback=callback, queue=queue_name, no_ack=True)
channel.basic_qos(prefetch_count=1)
channel.start_consuming()

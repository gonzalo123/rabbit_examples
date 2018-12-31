import pika
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

queue_name = 'example2_queue'
exchange_name = 'example2_exchange'

logging.basicConfig(level=logging.WARNING)


def callback(ch, method, properties, body):
    logging.warning("body {}".format(body))
    # emit ack manually
    # we need to specify the delivery_tag
    # delivery_tag is an autoincrement number
    # depends on the channel. If I stop the scrip and start again
    # delivery_tag will start again
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Connect to Rabbit using credentials
broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

# create a new channel
channel = broker_connection.channel()
# create the queue if doesn't exits
channel.queue_declare(queue=queue_name)
# create the exchange if it doesn't exists
channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
# bind exchange to queue
channel.queue_bind(exchange=exchange_name, queue=queue_name)

# register callback to the queue
# no_ack=False means that I need to send ack manually
channel.basic_consume(consumer_callback=callback, queue=queue_name, no_ack=False)
channel.basic_qos(prefetch_count=1)
channel.start_consuming()

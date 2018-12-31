import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)


def event(data):
    queue_name = 'example2_queue'
    exchange_name = 'example2_exchange'

    logging.info("emit message: {} to exchange: {}".format(data, exchange_name))

    # Connect to Rabbit using credentials
    broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

    # create a new channel
    channel = broker_connection.channel()
    # create the queue if doesn't exits
    channel.queue_declare(queue=queue_name)
    # create the exchange if it doesn't exists
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    # emit message to the exchange
    channel.basic_publish(exchange=exchange_name, routing_key='', body=data)
    broker_connection.close()


if __name__ == '__main__':
    event(sys.argv[1])

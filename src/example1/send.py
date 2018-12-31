import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)


def event(queue_name, data):
    logging.info("emit message: {} to queue: {}".format(data, queue_name))

    # Connect to Rabbit using credentials
    broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

    # create a new channel
    channel = broker_connection.channel()
    # create the queue if doesn't exits
    channel.queue_declare(queue=queue_name)
    # emit message to the default exchange
    # that means message will be deliver to the a queue called like the routing key
    channel.basic_publish(exchange='', routing_key=queue_name, body=data)
    broker_connection.close()


if __name__ == '__main__':
    event('example1', sys.argv[1])

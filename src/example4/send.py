import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)

queue_name = 'example4'


def event(data):
    logging.info("emit message: {} to queue: {}".format(data, queue_name))

    # Connect to Rabbit using credentials
    broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

    # create a new channel
    channel = broker_connection.channel()
    # create the queue if doesn't exits
    channel.queue_declare(queue=queue_name)

    # emit message to the default exchange
    # we add a header with retry count
    # also we add dead letter queue
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=data,
                          properties=pika.BasicProperties(
                              headers={
                                  'retries': 0,
                                  'x-dead-letter-queue': 'dlx'
                              }
                          ))
    broker_connection.close()


if __name__ == '__main__':
    event(sys.argv[1])

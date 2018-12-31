import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)

queue_name = 'deadqueue'


def delayed_sent(data, ttl):
    logging.info("emit message: {} to queue: {}".format(data, queue_name))

    # Connect to Rabbit using credentials
    broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

    # create a new channel
    channel = broker_connection.channel()
    # create the queue if doesn't exits
    # we define also the ttl and dead-letter queue
    result = channel.queue_declare(arguments={
        'x-message-ttl': ttl,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': queue_name
    })
    queue = result.method.queue

    # emit message via default exchange to the temporary queue
    # nobody will listen to this queue. We only want it to fordward
    # the message to the dead-letter queue
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=data
                          )
    broker_connection.close()


if __name__ == '__main__':
    delayed_sent(sys.argv[1], int(sys.argv[2]))

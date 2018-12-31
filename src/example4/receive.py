import pika
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

queue_name = 'example4'

logging.basicConfig(level=logging.WARNING)


def callback(ch, method, properties, body):
    # get retry header from properties
    retries = properties.headers['retries']
    logging.warning("retry: {}".format(retries))

    # if retry count en minor than 3
    if retries < 3:
        properties.headers['retries'] = retries + 1
        logging.warning("reject")
        # we reject the message
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        pika.BlockingConnection.sleep(broker_connection, duration=1)
        # and re-publish a new one in the same queue
        channel.basic_publish(exchange=method.exchange,
                              routing_key=method.routing_key,
                              body=body,
                              properties=properties)
    else:
        logging.warning("ack")
        dead_letter_queue = properties.headers['x-dead-letter-queue']
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        channel.basic_publish(exchange='',
                              routing_key=dead_letter_queue,
                              body=body,
                              properties=pika.BasicProperties(
                                  headers=properties.headers
                              ))


# Connect to Rabbit using credentials
broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

# create a new channel
channel = broker_connection.channel()
# create the queue if doesn't exits
channel.queue_declare(queue=queue_name)
channel.basic_consume(consumer_callback=callback, queue=queue_name, no_ack=False)
channel.start_consuming()

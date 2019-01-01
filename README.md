## Playing with RabbitMQ. Usage examples

The RabbitMQ documentation is great. You can copy and paste one example in your favourite language and all is up and running. That's great but sometimes we need to master a little bit. In this post I want to show different examples that I've playing with. Basically because I don't want to forget them. My computer isn't the best place to store my documentation.

### Example 1

It's the simpler example.

> Producer -> Queue -> Consumer.

I've started to understand RabbitMQ when I realized that this picture is impossible. One producer cannot write directly to a queue. We always need an Exchange. The trick here is that we're not creating the Exchange. We're using the default one (''). This exchange put the message in one queue named like the routing_key.

The Producer:
```python
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
```

And the consumer:
```python
import pika
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

queue_name = 'example1'

logging.basicConfig(level=logging.WARNING)


def callback(ch, method, properties, body):
    logging.warning("body {}".format(body))


# Connect to Rabbit using credentials
broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

# create a new channel
channel = broker_connection.channel()
# create the queue if doesn't exits
channel.queue_declare(queue=queue_name)

# register callback to the queue
# no_ack=True means auto ack will be sent
channel.basic_consume(consumer_callback=callback, queue=queue_name, no_ack=True)
channel.basic_qos(prefetch_count=1)
channel.start_consuming()
```

### Example 2
Now the same example than the previous one but using a fanout Exchange instead of the default one.

The producer
```python
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
```

The consumer needs to use a binding to join exchange and queue.
```python
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
```

### Example 3
The first two examples came from the official documentation. 
Now we're going to do something "more complicated". We'll retry each message 5 times. 
The idea is create a header with the retry count.
The Producer:
```python
import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)

queue_name = 'example3'


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
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=data,
                          properties=pika.BasicProperties(
                              headers={'retries': 0}
                          ))
    broker_connection.close()


if __name__ == '__main__':
    event(sys.argv[1])
```

In the Consumer we can see how we re-send a new message with the same body and headers but incrementing the counter.

```python
import pika
import sys
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

logging.basicConfig(level=logging.WARNING)

queue_name = 'example3'


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
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=data,
                          properties=pika.BasicProperties(
                              headers={'retries': 0}
                          ))
    broker_connection.close()


if __name__ == '__main__':
    event(sys.argv[1])
```

### Example 4
Now the idea is the same than the previous example but now we're going to send the message to a dead-letter queue after 3 retries.
Producer:
```python
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
```

Consumer
```python
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
```

And Dead letter consumer
```python
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
```

### Example 5
In the last example we're going to work with delayed sends. The idea is send messages to random generated queue. This queue has a ttl and after this time messages are resent to a dead-letter queue. If we listen to this dead-letter queue we'll see the messages with the ttl delay. 

The producer:

```python
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
```

And the consumer:
```python
import pika
import logging
from dotenv import load_dotenv
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path="{}/../.env".format(current_dir))

queue_name = 'deadqueue'

logging.basicConfig(level=logging.WARNING)


def callback(ch, method, properties, body):
    logging.warning("body {}".format(body))


# Connect to Rabbit using credentials
broker_connection = pika.BlockingConnection(pika.URLParameters(os.getenv('AMQP_URI')))

# create a new channel
channel = broker_connection.channel()
# create the queue if doesn't exits
channel.queue_declare(queue=queue_name)

# register callback to the queue
# no_ack=True means auto ack will be sent
channel.basic_consume(consumer_callback=callback, queue=queue_name, no_ack=True)
channel.basic_qos(prefetch_count=1)
channel.start_consuming()
```
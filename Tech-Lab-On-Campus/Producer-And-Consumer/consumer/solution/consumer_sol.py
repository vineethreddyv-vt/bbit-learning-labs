 

import os

import pika
from consumer_interface import mqConsumerInterface
#from RabbitMQ.interfaces.consumerInterface import consumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
       # super().__init__(binding_key, exchange_name, queue_name)
        self.binding_key= binding_key
        self.exchange_name = exchange_name
        self.queue_name= queue_name
         # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

         # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue_name)

        # Declare exchange and queue, and bind them
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.binding_key)

        # Set up message callback
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback, auto_ack=True)

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Print the received message
        print("Received message:", body.decode('utf-8'))

    def startConsuming(self) -> None:
        # Start consuming messages
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self) -> None:
        # Close connection and channel
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 09:25:35 2017
Usage:
    msg=json.dumps({'test':'result'})
    o = pubMsg(queue='test',rabhost='do-prd-rab-01.do.viaa.be',
           user='UUUUU',
           passwd='XXXXXX',
           msg=msg)
    print(o())

@author: tina
"""
import logging
from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials
# from pika import exceptions
LOGGER = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)


class PubMsg():
    """Publish a message to a queue with exchange and routing key"""
    def __init__(self, queue, rabhost,
                 user, passwd, msg, routing_key='py-fxp-publisher'):
        self.queue = queue
        self.host = rabhost
        self.topic_type = 'direct'
        self.user = user
        self.passwd = passwd
        self.result_exchange = 'py-fxp-publisher'
        self.publish_connection = BlockingConnection(
            ConnectionParameters(host=self.host,
                                 port=5672,
                                 virtual_host='/',
                                 retry_delay=3,
                                 connection_attempts=60,
                                 credentials=PlainCredentials(self.user,
                                                              self.passwd)))
        self.publish_channel = self.publish_connection.channel()
        self.result_routing = routing_key
        self.msg = msg
        self.publish_channel.queue_declare(queue=self.queue, passive=False,
                                           durable=True, exclusive=False,
                                           auto_delete=False)

    def __call__(self):
        if self.queue is not None\
                        and self.topic_type is not None:
            self.publish_channel.\
                exchange_declare(exchange='py-fxp-publisher')

            self.publish_channel.queue_bind(queue=self.queue,
                                            exchange=self.result_exchange,
                                            routing_key=self.result_routing)
            self.publish_channel.basic_publish(exchange=self.result_exchange,
                                               routing_key=self.result_routing,
                                               body=self.msg,
                                               properties=BasicProperties(
                                                   content_type='application/json',
                                                   delivery_mode=1))
        LOGGER.info('Message published to exchange: %s with routing key: %s',
                    self.result_exchange, self.result_routing)
        self.publish_connection.close()
        return True

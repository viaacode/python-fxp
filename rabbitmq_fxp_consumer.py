# -*- coding: utf-8 -*-
"""
Discription:
    - INPUT for FXP worker
    - RABBITMQ consumer, ACK and forward msg to service
USAGE:
    - post a msg on queue
message validates if:
     conf_schema = Schema({
            'destination_file': And(Use(str)),
            'destination_host': And(Use(str)),
            'destination_password': And(Use(str)),
            'destination_path': And(Use(str)),
            'destination_user': And(Use(str)),
            'source_file': And(Use(str)),
            'source_host': And(Use(str)),
            'source_password': And(Use(str)),
            'source_path': And(Use(str)),
            'source_user': And(Use(str)),
            Optional('move'): And(Use(bool))
            })

"""
import logging
import datetime
import json
import configparser
import pika
from schema import Schema, And, Use, Optional, SchemaError
from retry import retry
from celery import chain
from worker_tasks import res_pub, fxp_task

LOG_FORMAT = ('[%(asctime)-15s %(levelname) -8s %(name) -10s %(funcName) '
              '-20s %(lineno) -5d] %(message)s')
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
LOGGER = logging.getLogger(__name__)
CONFIG = configparser.ConfigParser()
CONFIG.read('config/config.ini')


def fxp_call(msg):
    """Description:
        - porcesses a json msg if valid:
            - send a  Chain of jobs to a clery worker, async
       Chain:
            - First FXP
            - Then Post to Rabitmq
    """
    msg = json.loads(msg)
    LOGGER.info('**** FXP call from rabbit input****')
    LOGGER.debug('incoming message %s', msg)
    default_err = {'error': 'not a valid message'}
    time_now = datetime.datetime.now().isoformat()
    # job = chain(fxp_task.s(msg),
    #             res_pub.s(CONFIG['fxp-rab-publisher']['user'],
    #                       CONFIG['fxp-rab-publisher']['passw'],
    #                       CONFIG['fxp-rab-publisher']['hostname'],
    #                       queue=CONFIG['fxp-rab-publisher']['queue'],
    #                       routing_key='py-fxp'))
    
    job=fxp_task.s(msg)
    def check_msg():
        """Checks msg to conform to fields and types of values"""
        def check(conf_schema, conf):
            try:
                conf_schema.validate(conf)
                return True
            except SchemaError:
                return False

        conf_schema = Schema({
            'destination_file': And(Use(str)),
            'destination_host': And(Use(str)),
            'destination_password': And(Use(str)),
            'destination_path': And(Use(str)),
            'destination_user': And(Use(str)),
            'source_file': And(Use(str)),
            'source_host': And(Use(str)),
            'source_password': And(Use(str)),
            'source_path': And(Use(str)),
            'source_user': And(Use(str)),
            Optional('move'): And(Use(bool))
            })
        valid_msg = check(conf_schema, msg)
        LOGGER.debug(valid_msg)
        if valid_msg:
            return True
        return False
    chk_msg = check_msg()
    LOGGER.info('valid fxp message: %s', chk_msg)
    if chk_msg:
        j = job.apply_async(retry=True)
        job_id = j.id
        LOGGER.info('fxp task %s started', job_id)
        LOGGER.info('FXP request for destination_file %s',
                    msg['destination_file'])
        out = {'ID': job_id,
               'Date': str(time_now)}
        LOGGER.info(str(out))
        return json.dumps(out)
    return json.dumps(default_err)


if __name__ == "__main__":
    URL = CONFIG['fxp-rab-consumer']['uri']
    PARAMS = pika.URLParameters(URL)
    CONNECTION = pika.BlockingConnection(PARAMS)
    # OPen a channel
    CHANNEL = CONNECTION.channel()
    QUEUE = CONFIG['fxp-rab-consumer']['queue']
    CHANNEL = CONNECTION.channel()
    CHANNEL.basic_qos(prefetch_count=1)
#    CHANNEL.queue_declare(queue=QUEUE, durable=True)
    try:
        # create a function which is called on incoming messages
        def call_back(ch, method, properties, body):
            """The function to call on incoming message"""
            fxp_call(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        CHANNEL.basic_consume(QUEUE, call_back)
        # start consuming (blocks)
        @retry(TimeoutError, tries=-1)
        def start():
            """This starts the consumer"""
            CHANNEL.start_consuming()

        start()
    except KeyboardInterrupt:
        CONNECTION.close()
        exit()

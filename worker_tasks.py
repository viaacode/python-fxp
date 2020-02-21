#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WORKERS stuff
Created on Fri Nov  3 13:58:25 2017
CELERY TASKS definations
  Note: fake=True variables to pass true to keep One chain
@author: tina
"""
from kombu import Exchange, Queue
from celery import Celery, current_task
import os
import json
import celeryconfig
from celery.utils.log import get_task_logger
from rabbitmq_publish import PubMsg
from fxp_file import Fxp

APP = Celery('tasks',)
APP.config_from_object(celeryconfig)
LOGGER = get_task_logger(__name__)


APP.conf.task_queues = (Queue('py-fxp',
                              Exchange('py-fxp'),
                              routing_key='py-fxp'),)
APP.conf.task_default_queue = 'py-fxp'
APP.conf.task_default_exchange_type = 'direct'
APP.conf.task_default_routing_key = 'py-fxp'

APP.conf.task_routes = {'tasks.worker_tasks.*': {'queue': 'py-fxp',
                                                 'routing_key': 'py-fxp', }, }


@APP.task(bind=True)
def fxp_task(self, msg):
    """FXP transfer """
    src_file = msg['source_file']
    src_user = msg['source_user']
    src_passwd = msg['source_password']
    dst_passwd = msg['destination_password']
    dst_user = msg['destination_user']
    dst_file = msg['destination_file']
    src_host = msg['source_host']
    dst_host = msg['destination_host']
    src_dir = msg['source_path']
    dst_dir = msg['destination_path']
    move = msg['move']
    in_path = os.path.join(src_dir, src_file)
    out_path = os.path.join(dst_dir, dst_file)
    try:
        fxp_outcome = Fxp(src_host, src_user, src_passwd,
                          dst_host, dst_user, dst_passwd,
                          in_path, out_path, move)()
        LOGGER.info('FXP task finished successful!: %s', fxp_outcome)
        return msg
    except Exception as fxp_e:
        LOGGER.error(fxp_e)
        LOGGER.error('***ERROR: %s HAS FAILED TO FXP to %s on %s ***',
                     msg['source_file'],
                     out_path,
                     dst_host)
        raise self.retry(coutdown=60*3, exc=fxp_e, max_retries=1)


# @app.task(rate_limit='5/h')
@APP.task(bind=True)
def ftp_result(self, infile, destfile, server, user, passwd,
               ftp_fake=False):
    """FTP a file, rename if exists"""
    if ftp_fake:
        return infile
    import ftplib
    infile = os.path.abspath(infile)
    f = open(infile, 'rb')
    ftp = ftplib.FTP(server)
    ftp.login(user, passwd)
    ftp.set_pasv(True)
    #  Test to see if the infile exists by getting the filesize by name.
    #  If a -1 is returned, the file does not exist.
    try:
        size = ftp.size(destfile)
        if size < 0:
            LOGGER.info("file does not exist OK")
        else:
            LOGGER.warning("!! Destination %s exists and is %s", str(size),
                           destfile)
            LOGGER.warning('renaming outputfile')
            destfile = os.path.splitext(destfile)[0] + '.1' + os.path.splitext(destfile)[1]
    except ConnectionError as conn_e:
        LOGGER.error(conn_e)
    try:
        LOGGER.info('Storing File %s to %s', infile, destfile)
        ftp.storbinary('STOR %s' % '{}'.format(destfile), f)
        LOGGER.info('file stored %s', destfile)
        ftp.quit()
        return infile
    except Exception as e:
        LOGGER.error(str(e))
        raise self.retry(coutdown=12, exc=e, max_retries=5)


@APP.task(bind=True)
def res_pub(self, msg, user, passw, host, queue, routing_key):
    t_id = current_task.request.id
    final_msg = json.dumps({'content': msg,
                            'task id': t_id
                            })
    try:
        PubMsg(queue=queue, rabhost=host,
               user=user, passwd=passw,
               msg=final_msg,
               routing_key=routing_key)()
        return final_msg
    except Exception as e:
        self.retry(coutdown=60*10, exc=e)

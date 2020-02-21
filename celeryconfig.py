# -*- coding: utf-8 -*-
"""
CONFIG for celery worker
"""
import os
import configparser
CONFIG = configparser.ConfigParser()
CONFIG.read('config/config.ini')
if 'BROKER_URL' in os.environ:
    broker_url = os.environ.get('BROKER_URL')
else:
    broker_url = CONFIG['Celery']['broker_url']
BROKER_URL = CONFIG['Celery']['broker_url']
if 'RESULT_BACKEND' in os.environ:
    result_backend = os.environ.get('RESULT_BACKEND')
else:
    result_backend = 'elasticsearch://do-prd-lst-01.do.viaa.be:9200/fxp/results'
task_serializer = 'json'
accept_content = ['json']
enable_utc = True
result_persistent = True

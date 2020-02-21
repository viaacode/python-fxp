#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 09:59:49 2019
Description:
    - FXP a file between 2 FTP servers
    - Param move=True, to delete the source file
    - Retry random wait between  1 min 3 min, on some FTP errors

Usage:
    Fxp('host1','user1','passwd1','host2','user2','passwd2',
        'full_source_path',
        'full_destination_path',move=False)()
@author: tina
"""

import logging
import os
import configparser
import ftpext
from celery.utils.log import get_logger
from retrying import retry
LOG_FORMAT = ('[%(asctime)-15s %(levelname) -8s %(name) -10s %(funcName) '
              '-20s %(lineno) -5d] %(message)s')
logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
LOGGER = get_logger(__name__)
CONFIG = configparser.ConfigParser()
CONFIG.read('config/config.ini')
PORT = 21

def retry_if_ftperror(exception):
    """Return True if we should retry, False otherwise"""
    if isinstance(exception, ftpext.ftplib.error_temp):
        LOGGER.warning('too manny/bad connection(s), retry is True')

        return isinstance(exception, ftpext.ftplib.error_temp)

    if isinstance(exception, ftpext.ftplib.error_perm):
        LOGGER.error('FTP Error: Permannetly failed')
        return False
    return False


def timeout_err(exception):
    """Return True if we should retry, False otherwise"""
    LOGGER.warning('Trouble connecting, retry is True')
    return isinstance(exception, TimeoutError)


@retry(retry_on_exception=timeout_err,
       wait_random_min=10000, wait_random_max=60000,
       stop_max_attempt_number=3)
def fxp_src(host='host1', user='user1', password='password1'):
    """ Define a sorce server"""
    try:
        ftp_src = ftpext.FTPExt(host, PORT, user, password, False)
    except ConnectionError as conn_e:
        LOGGER.error(conn_e)
        return False

    def _close():
        """ Use this to close the connection"""
        ftp_src.close()
    return ftp_src


@retry(retry_on_exception=timeout_err,
       wait_random_min=10000, wait_random_max=60000,
       stop_max_attempt_number=3)
def fxp_dst(host='host2', user='user2', password='password2'):
    """ Define a destination server"""
    try:
        ftp_dst = ftpext.FTPExt(host, PORT, user, password, False)
    except ConnectionError as conn_e:
        LOGGER.error(conn_e)
        return False

    def _close():
        ftp_dst.close()

    return ftp_dst


@retry(retry_on_exception=timeout_err,
       wait_random_min=60000, wait_random_max=180000,
       stop_max_attempt_number=3)
def delete_from_ftp(del_file, FTPE=None):
    '''Delete a file from ftp'''
    try:
        FTPE.delete(del_file)
    except FileNotFoundError:
        LOGGER.error('file: %s Not found', del_file)
    except ConnectionError as conn_e:
        LOGGER.error(conn_e)
    return True


@retry(retry_on_exception=retry_if_ftperror,
       wait_random_min=10000, wait_random_max=60000,
       stop_max_attempt_number=3)
class Fxp():
    """ Class for FXP transfer """
    def __init__(self,
                 host1,
                 user1,
                 password1,
                 host2,
                 user2,
                 password2,
                 in_path,
                 out_path,
                 move=False):
        self.move = move
        self.host1 = host1
        self.host2 = host2
        self.fxp_src = fxp_src(host1, user1, password1)
        LOGGER.debug('Logged in to source server %s', self.host1)
        self.fxp_dst = fxp_dst(host2, user2, password2)
        LOGGER.info('Logged in to destination server %s', self.host2)
        self.in_path = in_path
        self.in_dir, self.in_file = os.path.split(self.in_path)
        LOGGER.debug('cwd src host %s', self.in_dir)
        self.fxp_src.cwd(self.in_dir)
        self.fxp_src.ls()
        self.out_path = out_path
        self.out_dir, self.out_file = os.path.split(self.out_path)
        LOGGER.debug('cwd dest host %s', self.in_dir)
        self.fxp_dst.cwd(self.out_dir)
        self.out = os.path.join(self.out_dir, self.out_file)

    def close(self):
        """Close the connections"""
        try:
            self.fxp_src.close()
            LOGGER.info('closed connection to %s', self.host1)
        except ConnectionError as conn_e:
            LOGGER.error(conn_e)
            return False
        try:
            self.fxp_dst.close()
            LOGGER.info('closed connection to %s', self.host2)
        except ConnectionError as conn_e:
            LOGGER.error(conn_e)
            return False
        return True

    def run(self):
        '''The fxp shizel happens here'''
        try:
            self.fxp_src.fxp_to(self.in_file, self.fxp_dst, self.out_file)
            if self.move:
                LOGGER.info('delting source file %s', self.in_path)
                delete_from_ftp(del_file=self.in_file, FTPE=self.fxp_src)
            self.close()
        except ftpext.ftplib.error_temp as serveroverload_err:
            LOGGER.error(serveroverload_err)

    def __call__(self):
        LOGGER.info('transfer starting')
        self.run()
        return self.out

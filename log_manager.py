#!/usr/bin/python3

import logging
import logging.handlers
import syslog
import socket

def create_logger(name, log_level, syslog, syslog_server_ip):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if syslog:
        sh_formatter = logging.Formatter('%(name)s: %(message)s\n')
        sh = logging.handlers.SysLogHandler(address=(syslog_log_server_ip, 514),
        facility=syslog.LOG_INFO, socktype=socket.SOCK_STREAM)
        sh.setLevel(log_level)
        sh.setFormatter(sh_formatter)
        sh.append_nul = False
        logger.addHandler(sh)

    ch_formatter = logging.Formatter('%(name)s: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(ch_formatter)

    logger.addHandler(ch)

    return logger

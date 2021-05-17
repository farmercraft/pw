#!/usr/bin/python3

from common import *
from log_manager import *

log = create_logger("common_unit_test", logging.DEBUG, False, "")

def test_pm_pub_sub():
    pub = pm_pub(16000, log)
    sub1 = pm_sub("localhost", 16000, log)
    sub2 = pm_sub("localhost", 16000, log)

    pub_msg = pm_msg()
    pub_msg.str = "Test message from pub"
    log.info("=======> send test message")
    pub.send(pub_msg)

    sub1_msg = pm_msg()
    sub2_msg = pm_msg()
    log.info("=======> recv test message")

    sub1.recv(sub1_msg)
    sub1_msg.wait_complete()
    sub2.recv(sub2_msg)
    sub2_msg.wait_complete()

    log.info("=======> sub1 recv: " + sub1_msg.str)
    log.info("=======> sub2 recv: " + sub2_msg.str)

    del sub1
    del sub2
    del pub

def _main():
    log.info("+++++++> One pub -> Sub1 + Sub2")
    test_pm_pub_sub()
    log.info("+++++++> End")
    return

if __name__ == '__main__':
    _main()

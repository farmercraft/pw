#!/usr/bin/python3

from common import *
from log_manager import *
from source_manager import *

log = create_logger("source_manager_unit_test", logging.DEBUG, False, "")

def test_pm_udev_mon():
    pub = pm_udev_mon(log)
    sub1 = pm_sub("localhost", 16000, log)
    sub2 = pm_sub("localhost", 16000, log)

    sub1_msg = pm_msg()
    sub2_msg = pm_msg()

    log.info("=======> plug/unplug a block device")

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
    log.info("+++++++> Udev message -> Sub1 + Sub2")
    test_pm_udev_mon()
    log.info("+++++++> End")
    return

if __name__ == '__main__':
    _main()

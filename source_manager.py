#!/usr/bin/python3

import pyudev
import threading

from log_manager import *
from common import *

class udev_thread(threading.Thread):
    def __init__(self, log, pm_pub):
        threading.Thread.__init__(self)
        self.exit = False
        self.pm_pub = pm_pub
        self.log = log
        self.msg = pm_msg()

    def run(self):
        context = pyudev.Context()

        monitor = pyudev.Monitor.from_netlink(context)
        monitor.filter_by('block')

        for device in context.list_devices(subsystem="block"):
            if device and device.device_node.startswith("/dev/sd"):
                self.log.debug("device node: " + device.device_node)

        while not self.exit:
            device = monitor.poll(timeout=3)
            if device and device.device_node.startswith("/dev/sd"):
                self.msg.clear()

                if device.parent.device_node:
                    self.msg.str = "action: " + device.action + " node: " + device.device_node + " parent node: " + device.parent.device_node
                else:
                    self.msg.str = "action: " + device.action + " node: " + device.device_node + " parent node: " + str(None)

                self.pm_pub.send(self.msg)
                self.msg.wait_complete()

                self.log.debug("published msg: " + self.msg.str)

class pm_udev_mon:
    def __init__(self, log):
        self.log = log
        self.pm_pub = pm_pub(16000, log)
        self.thread = udev_thread(log, self.pm_pub)
        self.thread.start()

    def __del__(self):
        self.log.debug("pm_udev_mon is exiting")
        del self.pm_pub
        self.thread.exit = True
        self.thread.join()

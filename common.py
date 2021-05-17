#!/usr/bin/python3

import threading
import queue
import zmq

from log_manager import *

class pm_msg:
    def clear(self):
        self.complete = False
        self.str = None

    def wait_complete(self):
        self.mutex.acquire()

        while True:
            if not self.complete:
                self.cond.wait()
            else:
                break

        self.mutex.release()

    def __init__(self):
        self.mutex = threading.Lock()
        self.cond = threading.Condition(self.mutex)
        self.complete = False
        self.str = None

class pm_pub_thread(threading.Thread):
    def __init__(self, socket, log):
        threading.Thread.__init__(self)
        self.socket = socket
        self.mutex = threading.Lock()
        self.cond = threading.Condition(self.mutex)
        self.q = queue.Queue()
        self.exit = False
        self.log = log

    def run(self):
        self.log.debug('starting pm_pub thread')

        while not self.exit:
            self.mutex.acquire()

            while self.q.empty():
                self.log.debug('pub queue is empty. thread is going to sleep')
                self.cond.wait()
                self.log.debug('pm_pub thread is waking up, exit: '+ str(self.exit))
                if self.exit:
                    self.mutex.release()
                    return

            msg = self.q.get()

            self.mutex.release()

            self.log.debug('sending msg: ' + msg.str)
            self.socket.send_string(msg.str)

            msg.mutex.acquire()

            msg.complete = True
            msg.cond.notify()

            msg.mutex.release()

class pm_pub:
    def __init__(self, port, log):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*" + ":" + str(port))
        self.thread = pm_pub_thread(self.socket, log)
        self.thread.start()
        self.log = log

    def __del__(self):
        self.log.debug("pm pub exiting...")
        self.thread.exit = True
        self.thread.mutex.acquire()
        self.thread.cond.notify()
        self.thread.mutex.release()
        self.thread.join()
        del self.thread
        self.socket.close()
        self.context.term()

    def send(self, msg):
        self.thread.mutex.acquire()
        self.thread.q.put(msg)
        self.thread.cond.notify()
        self.thread.mutex.release()

class pm_sub_thread(threading.Thread):
    def __init__(self, socket, log):
        threading.Thread.__init__(self)
        self.socket = socket
        self.mutex = threading.Lock()
        self.cond = threading.Condition(self.mutex)
        self.q = queue.Queue()
        self.exit = False
        self.log = log

    def run(self):
        self.log.debug('starting pm_sub thread')

        while not self.exit:
            self.mutex.acquire()

            while self.q.empty():
                self.log.debug('sub queue is empty. thread is going to sleep')
                self.cond.wait()
                self.log.debug('pm_sub thread is waking up, exit: ' + str(self.exit))
                if self.exit:
                    self.mutex.release()
                    return

            msg = self.q.get()
            self.log.debug("start recving msg...")
            msg.str = self.socket.recv_string()
            self.log.debug("recving msg: " + msg.str)

            self.mutex.release()

            msg.mutex.acquire()
            msg.complete = True
            msg.cond.notify()
            msg.mutex.release()

class pm_sub:
    def __init__(self, ip, port, log):
        self.log = log
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect("tcp://" + ip + ":" + str(port))
        self.socket.subscribe("")
        self.thread = pm_sub_thread(self.socket, log)
        self.thread.start()

    def __del__(self):
        self.log.debug("pm sub exiting...")
        self.thread.exit = True
        self.thread.mutex.acquire()
        self.thread.cond.notify()
        self.thread.mutex.release()
        self.thread.join()
        del self.thread
        self.socket.close()
        self.context.term()

    def recv(self, msg):
        self.thread.mutex.acquire()
        self.thread.q.put(msg)
        self.thread.cond.notify()
        self.thread.mutex.release()

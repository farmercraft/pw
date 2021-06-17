#!/usr/bin/python3

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.filesystems import UnixFilesystem

import logging
import logging.handlers
import syslog
import socket
import os.path
import threading
import queue
import shutil
import psutil
import inotify.adapters
from pw_conf import *

#verbose debug message
pw_ftp_debug = True

def create_logger(log_level=logging.DEBUG):
    logger = logging.getLogger("pw ftp")
    logger.setLevel(log_level)

    sh_formatter = logging.Formatter(socket.gethostname() + ': %(name)s: %(message)s\n')
    sh = logging.handlers.SysLogHandler(address=(log_server_ip, 514),
            facility=syslog.LOG_INFO, socktype=socket.SOCK_STREAM)
    sh.setLevel(log_level)
    sh.setFormatter(sh_formatter)
    sh.append_nul = False

    ch_formatter = logging.Formatter(socket.gethostname() + ': %(name)s: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(ch_formatter)

    logger.addHandler(sh)
    logger.addHandler(ch)

    return logger

if pw_ftp_debug:
    log = create_logger(logging.DEBUG)
else:
    log = create_logger(logging.INFO)

def can_access_dir(dir_path):
    return os.path.isdir(dir_path) and os.access(dir_path, os.R_OK) and os.access(dir_path, os.W_OK) and os.access(dir_path, os.X_OK)

def auto_populate_root():
    partitions = psutil.disk_partitions()

    for p in partitions:
        if not "/dev/sd" and not "/dev/nvme" in p.device:
            continue

        if p.mountpoint == "/":
            continue

        if not p.fstype == "ext4" and not p.fstype == "xfs" and not p.fstype == "f2fs":
            continue

        dir_path = p.mountpoint + '/' + pw_autodetect_plot_dir

        if not can_access_dir(dir_path):
            log.info("Cannot access plot folder " + dir_path + " on " + p.mountpoint)
            continue

        s = psutil.disk_usage(p.mountpoint)
        t = int(s.total / 1024 / 1024 / 1024 / 1024)

        if t < pw_autodetect_min_dst_source_size:
            log.info("detect SRC source:" + p.mountpoint + " total: " + str(int(t)) + ' TB')
            path = ftp_root + "/" + dir_path.replace("/", "-")
            log.info("Creating " + dir_path + " -> " + path)
            os.symlink(dir_path, path)

def manually_populate_root():
    for dir_path in src_plots_dir.keys():
        path = ftp_root + "/" + os.path.basename(dir_path)
        log.info("Creating " + dir_path + " -> " + path)
        os.symlink(dir_path, path)

def recreate_ftp_root():
    os.system("rm -rf " + ftp_root)
    os.mkdir(ftp_root)
    log.debug("Create FTP root:" + ftp_root)

    if pw_autodetect_source:
        if pw_autodetect_home_source:
            home_path = os.environ['HOME']
            dir_path = os.path.join(home_path, pw_autodetect_plot_dir)
            log.info("Check home source: " + dir_path)
            if can_access_dir(dir_path):
                path = ftp_root + "/" + os.path.basename(dir_path)
                log.info("Creating " + dir_path + " -> " + path)
                os.symlink(dir_path, path)
            else:
                log.info("cannot access " + dir_path)

        auto_populate_root()
    else:
        manually_populate_root()

class ftp_server_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        log.debug('starting ftp server on ' + ftp_root);

        authorizer = DummyAuthorizer()

        authorizer.add_user("pw_ftp", "pw_ftp", ftp_root, perm='elradfmw')

        handler = FTPHandler
        handler.authorizer = authorizer
        handler.abstracted_fs = UnixFilesystem

        handler.banner = "pw_ftp is ready"

        address = ('', 2121)
        server = FTPServer(address, handler)

        server.max_cons = 256
        server.max_cons_per_ip = 5

        server.serve_forever()

def start_ftp_server():
    ftp_thread = ftp_server_thread()
    ftp_thread.start()

ftp_root = "/tmp/pw-" + str(os.getpid())
ftp_thread = None

def read_pid():
    pid = None

    if os.path.exists("/tmp/pw_ftp_pid.log"):
        f = open('/tmp/pw_ftp_pid.log', 'r')
        pid = int(f.read())
        f.close()

    return pid

def write_pid():
    f = open('/tmp/pw_ftp_pid.log', 'w')
    f.write(str(os.getpid()))
    f.close()

def check_running():
    pid = read_pid()
    if pid:
        running = psutil.pids()
        if pid in running:
            raise RuntimeError("pw_ftp is already running")
        else:
            write_pid()
    else:
        write_pid()

def _main():
    check_running()
    recreate_ftp_root()
    start_ftp_server()

    i = inotify.adapters.Inotify()

    i.add_watch(".")

    for event in i.event_gen():
        if event is None:
            continue

        (header, type_names, watch_path, filename) = event

        if "IN_CLOSE_WRITE" not in type_names and "IN_MOVED_TO" not in type_names:
            continue

        if filename != "pw_conf.py":
            continue

        recreate_ftp_root()

if __name__ == '__main__':
    _main()

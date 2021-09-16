#!/usr/bin/python3

import logging
import logging.handlers
import syslog
import socket
import os.path
import threading
import queue
import shutil
import psutil
import time

import inotify.adapters
from pw_conf import *

#test work dispatch with ping-pong prio
pw_test_prio = False

#dryrun, won't touch the actual file
pw_test_dryrun = False

#trigger the wq stall
pw_test_wq_stall = False

#verbose debug message
pw_debug = True
pw_debug_dump_file_list = False

def create_logger(log_level=logging.DEBUG):
    logger = logging.getLogger("plot watcher")
    logger.setLevel(log_level)

    ch_formatter = logging.Formatter(socket.gethostname() + ': %(name)s: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(ch_formatter)

    logger.addHandler(ch)

    if log_server_ip:
        sh_formatter = logging.Formatter(socket.gethostname() + ': %(name)s: %(message)s\n')
        sh = logging.handlers.SysLogHandler(address=(log_server_ip, 514),
                facility=syslog.LOG_INFO, socktype=socket.SOCK_STREAM)
        sh.setLevel(log_level)
        sh.setFormatter(sh_formatter)
        sh.append_nul = False
        logger.addHandler(sh)
    else:
        logger.info("Remote syslog logging is disabled")

    return logger

if pw_debug:
    log = create_logger(logging.DEBUG)
else:
    log = create_logger(logging.INFO)

class plot_file:
    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.full_path = os.path.join(path, name)
        self.actual_size = int(os.path.getsize(self.full_path))
        self.size = int(self.actual_size / 1024 / 1024 / 1024)

class plot_source:
    def get_plot_file_list(self, dir_path, file_dict):
        list = os.listdir(dir_path)
        for name in list:
            full_path = os.path.join(dir_path, name)
            if os.path.isfile(full_path) and name.endswith(".plot"):
                if pw_autodetect_mv_mode:
                    t1 = os.path.getctime(full_path)
                    t = os.path.getmtime(full_path)

                    if int(t1) > int(t):
                        t1 = t

                    t2 = time.mktime(time.strptime(pw_autodetect_mv_start, '%Y-%m-%d %H:%M:%S'))
                    if int(t1) > int(t2):
                        file_dict[name] = plot_file(dir_path, name)
                    else:
                        log.debug("pw_autodetect_mv_mode: skip file: " + full_path)
                else:
                    file_dict[name] = plot_file(dir_path, name)

    def __init__(self, mountpoint, dir, src):
        self.mountpoint = mountpoint
        self.dir = dir
        self.ready = False
        self.avail = 0
        self.copying_size = 0
        self.avail_after_copy = 0
        self.file_dict = {}
        self.file_copying_dict = {}
        self.src = src
        self.mutex = threading.Lock()
        self.get_plot_file_list(dir, self.file_dict)
        self.update_status()
        self.debug_set_full = False; #Debug

    def lock(self):
        self.mutex.acquire();

    def unlock(self):
        self.mutex.release();

    def full(self):
        if self.debug_set_full:
            return True;
        else:
            return self.avail < 110

    def full_after_copy(self):
        if self.debug_set_full:
            return True;
        else:
            return self.avail_after_copy < 110

    def add_copying_file(self, f):
        self.file_copying_dict[f.name] = f
        self.copying_size = self.copying_size + f.size
        self.update_status()

    def del_copying_file(self, f):
        self.file_copying_dict.pop(f.name)
        self.copying_size = self.copying_size - f.size
        self.update_status()

    def add_file(self, f):
        self.file_dict[f.name] = f

    def del_file(self, f):
        self.file_dict.pop(f.name)

    def has_file(self, f):
        if self.file_dict.__contains__(f.name) and self.file_dict[f.name].actual_size == f.actual_size:
            r = True
        else:
            r = False
        return r;

    def has_copying_file(self, f):
        if self.file_copying_dict.__contains__(f.name) and self.file_copying_dict[f.name].actual_size == f.actual_size:
            r = True
        else:
            r = False
        return r;

    def update_status(self):
        self.ready = os.path.ismount(self.mountpoint) and can_access_dir(self.dir)
        if not self.ready:
            return;

        info = os.statvfs(self.dir)
        self.avail = int(info.f_frsize * info.f_bavail / 1024 / 1024 / 1024)

        if self.src:
            self.avail_after_copy = self.avail + self.copying_size
        else:
            self.avail_after_copy = self.avail - self.copying_size

    def dump(self, tag):
        if self.src:
            log.debug('[ ' + tag + ' ] ' + '======= SRC plot source dump =======')
        else:
            log.debug('[ ' + tag + ' ] ' + '======= DST plot source dump =======')
        log.debug('[ ' + tag + ' ] ' + 'plot dir: ' + self.dir + ' mountpoint: ' + self.mountpoint + ' [avail] ' + str(self.avail) + 'G'
                + ' [ready] ' + str(self.ready) + ' [copying] ' +
                str(self.copying_size) + 'G [full] ' + str(self.full()) + ' [ full after copy ]' + str(self.full_after_copy()));
        log.debug('[ ' + tag + ' ] ' + 'plot dir: ' + self.dir + ' file dict:')
        if pw_debug_dump_file_list:
            log.debug(self.file_dict)
        log.debug('[ ' + tag + ' ] ' + 'plot dir: ' + self.dir + ' file copying dict:')
        if pw_debug_dump_file_list:
            log.debug(self.file_copying_dict)

class work_item:
    def __init__(self, f, in_source, prio):
        self.plot_file = f
        self.in_source = in_source
        self.complete = False
        self.prio = prio

    def dump(self, tag):
        log.debug('[ ' + tag + ' ] ' + '======= work item dump =======')
        log.debug('[ ' + tag + ' ] ' + 'work item: ' + self.plot_file.full_path + ' size: ' + str(self.plot_file.size) + 'G' + ' actual size: ' + str(self.plot_file.actual_size) + 'B' + ' complete ' + self.complete);

class work_queue:
    def __init__(self, out_source):
        self.stalled = False
        self.lo_q = queue.Queue()
        self.hi_q = queue.Queue()
        self.mutex = threading.Lock()
        self.cond = threading.Condition(self.mutex)
        self.copying_size = 0
        self.out_source = out_source
        self.file_copying_dict = {}

        self.debug_prio = 3 #debug

    def lock(self):
        self.mutex.acquire()

    def unlock(self):
        self.mutex.release()

    def enqueue(self, item):
        if not item.prio:
            self.hi_q.put(item)
        else:
            self.lo_q.put(item)

        log.debug('enqueue work: ' + item.plot_file.full_path + ' into ' + self.out_source.dir)
        self.copying_size = self.copying_size + item.plot_file.actual_size
        self.file_copying_dict[item.plot_file.name] = item
        self.dump('wq_enqueue')
        self.cond.notify()

    def dequeue(self):
        try:
            if not self.hi_q.empty():
                item = self.hi_q.get()
            else:
                item = self.lo_q.get()
        except queue.Full:
            log.debug('dequeue: wq is empty? ' + self.out_source.dir)
            return None

        log.debug('dequeue work: ' + item.plot_file.full_path + ' into ' + self.out_source.dir)
        self.dump('wq_dequeue')
        return item

    def complete(self, item):
        log.debug('complete work: ' + item.plot_file.full_path + ' into ' + self.out_source.dir)
        self.copying_size = self.copying_size - item.plot_file.actual_size
        self.file_copying_dict.pop(item.plot_file.name)
        self.dump('wq_complete')

    def stall(self):
        log.debug('workqueue: ' + self.out_source.dir + ' stall , retiring all work items')
        self.stalled = True;
        self.hi_q.queue.clear()
        self.lo_q.queue.clear()

        for name in list(self.file_copying_dict.keys()):
            item = self.file_copying_dict[name]
            self.complete(item)

            src = item.in_source
            dst = self.out_source

            src.del_copying_file(item.plot_file)
            src.add_file(item.plot_file)
            dst.del_copying_file(item.plot_file)

            del item

        self.dump('wq_stall')

    def dump(self, tag):
        log.debug('[ ' + tag + ' ] ' + '======= work queue dump =======')
        log.debug('[ ' + tag + ' ] ' + 'work queue ' + self.out_source.dir + ' stalled ' + str(self.stalled) + ' copying size ' + str(self.copying_size))
        if pw_debug_dump_file_list:
            log.debug(self.file_copying_dict)

def can_access_dir(dir_path):
    return os.path.isdir(dir_path) and os.access(dir_path, os.R_OK) and os.access(dir_path, os.W_OK) and os.access(dir_path, os.X_OK)

def can_access_file(f):
    return os.access(f.full_path, os.R_OK) and os.access(f.full_path, os.W_OK) and os.access(f.full_path, os.F_OK)

def show_all_plot_sources():
    for path in src_plot_source_dict.keys():
        plot = src_plot_source_dict[path]
        log.info('src plot: ' + ' [dir] ' + plot.dir + ' [avail] ' + str(plot.avail) + 'G'
                + ' [ready] ' + str(plot.ready) + ' [full] ' + str(plot.full()));

    for path in dst_plot_source_dict.keys():
        plot = dst_plot_source_dict[path]
        log.info('dst plot: ' + ' [dir] ' + plot.dir + ' [avail] ' + str(plot.avail) + 'G'
                + ' [ready] ' + str(plot.ready) + ' [full] ' + str(plot.full()));

def manually_populate_plot_sources(src, dst):
    for dir_path in src.keys():
        mp = src[dir_path]
        plot = plot_source(mp, dir_path, True)
        plot.dump('manually_populate_plot_source');

        src_plot_source_dict[dir_path] = plot

    for dir_path in dst.keys():
        mp = dst[dir_path]
        plot = plot_source(mp, dir_path, False)
        plot.dump('manually_populate_plot_source');

        dst_plot_source_dict[dir_path] = plot

def auto_populate_plot_sources(src, dst):
    partitions = psutil.disk_partitions()

    for p in partitions:
        if not "/dev/sd" in p.device and not "/dev/nvme" in p.device:
            continue

        if p.mountpoint == "/":
            continue

        if not p.fstype == "ext4" and not p.fstype == "xfs" and not p.fstype == "fuseblk":
            continue

        dir_path = p.mountpoint + '/' + pw_autodetect_plot_dir

        if not can_access_dir(dir_path):
            log.info("Cannot access plot folder " + dir_path + " on " + p.mountpoint)
            continue

        s = psutil.disk_usage(p.mountpoint)
        t = int(s.total / 1024 / 1024 / 1024 / 1024)

        if t >= pw_autodetect_min_dst_source_size:
            log.info("detect DST source:" + p.mountpoint + " total: " + str(int(t)) + ' TB')
            plot = plot_source(p.mountpoint, dir_path, False)
            plot.dump('auto_populate_plot_source');

            dst_plot_source_dict[dir_path] = plot
            dst[dir_path] = p.mountpoint
        else:
            os.system("sudo blockdev -v --setra 16384 " + p.device)
            log.info("detect SRC source:" + p.mountpoint + " total: " + str(int(t)) + ' TB')
            plot = plot_source(p.mountpoint, dir_path, True)
            plot.dump('auto_populate_plot_source');

            src_plot_source_dict[dir_path] = plot
            src[dir_path] = p.mountpoint

def cmp_free_from_part_info(info):
    return info.free

def add_dst_source(mp, dir_path, t):
    log.info("Add DST source:" + mp + " total: " + str(int(t)) + ' TB')
    plot = plot_source(mp, dir_path, False)
    plot.dump('add_dst_plot_source');

    dst_plot_source_dict[dir_path] = plot
    dst_plots_dir[dir_path] = mp

def add_src_source(mp, dir_path, t):
    log.info("Add SRC source:" + mp + " total: " + str(int(t)) + ' TB')
    plot = plot_source(mp, dir_path, True)
    plot.dump('add_src_plot_source');

    src_plot_source_dict[dir_path] = plot
    src_plots_dir[dir_path] = mp

class part_info:
    def __init__(self, free, used, total, dir_path, mp):
        self.free = free
        self.used = used
        self.total = total
        self.dir_path = dir_path
        self.mp = mp

def auto_populate_plot_sources_merge_mode(src, dst):
    partitions = psutil.disk_partitions()
    source = []
    plot_file_size = 103

    for p in partitions:
        if not "/dev/sd" in p.device and not "/dev/nvme" in p.device:
            continue

        if p.mountpoint == "/":
            continue

        if not p.fstype == "ext4" and not p.fstype == "xfs" and not p.fstype == "fuseblk":
            continue

        dir_path = p.mountpoint + '/' + pw_autodetect_plot_dir

        if not can_access_dir(dir_path):
            log.info("Cannot access plot folder " + dir_path + " on " + p.mountpoint)
            continue

        s = psutil.disk_usage(p.mountpoint)
        t = int(s.total / 1024 / 1024 / 1024 / 1024)

        if t < pw_autodetect_merge_disk_min_size:
            continue

        f = int(s.free / 1024 / 1024 / 1024)
        if f < plot_file_size:
            continue

        u = int(s.used / 1024 / 1024 / 1024)
        source.append(part_info(f, u, t, dir_path, p.mountpoint))

    if not source:
        raise RuntimeError("Cannot find any available disks for merging")

    source.sort(key=cmp_free_from_part_info)
    log.debug(source)

    while True:
        l = len(source)

        if l == 1:
            break

        dst = source[0]
        src = source[l-1]

        log.debug("src" + " free " + str(src.free) + " used " + str(src.used) + " dir_path " + src.dir_path + " mp " + src.mp)
        log.debug("dst" + " free " + str(dst.free) + " used " + str(dst.used) + " dir_path " + dst.dir_path + " mp " + dst.mp)

        if dst.free >= src.used:
            dst.free = dst.free - src.used
            source.remove(src)
            log.debug("add src " + src.dir_path)
            add_src_source(src.mp, src.dir_path, src.total)
            src = None
            continue
        else:
            source.remove(dst)

            if dst.free > plot_file_size:
                src.used = src.used - dst.free

            log.debug("add dst" + dst.dir_path)
            add_dst_source(dst.mp, dst.dir_path, dst.total)
            dst = None
            continue;

    if src:
        log.debug("add src " + src.dir_path)
        add_src_source(src.mp, src.dir_path, src.total)

    if dst:
        log.debug("add dst" + dst.dir_path)
        add_dst_source(dst.mp, dst.dir_path, dst.total)

def populate_plot_source():
    if pw_autodetect_source:
        src_plots_dir.clear()
        dst_plots_dir.clear()
        if pw_autodetect_home_source:
            home_path = os.environ['HOME']
            dir_path = os.path.join(home_path, pw_autodetect_plot_dir)
            log.info("Check home source: " + dir_path)
            if can_access_dir(dir_path):
                mp = "/"
                plot = plot_source(mp, dir_path, True)
                src_plot_source_dict[dir_path] = plot
                src_plots_dir[dir_path] = mp
                plot.dump('auto_populate_plot_source');
            else:
                log.info("cannot access " + dir_path)
        if pw_autodetect_merge_mode:
            auto_populate_plot_sources_merge_mode(src_plots_dir, dst_plots_dir)
        else:
            auto_populate_plot_sources(src_plots_dir, dst_plots_dir)
        if not src_plots_dir:
            raise RuntimeError("Cannot find any available SRC source")
        if not dst_plots_dir:
            raise RuntimeError("Cannot find any available DST source")
    else:
        manually_populate_plot_sources(src_plots_dir, dst_plots_dir)

    show_all_plot_sources()

class worker_thread(threading.Thread):
    def __init__(self, wq):
        threading.Thread.__init__(self)
        self.wq = wq

    def run(self):
        log.debug('starting work thread for ' + self.wq.out_source.dir);

        while not exit_flag:
            self.wq.lock()

            while self.wq.lo_q.empty() and self.wq.hi_q.empty():
                all_full = True;

                for dir_path in dst_plot_source_dict.keys():
                    dst_plot_source_dict[dir_path].lock()

                for dir_path in dst_plot_source_dict.keys():
                    dst = dst_plot_source_dict[dir_path]
                    if not dst.full() or dst.file_copying_dict:
                        all_full = False
                        break;

                if all_full:
                    log.info('[ ALL FULL ] All the dst sources are full, exit!')
                    os._exit(1)
                elif self.wq.out_source.full_after_copy():
                    log.info('[ FULL ] ' + self.wq.out_source.dir + ' is full ')
                    kick_dispatcher()
                else:
                    kick_dispatcher()

                for dir_path in dst_plot_source_dict.keys():
                    dst_plot_source_dict[dir_path].unlock()

                log.debug('worker thread ' + self.wq.out_source.dir + ' is going to sleep')
                self.wq.cond.wait()

            log.debug('thread ' + self.wq.out_source.dir +' wake up')

            item = self.wq.dequeue();
            self.wq.unlock()

            process_work_item(self.wq, item)
            complete_work_item(self.wq, item)

def populate_workers():
    for dir_path in dst_plot_source_dict.keys():
        dst = dst_plot_source_dict[dir_path]

        wq = work_queue(dst)
        thread = worker_thread(wq)
        thread.start()
        worker_thread_list.append(thread)

class dispatcher_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.event.clear()

    def run(self):
        log.debug('starting dispatch thread');

        while not exit_flag:
            self.event.wait()

            log.debug('dispatch thread wake up')

            self.event.clear()

            lock_workqueues_and_sources()
            process_pending_sources()
            unlock_workqueues_and_sources()

def populate_dispatcher():
    thread = dispatcher_thread()
    thread.start()
    dispatcher_thread_list.append(thread)

def kick_dispatcher():
    for t in dispatcher_thread_list:
        t.event.set()

def lock_workqueues_and_sources():
    for t in worker_thread_list:
        t.wq.lock();

    for dir_path in dst_plot_source_dict.keys():
        dst_plot_source_dict[dir_path].lock()

    for dir_path in src_plot_source_dict.keys():
        src_plot_source_dict[dir_path].lock()

def unlock_workqueues_and_sources():
    for dir_path in src_plot_source_dict.keys():
        src_plot_source_dict[dir_path].unlock()

    for dir_path in dst_plot_source_dict.keys():
        dst_plot_source_dict[dir_path].unlock()

    for t in worker_thread_list:
        t.wq.unlock();

def file_in_source_dict(f, d):
    for dir_path in d.keys():
        source = d[dir_path]
        if source.has_file(f):
            return True

        if source.has_copying_file(f):
            return True

    return False

def process_pending_sources():
    src_plot_iter = {}

    for dir_path in src_plot_source_dict.keys():
        src = src_plot_source_dict[dir_path]
        src_plot_iter[dir_path] = iter(list(src.file_dict.keys()))

    while len(src_plot_iter):
        for dir_path in list(src_plot_iter.keys()):
            log.debug('process_pending_sources: dir: ' + dir_path)
            src = src_plot_source_dict[dir_path]
            if src.file_copying_dict:
                del src_plot_iter[dir_path]
                continue;

            it = src_plot_iter[dir_path]
            try:
                file_name = next(it)
                log.debug('process_pending_sources: file: ' + file_name)
            except StopIteration:
                del src_plot_iter[dir_path]
                continue

            f = src.file_dict[file_name]
            if file_in_source_dict(f, dst_plot_source_dict):
                log.info('Found the ' + f.full_path + ' in dst sources, skipped')
                src.del_file(f)
            else:
                if not dispatch_file(f):
                    return False
    return True

def process_work_item(wq, item):
    log.debug('process work item')

    if pw_test_dryrun:
        log.info('[ TEST DRY RUN ] Moving ' + item.plot_file.full_path + ' to ' + wq.out_source.dir + '...')
        item.complete = True;
    elif pw_test_wq_stall:
        log.info('[ TEST WQ_STALL ] Moving ' + item.plot_file.full_path + ' to ' + wq.out_source.dir + '...')
        item.complete = False;
    else:
        log.info('Moving ' + item.plot_file.full_path + ' to ' + wq.out_source.dir)

        try:
            shutil.move(item.plot_file.full_path, os.path.join(wq.out_source.dir, item.plot_file.name))
        except:
            log.info('Fail to move ' + item.plot_file.full_path + ' to ' + wq.out_source.dir)

            try:
                if can_access_file(item.plot_file):
                    os.remove(os.path.join(wq.out_source.dir, item.plot_file.name))
                else:
                    log.info('SRC file ' + item.plot_file.full_path + ' is removed when moving failed! Skip deleting incomplete DST file.')
            except OSError:
                    pass

            item.complete = False;
        else:
            item.complete = True;
            log.info('Done ' + item.plot_file.full_path)

def complete_work_item(wq, item):
    log.debug('complete work item')

    src = item.in_source;
    dst = wq.out_source;

    wq.lock();
    dst.lock();
    src.lock();

    wq.complete(item)

    src.del_copying_file(item.plot_file)
    dst.del_copying_file(item.plot_file)

    if not item.complete:
        log.info('work: plot file: ' + item.plot_file.full_path + ' is not complete')
        if not can_access_file(item.plot_file):
            log.info('plot file ' + item.plot_file.full_path + ' can not be accessed, skip it')
        else:
            log.info('plot file can be accessed, but dst can not, workqueue stall')
            src.add_file(item.plot_file)
            wq.stall()
    else:
        dst.add_file(item.plot_file)

    del item

    src.dump('complete_work_item')
    dst.dump('complete_work_item')

    if pw_test_wq_stall:
        dst.debug_set_full = True;
    
    dst.update_status();

    if not dst.ready:
        log.info('dst dir ' + dst.dir + ' is not ready, workqueue stall')
        wq.stall()
    elif dst.full():
        log.info('dst dir ' + dst.dir + ' is full, workqueue stall')
        wq.stall()

    src.unlock();
    dst.unlock();
    wq.unlock();

def dispatch_file(f):
    log.debug('dispatch file ' + f.full_path)

    src = src_plot_source_dict[f.path];

    best = None

    for t in worker_thread_list:
        wq = t.wq;

        wq.dump('dispatch_file')

        if wq.stalled:
            continue

        if wq.out_source.full_after_copy():
            continue;

        if best is None:
            best = t
            continue

        if wq.copying_size < best.wq.copying_size:
            best = t

    if best is None:
        log.debug('fail to dispatch the work ' + f.name)
        log.debug('no available workqueue, stop dispatch.')
        return False

    wq = best.wq
    dst = wq.out_source;

    log.debug('pick the idlest q: ' + dst.dir)

    if not pw_test_prio:
        if src.full():
            log.debug('work - high priority')
            prio = 0
        else:
            log.debug('work - normal priority')
            prio = 3
    else:
        log.debug('[ TEST PRIO ] - ' + str(wq.debug_prio))
        prio = wq.debug_prio

        if prio == 3:
            wq.debug_prio = 0
        else:
            wq.debug_prio = 3

    src.del_file(f)
    src.add_copying_file(f)

    dst.add_copying_file(f)

    src.dump('dispatch_file')
    dst.dump('dispatch_file')

    item = work_item(f, src, prio)
    wq.enqueue(item);

    return True;

def read_pid():
    pid = None

    if os.path.exists("/tmp/pw_pid.log"):
        f = open('/tmp/pw_pid.log', 'r')
        pid = int(f.read())
        f.close()

    return pid

def write_pid():
    f = open('/tmp/pw_pid.log', 'w')
    f.write(str(os.getpid()))
    f.close()

def check_running():
    pid = read_pid()
    if pid:
        running = psutil.pids()
        if pid in running:
            raise RuntimeError("pw_python is already running")
        else:
            write_pid()
    else:
        write_pid()

src_plot_source_dict = {}
dst_plot_source_dict = {}

worker_thread_list = []
dispatcher_thread_list = []
exit_flag = 0

def _main():
    check_running()
    populate_plot_source()
    populate_workers()
    populate_dispatcher()

    kick_dispatcher()

    i = inotify.adapters.Inotify()

    for path in src_plots_dir.keys():
        log.debug("add watch: " + path)
        i.add_watch(path)

    for event in i.event_gen():
        if event is None:
            continue

        (header, type_names, watch_path, filename) = event

        if not filename.endswith(".plot"):
            continue

        if not os.path.isfile(watch_path + '/' + filename):
            continue

        if "IN_CLOSE_WRITE" not in type_names and "IN_MOVED_TO" not in type_names:
            continue

        log.debug('inotify event: watch_path: ' + watch_path + ' file name ' + filename)

        lock_workqueues_and_sources()

        f = plot_file(watch_path, filename)

        if file_in_source_dict(f, dst_plot_source_dict):
            del f
        else:
            src = src_plot_source_dict[f.path];
            src.add_file(f);

        unlock_workqueues_and_sources()

        kick_dispatcher()


if __name__ == '__main__':
    _main()

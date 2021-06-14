#!/bin/sh

echo `hostname` | sudo -S apt-get -y install python3-pip
echo `hostname` | sudo -S pip3 install inotify psutil
echo `hostname` | sudo -S pip3 pyftpdlib wget lftp

touch pw_conf.py
./pw_ftp.py &
./pw_python.py

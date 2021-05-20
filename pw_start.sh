#!/bin/sh

echo `hostname` | sudo -S apt-get -y install python3-pip

pip3 install inotify
pip3 install psutil
pip3 install socketserver

./pw_python.py

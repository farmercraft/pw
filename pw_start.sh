#!/bin/sh

echo `hostname` | sudo -S apt-get -y install python3-pip

pip3 install inotify
pip3 install psutil

./pw_python.py

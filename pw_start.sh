#!/bin/sh

echo `hostname` | sudo -S apt-get -y install python3-pip

pip3 install inotify

./pw_python.py

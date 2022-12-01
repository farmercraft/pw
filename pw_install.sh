#!/bin/sh

git clone https://github.com/plotmanager/pw.git
sudo cp -r pw /usr/local/pw
sudo ln -s /usr/local/pw/pw_python.py  /usr/local/bin/pw
sudo chmod a+rwx /usr/local/bin/pw
echo `hostname` | sudo -S apt-get -y install python3-pip
echo `hostname` | sudo -S pip3 install inotify psutil


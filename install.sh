#!/bin/bash
set -e
sudo yum install -y python-pip
sudo pip2.7 install virtualenv

virtualenv -p python2.7 venv2.7
source venv2.7/bin/activate
pip install -e .[cwl,mesos]

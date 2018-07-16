#!/bin/bash
set -e
sudo yum install -y python-pip python-devel
sudo pip2.7 install virtualenv

virtualenv -p python2.7 venv2.7
source venv2.7/bin/activate
git clone https://github.com/heliumdatacommons/schema_salad.git
pip install ./schema_salad/
pip install chronos-python
pip install -e .[cwl,mesos,aws]

sudo ln -s $(pwd)/_toil_worker.sh /usr/bin/_toil_worker

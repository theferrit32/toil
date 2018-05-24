# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#

def _gridengineBatchSystemFactory():
    from toil.batchSystems.gridengine import GridEngineBatchSystem
    return GridEngineBatchSystem

def _chronosBatchSystemFactory():
    from toil.batchSystems.chronos import ChronosBatchSystem
    return ChronosBatchSystem

def _parasolBatchSystemFactory():
    from toil.batchSystems.parasol import ParasolBatchSystem
    return ParasolBatchSystem

def _lsfBatchSystemFactory():
    from toil.batchSystems.lsf import LSFBatchSystem
    return LSFBatchSystem

def _singleMachineBatchSystemFactory():
    from toil.batchSystems.singleMachine import SingleMachineBatchSystem
    return SingleMachineBatchSystem

def _mesosBatchSystemFactory():
    from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
    return MesosBatchSystem

def _slurmBatchSystemFactory():
    from toil.batchSystems.slurm import SlurmBatchSystem
    return SlurmBatchSystem

def _torqueBatchSystemFactory():
    from toil.batchSystems.torque import TorqueBatchSystem
    return TorqueBatchSystem

def _htcondorBatchSystemFactory():
    from toil.batchSystems.htcondor import HTCondorBatchSystem
    return HTCondorBatchSystem


_DEFAULT_REGISTRY = {
    'chronos'        : _chronosBatchSystemFactory,
    'parasol'        : _parasolBatchSystemFactory,
    'singleMachine'  : _singleMachineBatchSystemFactory,
    'single_machine' : _singleMachineBatchSystemFactory,
    'gridEngine'     : _gridengineBatchSystemFactory,
    'gridengine'     : _gridengineBatchSystemFactory,
    'lsf'            : _lsfBatchSystemFactory,
    'LSF'            : _lsfBatchSystemFactory,
    'mesos'          : _mesosBatchSystemFactory,
    'Mesos'          : _mesosBatchSystemFactory,
    'slurm'          : _slurmBatchSystemFactory,
    'Slurm'          : _slurmBatchSystemFactory,
    'torque'         : _torqueBatchSystemFactory,
    'Torque'         : _torqueBatchSystemFactory,
    'htcondor'       : _htcondorBatchSystemFactory,
    'HTCondor'       : _htcondorBatchSystemFactory
    }

_UNIQUE_NAME = {
    'parasol',
    'singleMachine',
    'gridEngine',
    'LSF',
    'Mesos',
    'Slurm',
    'Torque',
    'HTCondor'
        }

_batchSystemRegistry = _DEFAULT_REGISTRY.copy()
_batchSystemNames = set(_UNIQUE_NAME)

def addBatchSystemFactory(key, batchSystemFactory):
    _batchSystemNames.add(key)
    _batchSystemRegistry[key] = batchSystemFactory

def batchSystemFactoryFor(batchSystem):
    return _batchSystemRegistry[batchSystem ]

def defaultBatchSystem():
    return 'singleMachine'

def uniqueNames():
    return list(_batchSystemNames)

def batchSystems():
    list(set(_batchSystemRegistry.values()))

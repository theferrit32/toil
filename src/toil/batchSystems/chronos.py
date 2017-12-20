from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import logging

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem

logger = logging.getLogger(__name__)

class ChronosBatchSystem(AbstractBatchSystem):
    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        #super(ChronosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        super(ChronosBatchSystem, self).__init__()
        logger.info("config: {}".format(config))

    def setUserScript(self, userScript):
        raise NotImplementedError()
    def issueBatchJob(self, jobNode):
        logger.info("jobNode: {}".format(vars(jobNode)))
        logger.info("jobNode command: {}".format(jobNode.command))
        return 0
    def killBatchJobs(self, jobIDs):
        raise NotImplementedError()
    def getIssuedBatchJobIDs(self):
        return []
    def getRunningBatchJobIDs(self):
        return {}
    def getUpdatedBatchJob(self, maxWait):
        return None
    def shutdown(self):
        logger.info("shutdown called")
        pass
    def setEnv(self, name, value=None):
        raise NotImplementedError()

    @classmethod
    def getRescueBatchJobFrequency(cls):
        raise NotImplementedError()
    @classmethod
    def setOptions(cls, setOption):
        pass

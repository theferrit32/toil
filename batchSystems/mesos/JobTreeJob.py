__author__ = 'CJ'
from jobTree.batchSystems.mesos.ResourceSummary import ResourceSummary

class JobTreeJob:
    # describes basic job tree job, with various resource requirements.
    def __init__(self, jobID, cpu, memory, command, cwd):
        self.resources = ResourceSummary(memory=memory, cpu=cpu)
        self.jobID = jobID
        self.command = command
        self.cwd = cwd
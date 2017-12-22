from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import logging
import urllib
from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem, BatchSystemSupport
import chronos

logger = logging.getLogger(__name__)

class ChronosBatchSystem(BatchSystemSupport): #TODO look at how singleMachine batch system does clean up/shudown
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

        self.issued_jobs = []
        self.jobStoreID = None

    def setUserScript(self, userScript):
        raise NotImplementedError()


    """
    Currently returning the string name of the chronos job instead of an int id
    """
    def issueBatchJob(self, jobNode):
        # store jobStoreID as a way to reference this batch of jobs
        self.jobStoreID = jobNode.jobStoreID
        logger.info("jobNode: {}".format(vars(jobNode)))
        logger.info("jobNode command: {}".format(jobNode.command))
        client = chronos.connect("stars-app.renci.org/chronos", proto="https")
        job_name = jobNode.jobName.split("/")[-1] + "-" + jobNode.jobStoreID
        job_name = job_name.replace("/", "-")
        job = {
            "name": job_name,
            "command": ( # replace /path/to/_toil_worker [args] with /path/to/workerscriptlauncher [args]
                "/opt/toil/_toil_worker.sh "
                + " ".join(jobNode.command.split(" ")[1:]) # args after original _toil_worker
                ),
            "owner": "nobody@domain.ext",
            "schedule": "R//P1Y",
            "epsilon": "PT15M",
            "execute_now": True,
            "shell": True,
            "disabled": False,
            "runAsUser": "evryscope",
            "constraints": [
                [
                    "hostname", "EQUALS", "stars-dw0.edc.renci.org"
                ]
            ]
        }
        logger.info("Creating job in chronos: \n%s" % job)
        # encode the field values
        #for field in job:
        #    if isinstance(job[field], basestring):
        #        encoded_value = urllib.quote_plus(job[field])
        #        job[field] = encoded_value
        #logger.info("Encoded job: %s" % job)
        # TODO handle return value here
        """j2 = {
            "name": "asdf",
            "command": "ls",
            "owner": "nobody@domain.ext",
            "schedule": "R//P1Y",
            "epsilon": "PT15M",
            "execute_now": True,
            "shell": True,
            "disabled": False,
            "runAsUser": "evryscope"
        }"""
        ret = client.add(job)
        logger.info("Chronos ret: %s" % ret)
        #self.issued_jobs.append(job)
        #return self.issued_jobs.index(jobs)
        return job["name"]


    def killBatchJobs(self, jobIDs):
        # TODO
        pass

    """
    Currently returning the string name of the jobs as the ids, not int ids
    Matches ids from issueBatchJob
    """
    def getIssuedBatchJobIDs(self):
        if not self.jobStoreID:
            return []
        client = chronos.connect("stars-app.renci.org/chronos")
        jobs = client.search(name=self.jobStoreID)
        ids = [j["name"] for j in jobs]
        return ids

    """
    Returns {<jobname>: 0, ...}
    # TODO fill in the 0 with the number of seconds the job has been running.
    Requires interacting with the Mesos API, somewhere in here:
        http://mesos.apache.org/documentation/latest/endpoints/
    """
    def getRunningBatchJobIDs(self):
        if not self.jobStoreID:
            return {}
        client = chronos.connect("stars-app.renci.org/chronos")
        jobs = client.search(name=self.jobStoreID)

        jobs_summary = client._call("/scheduler/jobs/summary")["jobs"]
        running_jobs = {}
        for j in jobs:
            # look for this job in the job summary list (which has the state and status fields)
            for summary in jobs_summary:
                if summary["name"] == j["name"]:
                    # add status field from summary to job obj
                    j["status"] = summary["status"]
                    j["state"] = summary["state"]
                    if "running" in j["state"]:
                        running_jobs[j["name"]] = 0
        return running_jobs

    def getUpdatedBatchJob(self, maxWait):
        #TODO
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

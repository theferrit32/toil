from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import logging
import urllib
from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem, BatchSystemSupport
import chronos
import time
from threading import Thread
from six.moves.queue import Empty, Queue

logger = logging.getLogger(__name__)

class ChronosBatchSystem(BatchSystemSupport):
    #TODO look at how singleMachine batch system does clean up/shutdown

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        #super(ChronosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        super(ChronosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        logger.info("config: {}".format(config))
        """
        List of jobs in format:
        {
            "name": <str>,
            ... chronos job fields
            "issued_time": <Date>,
            "status": <success|failed>,
            #"state": <idle|running|..>
        }
        """
        self.issued_jobs = []
        self.updated_jobs_queue = Queue()
        self.jobStoreID = None
        self.running = True
        self.worker = Thread(target=self.updated_job_worker, args=())
        self.worker.start()

    def updated_job_worker(self):
        # poll chronos api and check for changed job statuses
        client = chronos.connect("stars-app.renci.org/chronos", proto="https")
        while self.running:
            # jobs for the job store of this batch
            remote_jobs = client.search(name=self.jobStoreID)
            # job summary info, contains status for jobs, which we need
            remote_jobs_summary = client._call("/scheduler/jobs/summary")["jobs"]

            for cached_job in self.issued_jobs:
                job_name = cached_job["name"]
                remote_job = None
                for j in remote_jobs_summary:
                    if j["name"] == job_name:
                        remote_job = j
                if not remote_job:
                    logger.error("Job '%s' not found in chronos" % job_name)
                    return
                if remote_job["status"] != cached_job["status"]:
                    cached_job["status"] = remote_job["status"]

                    proc_status = chronos_status_to_proc_status(cached_job["status"])
                    logger.info("Job '%s' updated with status '%s'" % (job_name, cached_job["status"]))
                    self.updated_jobs_queue.put(
                        (job_name, proc_status, time.time() - cached_job["issued_time"])
                    )
            time.sleep(1)

    def setUserScript(self, userScript):
        raise NotImplementedError()


    """
    Currently returning the string name of the chronos job instead of an int id
    """
    def issueBatchJob(self, jobNode):
        # store jobStoreID as a way to reference this batch of jobs
        self.jobStoreID = jobNode.jobStoreID.replace("/", "-")
        logger.info("jobNode: {}".format(vars(jobNode)))
        logger.info("jobNode command: {}".format(jobNode.command))
        client = chronos.connect("stars-app.renci.org/chronos", proto="https")

        # if a job with this name already exists, it will be overwritten in chronos.
        # we don't want this, so increment a unique counter on the end of it.
        simple_jobnode_jobname = jobNode.jobName.split("/")[-1]
        dup_job_name_counter = len(
                [x for x in self.issued_jobs if simple_jobnode_jobname in x["name"]])
        job_name = "{}_{}_{}".format(
                jobNode.jobStoreID, jobNode.jobName.split("/")[-1], dup_job_name_counter)
        job_name = job_name.replace("/", "-")

        job = {
            "name": job_name,
            "command": ( # replace /path/to/_toil_worker [args] with /path/to/workerscriptlauncher [args]
                "/opt/toil/_toil_worker.sh "
                + " ".join(jobNode.command.split(" ")[1:]) # args after original _toil_worker
                ),
            "owner": "nobody@domain.ext",
            "schedule": "R1//P1Y",
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

        # TODO handle return value here
        ret = client.add(job)
        logger.info("Chronos ret: %s" % ret)

        job["issued_time"] = time.time()
        job["status"] = "fresh" # corresponds to status in chronos for jobs that have not yet run
        self.issued_jobs.append(job)

        #return self.issued_jobs.index(job)
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
        client = chronos.connect("stars-app.renci.org/chronos", proto="https")
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
        client = chronos.connect("stars-app.renci.org/chronos", proto="https")
        jobs = client.search(name=self.jobStoreID)

        jobs_summary = client._call("/scheduler/jobs/summary")["jobs"]
        running_jobs = {}
        for j in jobs:
            # look for this job in the job summary list (which has the state and status fields)
            for summary in jobs_summary:
                if summary["name"] == j["name"]:
                    # add state field from summary to job obj
                    j["status"] = summary["status"]
                    j["state"] = summary["state"]
                    if "running" in j["state"]:
                        # look up local job obj which contains the issued time and compare to now
                        # (not the actual run time in mesos, just time since it was issued in toil)
                        run_seconds = 0
                        for lj in self.issued_jobs:
                            if lj["name"] == j["name"]:
                                run_seconds = time.time() - lj["issued_time"]
                        running_jobs[j["name"]] = run_seconds
        return running_jobs


    def getUpdatedBatchJob(self, maxWait):
        while True:
            try:
                job_id, status, wallTime = self.updated_jobs_queue.get(timeout=maxWait)
            except Empty:
                return None

            return job_id, status, wallTime

        return None


    def shutdown(self):
        logger.info("shutdown called")
        self.running = False
        self.worker.join()

    def setEnv(self, name, value=None):
        raise NotImplementedError()

    @classmethod
    def getRescueBatchJobFrequency(cls):
        raise NotImplementedError()
    @classmethod
    def setOptions(cls, setOption):
        pass


def chronos_status_to_proc_status(status):
    if not status or status == "failure":
        return 1
    else:
        return 0

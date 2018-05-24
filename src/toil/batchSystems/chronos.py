from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import logging
import urllib
from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem, BatchSystemSupport
import chronos
import time
import os
from threading import Thread
import six
from six.moves.queue import Empty, Queue
from six.moves.urllib.parse import urlparse
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
        logger.debug("config: {}".format(config))
        c = os.getenv("CHRONOS_URL")
        if not c:
            raise RuntimeError("Chronos batch system requires CHRONOS_URL to be set")
        urlp = urlparse(c)
        if urlp.scheme:
            self.chronos_proto = urlp.scheme
            self.chronos_endpoint = c[len(urlp.scheme) + 3:]
        else:
            self.chronos_proto = "https"
            self.chronos_endpoint = c
        #print("proto: " + str(self.chronos_proto))
        #print("endpoint: " + str(self.chronos_endpoint))
        self.shared_filesystem_password = os.getenv("IRODS_PASSWORD")
        if self.chronos_endpoint is None:
            raise RuntimeError(
                "Chronos batch system requires environment variable "
                "'TOIL_CHRONOS_ENDPOINT' to be defined.")
        if self.chronos_proto is None:
            self.chronos_proto = "http"
        if self.shared_filesystem_password is None:
            raise RuntimeError(
                "Chronos batch system requires a password for shared filesystem")

        """
        List of jobs in format:
        { "name": <str>,
           ... other chronos job fields
          "issued_time": <issued time in seconds (Float)>,
          "status": <fresh|success|failed> }
        """
        self.issued_jobs = []

        self.updated_jobs_queue = Queue()
        self.jobStoreID = None
        self.running = True
        self.worker = Thread(target=self.updated_job_worker, args=())
        self.worker.start()

    def updated_job_worker(self):
        # poll chronos api and check for changed job statuses
        client = get_chronos_client(self.chronos_endpoint, self.chronos_proto)
        not_found_counts = {}
        not_found_fail_threshold = 5 # how many times to allow a job to not be found
        while self.running:
            logger.info("Checking job summary in Chronos")
            # jobs for the job store of this batch
            #remote_jobs = client.search(name=self.jobStoreID)
            # job summary info, contains status for jobs, which we need
            remote_jobs_summary = client._call("/scheduler/jobs/summary")["jobs"]

            for cached_job in self.issued_jobs:
                job_name = cached_job["name"]
                logger.info("Checking status of job '%s'" % job_name)
                remote_job = None
                for j in remote_jobs_summary:
                    if j["name"] == job_name:
                        remote_job = j
                if not remote_job:
                    logger.error("Job '%s' not found in chronos" % job_name)
                    if job_name not in not_found_counts: not_found_counts[job_name] = 0
                    not_found_counts[job_name] += 1
                    if not_found_counts[job_name] > not_found_fail_threshold:
                        raise RuntimeError(
                            "Chronos job not found during {} poll iterations".format(
                                not_found_fail_threshold))
                    else:
                        continue #if not found, could be race condition with chronos REST API job adding
                if remote_job["status"] != cached_job["status"]:
                    cached_job["status"] = remote_job["status"]

                    proc_status = chronos_status_to_proc_status(cached_job["status"])
                    logger.info("Job '{}' updated in Chronos with status '{}'".format(job_name, cached_job["status"]))
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
        logger.info("jobNode: " + str(vars(jobNode)))
        # store jobStoreID as a way to reference this batch of jobs
        self.jobStoreID = jobNode.jobStoreID.replace("/", "-")
        logger.debug("issuing batch job with unique ID: {}".format(self.jobStoreID))
        logger.debug("jobNode command: {}".format(jobNode.command))
        client = get_chronos_client(self.chronos_endpoint, self.chronos_proto)

        # if a job with this name already exists, it will be overwritten in chronos.
        # we don't want this, so increment a unique counter on the end of it.
        simple_jobnode_jobname = jobNode.jobName.split("/")[-1]
        dup_job_name_counter = len(
                [x for x in self.issued_jobs if simple_jobnode_jobname in x["name"]])
        job_name = "{}_{}_{}".format(
                jobNode.jobStoreID, jobNode.jobName.split("/")[-1], dup_job_name_counter)
        job_name = job_name.replace("/", "-")

        # all environment variables in this context that start with IRODS_ will be passed to worker containers
        env_str = ""
        for k,v in six.iteritems(os.environ):
            if k.startswith("IRODS_"):
                env_str += "-e {}='{}' ".format(k,v)

        job = {
            "name": job_name,
            "command": ( # replace /path/to/_toil_worker [args] with /path/to/workerscriptlauncher [args]
                #"/opt/toil/_toil_worker.sh " # toil requires worker process to have "_toil_worker" in it
                "sudo docker run --privileged {} heliumdatacommons/datacommons-base _toil_worker '{}'".format(
                        env_str, # aggregated environment vars
                        " ".join(jobNode.command.split(" ")[1:])) # args after original _toil_worker
                ),
            "owner": "nobody@domain.ext",
            "schedule": "R1//P1Y",
            "epsilon": "PT15M",
            "execute_now": True,
            "shell": True,
            "disabled": False,
        }
        logger.info("Creating job in chronos: \n%s" % job)

        # TODO is this return value relevant?
        ret = client.add(job)

        job["issued_time"] = time.time()
        job["status"] = "fresh" # corresponds to status in chronos for jobs that have not yet run
        self.issued_jobs.append(job)

        return job["name"]


    """
    Kill the tasks for a list of jobs in chronos, and delete the jobs in chronos
    """
    def killBatchJobs(self, jobIDs):
        client = get_chronos_client(self.chronos_endpoint, self.chronos_proto)
        for jobID in jobIDs:
            client.delete_tasks(jobID)
            client.delete(jobID)
            logger.info("Removed job '{}' from chronos.".format(jobID))


    """
    Currently returning the string name of the jobs as the ids, not int ids
    Matches ids from issueBatchJob
    """
    def getIssuedBatchJobIDs(self):
        if not self.jobStoreID:
            return []
        client = get_chronos_client(self.chronos_endpoint, self.chronos_proto)
        jobs = client.search(name=self.jobStoreID)
        ids = [j["name"] for j in jobs]
        return ids

    """
    Returns {<jobname(str)>: <seconds(int)>, ...}
    """
    def getRunningBatchJobIDs(self):
        if not self.jobStoreID:
            return {}
        client = get_chronos_client(self.chronos_endpoint, self.chronos_proto)
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

    """
    Returns the most recently updated job. Waits up to maxWait for a job to be marked as updated.
    The worker thread marks a job as updated when its status changes in Chronos.
    """
    def getUpdatedBatchJob(self, maxWait):
        while True:
            try:
                job_id, status, wallTime = self.updated_jobs_queue.get(timeout=maxWait)
            except Empty:
                return None

            return job_id, status, wallTime

        return None


    def shutdown(self):
        logger.debug("shutdown called")
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

def get_chronos_client(endpoint, proto):
    client = chronos.connect(endpoint, proto=proto)
    return client

def chronos_status_to_proc_status(status):
    if not status or status == "failure":
        return 1
    else:
        return 0

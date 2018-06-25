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
# limitations under the License.

"""
The leader script (of the leader/worker pair) for running jobs.
"""
from __future__ import absolute_import
from __future__ import division

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
from builtins import super
import logging
import time

try:
    import cPickle as pickle
except ImportError:
    import pickle

from toil.lib.humanize import bytes2human
from toil import resolveEntryPoint
try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()
from toil.jobStores.abstractJobStore import NoSuchJobException
from toil.provisioners.clusterScaler import ScalerThread
from toil.serviceManager import ServiceManager
from toil.statsAndLogging import StatsAndLogging
from toil.job import JobNode, ServiceJobNode
from toil.toilState import ToilState
from toil.common import ToilMetrics

logger = logging.getLogger( __name__ )

###############################################################################
# Implementation Notes
#
# Multiple predecessors:
#   There is special-case handling for jobs with multiple predecessors as a
#   performance optimization. This minimize number of expensive loads of
#   jobGraphs from jobStores.  However, this special case could be unnecessary.
#   The jobGraph is loaded to update predecessorsFinished, in
#   _checkSuccssorReadyToRunMultiplePredecessors, however it doesn't appear to
#   write the jobGraph back the jobStore.  Thus predecessorsFinished may really
#   be leader state and could moved out of the jobGraph.  This would make this
#   special-cases handling unnecessary and simplify the leader.
#   Issue #2136
###############################################################################



####################################################
# Exception thrown by the Leader class when one or more jobs fails
####################################################

class FailedJobsException(Exception):
    def __init__(self, jobStoreLocator, failedJobs, jobStore):
        msg = "The job store '%s' contains %i failed jobs" % (jobStoreLocator, len(failedJobs))
        try:
            msg += ": %s" % ", ".join((str(failedJob) for failedJob in failedJobs))
            for jobNode in failedJobs:
                job = jobStore.load(jobNode.jobStoreID)
                if job.logJobStoreFileID:
                    msg += "\n=========> Failed job %s \n" % jobNode
                    with job.getLogFileHandle(jobStore) as fH:
                        msg += fH.read().decode('utf-8')
                    msg += "<=========\n"
        # catch failures to prepare more complex details and only return the basics
        except:
            logger.exception('Exception when compiling information about failed jobs')
        super().__init__()
        self.jobStoreLocator = jobStoreLocator
        self.numberOfFailedJobs = len(failedJobs)

####################################################
# Exception thrown by the Leader class when a deadlock is encountered due to insufficient
# resources to run the workflow
####################################################

class DeadlockException(Exception):
    def __init__(self, msg):
        msg = "Deadlock encountered: " + msg
        super().__init__()

####################################################
##Following class represents the leader
####################################################

class Leader(object):
    """ Class that encapsulates the logic of the leader.
    """
    def __init__(self, config, batchSystem, provisioner, jobStore, rootJob, jobCache=None):
        """
        :param toil.common.Config config:
        :param toil.batchSystems.abstractBatchSystem.AbstractBatchSystem batchSystem:
        :param toil.provisioners.abstractProvisioner.AbstractProvisioner provisioner
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:
        :param toil.jobGraph.JobGraph rootJob

        If jobCache is passed, it must be a dict from job ID to pre-existing
        JobGraph objects. Jobs will be loaded from the cache (which can be
        downloaded from the jobStore in a batch) during the construction of the ToilState object.
        """
        # Object containing parameters for the run
        self.config = config

        # The job store
        self.jobStore = jobStore
        self.jobStoreLocator = config.jobStore

        # Get a snap shot of the current state of the jobs in the jobStore
        self.toilState = ToilState(jobStore, rootJob, jobCache=jobCache)
        logger.info("Found %s jobs to start and %i jobs with successors to run",
                        len(self.toilState.updatedJobs), len(self.toilState.successorCounts))

        # Batch system
        self.batchSystem = batchSystem
        assert len(self.batchSystem.getIssuedBatchJobIDs()) == 0 #Batch system must start with no active jobs!
        logger.info("Checked batch system has no running jobs and no updated jobs")

        # Map of batch system IDs to IssuedJob tuples
        self.jobBatchSystemIDToIssuedJob = {}

        # Number of preempetable jobs currently being run by batch system
        self.preemptableJobsIssued = 0

        # Tracking the number service jobs issued,
        # this is used limit the number of services issued to the batch system
        self.serviceJobsIssued = 0
        self.serviceJobsToBeIssued = [] # A queue of service jobs that await scheduling
        #Equivalents for service jobs to be run on preemptable nodes
        self.preemptableServiceJobsIssued = 0
        self.preemptableServiceJobsToBeIssued = []

        # Timing of the jobGraph rescuing method
        self.timeSinceJobsLastRescued = None

        # Hash to store number of times a job is lost by the batch system,
        # used to decide if to reissue an apparently missing job
        self.reissueMissingJobs_missingHash = {}

        # Class used to create/destroy nodes in the cluster, may be None if
        # using a statically defined cluster
        self.provisioner = provisioner

        # Create cluster scaling thread if the provisioner is not None
        self.clusterScaler = None
        if self.provisioner is not None and len(self.provisioner.nodeTypes) > 0:
            self.clusterScaler = ScalerThread(self.provisioner, self, self.config)

        # A service manager thread to start and terminate services
        self.serviceManager = ServiceManager(jobStore, self.toilState)

        # A thread to manage the aggregation of statistics and logging from the run
        self.statsAndLogging = StatsAndLogging(self.jobStore, self.config)

        # Set used to monitor deadlocked jobs
        self.potentialDeadlockedJobs = set()
        self.potentialDeadlockTime = 0

        # A dashboard that runs on the leader node in AWS clusters to track the state
        # of the cluster
        self.toilMetrics = None

        # internal jobs we should not expose at top level debugging
        self.debugJobNames = ("CWLJob", "CWLWorkflow", "CWLScatter", "CWLGather",
                              "ResolveIndirect")

    def run(self):
        """
        This runs the leader process to issue and manage jobs.

        :raises: toil.leader.FailedJobsException if at the end of function their remain \
        failed jobs

        :return: The return value of the root job's run function.
        :rtype: Any
        """
        # Start the stats/logging aggregation thread
        self.statsAndLogging.start()
        if self.config.metrics:
            self.toilMetrics = ToilMetrics(provisioner=self.provisioner)

        try:

            # Start service manager thread
            self.serviceManager.start()
            try:

                # Create cluster scaling processes if not None
                if self.clusterScaler is not None:
                    self.clusterScaler.start()

                try:
                    # Run the main loop
                    self.innerLoop()
                finally:
                    if self.clusterScaler is not None:
                        logger.info('Waiting for workers to shutdown.')
                        startTime = time.time()
                        self.clusterScaler.shutdown()
                        logger.info('Worker shutdown complete in %s seconds.', time.time() - startTime)

            finally:
                # Ensure service manager thread is properly shutdown
                self.serviceManager.shutdown()

        finally:
            # Ensure the stats and logging thread is properly shutdown
            self.statsAndLogging.shutdown()
            if self.toilMetrics:
                self.toilMetrics.shutdown()


        # Filter the failed jobs
        self.toilState.totalFailedJobs = [j for j in self.toilState.totalFailedJobs if self.jobStore.exists(j.jobStoreID)]

        logger.info("Finished toil run %s" %
                     ("successfully." if not self.toilState.totalFailedJobs \
                else ("with %s failed jobs." % len(self.toilState.totalFailedJobs))))

        if len(self.toilState.totalFailedJobs):
            logger.info("Failed jobs at end of the run: %s", ' '.join(str(job) for job in self.toilState.totalFailedJobs))
        # Cleanup
        if len(self.toilState.totalFailedJobs) > 0:
            raise FailedJobsException(self.config.jobStore, self.toilState.totalFailedJobs, self.jobStore)


        return self.jobStore.getRootJobReturnValue()

    def _handledFailedSuccessor(self, jobNode, jobGraph, successorJobStoreID):
        """Deal with the successor having failed. Return True if there are
        still active successors. Return False if all successors have failed
        and the job is queued to run to handle the failed successors."""
        logger.debug("Successor job: %s of job: %s has failed """
                     "predecessors", jobNode, jobGraph)

        # Add the job to the set having failed successors
        self.toilState.hasFailedSuccessors.add(jobGraph.jobStoreID)

        # Reduce active successor count and remove the successor as an active successor of the job
        self.toilState.successorCounts[jobGraph.jobStoreID] -= 1
        assert self.toilState.successorCounts[jobGraph.jobStoreID] >= 0
        self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].remove(jobGraph)
        if len(self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID]) == 0:
            self.toilState.successorJobStoreIDToPredecessorJobs.pop(successorJobStoreID)

        # If the job now has no active successors add to active jobs
        # so it can be processed as a job with failed successors
        if self.toilState.successorCounts[jobGraph.jobStoreID] == 0:
            logger.debug("Job: %s has no successors to run "
                         "and some are failed, adding to list of jobs "
                         "with failed successors", jobGraph)
            self.toilState.successorCounts.pop(jobGraph.jobStoreID)
            self.toilState.updatedJobs.add((jobGraph, 0))
            return False


    def _checkSuccssorReadyToRunMultiplePredecessors(self, jobGraph, jobNode, successorJobStoreID):
        """Handle the special cases of checking if a successor job is
        ready to run when there are multiple predecessors"""
        # See implementation note at the top of this file for discussion of multiple predecessors
        logger.debug("Successor job: %s of job: %s has multiple "
                     "predecessors", jobNode, jobGraph)

        # Get the successor job graph, which is caches
        if successorJobStoreID not in self.toilState.jobsToBeScheduledWithMultiplePredecessors:
            self.toilState.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID] = self.jobStore.load(successorJobStoreID)
        successorJobGraph = self.toilState.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID]

        # Add the jobGraph as a finished predecessor to the successor
        successorJobGraph.predecessorsFinished.add(jobGraph.jobStoreID)

        # If the successor is in the set of successors of failed jobs
        if successorJobStoreID in self.toilState.failedSuccessors:
            if not self._handledFailedSuccessor(jobNode, jobGraph, successorJobStoreID):
                return False

        # If the successor job's predecessors have all not all completed then
        # ignore the jobGraph as is not yet ready to run
        assert len(successorJobGraph.predecessorsFinished) <= successorJobGraph.predecessorNumber
        if len(successorJobGraph.predecessorsFinished) < successorJobGraph.predecessorNumber:
            return False
        else:
            # Remove the successor job from the cache
            self.toilState.jobsToBeScheduledWithMultiplePredecessors.pop(successorJobStoreID)
            return True

    def _makeJobSuccssorReadyToRun(self, jobGraph, jobNode):
        """make a successor job ready to run, returning False if they should
        not yet be run"""
        successorJobStoreID = jobNode.jobStoreID
        #Build map from successor to predecessors.
        if successorJobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = []
        self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobGraph)

        if jobNode.predecessorNumber > 1:
            return self._checkSuccssorReadyToRunMultiplePredecessors(jobGraph, jobNode, successorJobStoreID)
        else:
            return True

    def _runJobSuccessors(self, jobGraph):
        assert len(jobGraph.stack[-1]) > 0
        logger.debug("Job: %s has %i successors to schedule",
                     jobGraph.jobStoreID, len(jobGraph.stack[-1]))
        #Record the number of successors that must be completed before
        #the jobGraph can be considered again
        assert jobGraph.jobStoreID not in self.toilState.successorCounts
        self.toilState.successorCounts[jobGraph.jobStoreID] = len(jobGraph.stack[-1])

        # For each successor schedule if all predecessors have been completed
        successors = []
        for jobNode in jobGraph.stack[-1]:
            if self._makeJobSuccssorReadyToRun(jobGraph, jobNode):
                successors.append(jobNode)
        self.issueJobs(successors)

    def _processFailedSuccessors(self, jobGraph):
        """Some of the jobs successors failed then either fail the job
        or restart it if it has retries left and is a checkpoint job"""

        if jobGraph.jobStoreID in self.toilState.servicesIssued:
            # The job has services running, signal for them to be killed
            # once they are killed then the jobGraph will be re-added to
            # the updatedJobs set and then scheduled to be removed
            logger.debug("Telling job: %s to terminate its services due to successor failure",
                         jobGraph.jobStoreID)
            self.serviceManager.killServices(self.toilState.servicesIssued[jobGraph.jobStoreID],
                                             error=True)
        elif jobGraph.jobStoreID in self.toilState.successorCounts:
            # The job has non-service jobs running wait for them to finish
            # the job will be re-added to the updated jobs when these jobs
            # are done
            logger.debug("Job %s with ID: %s with failed successors still has successor jobs running",
                         jobGraph, jobGraph.jobStoreID)
        elif jobGraph.checkpoint is not None and jobGraph.remainingRetryCount > 1:
            # If the job is a checkpoint and has remaining retries then reissue it.
            # The logic behind using > 1 rather than > 0 here: Since this job has
            # been tried once (without decreasing its retry count as the job
            # itself was successful), and its subtree failed, it shouldn't be retried
            # unless it has more than 1 try.
            logger.warn('Job: %s is being restarted as a checkpoint after the total '
                        'failure of jobs in its subtree.', jobGraph.jobStoreID)
            self.issueJob(JobNode.fromJobGraph(jobGraph))
        else:
            # Mark it totally failed
            logger.debug("Job %s is being processed as completely failed", jobGraph.jobStoreID)
            self.processTotallyFailedJob(jobGraph)

    def _processReadyJob(self, jobGraph, resultStatus):
        logger.debug('Updating status of job %s with ID %s: with result status: %s',
                     jobGraph, jobGraph.jobStoreID, resultStatus)

        if jobGraph in self.serviceManager.jobGraphsWithServicesBeingStarted:
            # This stops a job with services being issued by the serviceManager from
            # being considered further in this loop. This catch is necessary because
            # the job's service's can fail while being issued, causing the job to be
            # added to updated jobs.
            logger.debug("Got a job to update which is still owned by the service "
                         "manager: %s", jobGraph.jobStoreID)
        elif jobGraph.jobStoreID in self.toilState.hasFailedSuccessors:
            self._processFailedSuccessors(jobGraph)
        elif jobGraph.command is not None or resultStatus != 0:
            # The jobGraph has a command it must be run before any successors.
            # Similarly, if the job previously failed we rerun it, even if it doesn't have a
            # command to run, to eliminate any parts of the stack now completed.
            isServiceJob = jobGraph.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob

            # If the job has run out of retries or is a service job whose error flag has
            # been indicated, fail the job.
            if (jobGraph.remainingRetryCount == 0
                or isServiceJob and not self.jobStore.fileExists(jobGraph.errorJobStoreID)):
                self.processTotallyFailedJob(jobGraph)
                logger.warn("Job %s with ID %s is completely failed",
                            jobGraph, jobGraph.jobStoreID)
            else:
                # Otherwise try the job again
                self.issueJob(JobNode.fromJobGraph(jobGraph))
        elif len(jobGraph.services) > 0:
            # the job has services to run, which have not been started, start them
            # Build a map from the service jobs to the job and a map
            # of the services created for the job
            assert jobGraph.jobStoreID not in self.toilState.servicesIssued
            self.toilState.servicesIssued[jobGraph.jobStoreID] = {}
            for serviceJobList in jobGraph.services:
                for serviceTuple in serviceJobList:
                    serviceID = serviceTuple.jobStoreID
                    assert serviceID not in self.toilState.serviceJobStoreIDToPredecessorJob
                    self.toilState.serviceJobStoreIDToPredecessorJob[serviceID] = jobGraph
                    self.toilState.servicesIssued[jobGraph.jobStoreID][serviceID] = serviceTuple

            # Use the service manager to start the services
            self.serviceManager.scheduleServices(jobGraph)

            logger.debug("Giving job: %s to service manager to schedule its jobs", jobGraph.jobStoreID)
        elif len(jobGraph.stack) > 0:
            # There are exist successors to run
            self._runJobSuccessors(jobGraph)
        elif jobGraph.jobStoreID in self.toilState.servicesIssued:
            logger.debug("Telling job: %s to terminate its services due to the "
                         "successful completion of its successor jobs",
                         jobGraph)
            self.serviceManager.killServices(self.toilState.servicesIssued[jobGraph.jobStoreID], error=False)
        else:
            #There are no remaining tasks to schedule within the jobGraph, but
            #we schedule it anyway to allow it to be deleted. Remove the job

            #TODO: An alternative would be simple delete it here and add it to the
            #list of jobs to process, or (better) to create an asynchronous
            #process that deletes jobs and then feeds them back into the set
            #of jobs to be processed
            if jobGraph.remainingRetryCount > 0:
                self.issueJob(JobNode.fromJobGraph(jobGraph))
                logger.debug("Job: %s is empty, we are scheduling to clean it up", jobGraph.jobStoreID)
            else:
                self.processTotallyFailedJob(jobGraph)
                logger.warn("Job: %s is empty but completely failed - something is very wrong", jobGraph.jobStoreID)

    def _processReadyJobs(self):
        """Process jobs that are ready to be scheduled/have successors to schedule"""
        logger.debug('Built the jobs list, currently have %i jobs to update and %i jobs issued',
                     len(self.toilState.updatedJobs), self.getNumberOfJobsIssued())

        updatedJobs = self.toilState.updatedJobs # The updated jobs to consider below
        self.toilState.updatedJobs = set() # Resetting the list for the next set

        for jobGraph, resultStatus in updatedJobs:
            self._processReadyJob(jobGraph, resultStatus)

    def _startServiceJobs(self):
        """Start any service jobs available from the service manager"""
        self.issueQueingServiceJobs()
        while True:
            serviceJob = self.serviceManager.getServiceJobsToStart(0)
            # Stop trying to get jobs when function returns None
            if serviceJob is None:
                break
            logger.debug('Launching service job: %s', serviceJob)
            self.issueServiceJob(serviceJob)

    def _processJobsWithRunningServices(self):
        """Get jobs whose services have started"""
        while True:
            jobGraph = self.serviceManager.getJobGraphWhoseServicesAreRunning(0)
            if jobGraph is None: # Stop trying to get jobs when function returns None
                break
            logger.debug('Job: %s has established its services.', jobGraph.jobStoreID)
            jobGraph.services = []
            self.toilState.updatedJobs.add((jobGraph, 0))

    def _gatherUpdatedJobs(self, updatedJobTuple):
        """Gather any new, updated jobGraph from the batch system"""
        jobID, result, wallTime = updatedJobTuple
        # easy, track different state
        try:
            updatedJob = self.jobBatchSystemIDToIssuedJob[jobID]
        except KeyError:
            logger.warn("A result seems to already have been processed "
                        "for job %s", jobID)
        else:
            if result == 0:
                cur_logger = (logger.debug if str(updatedJob.jobName).startswith(CWL_INTERNAL_JOBS)
                              else logger.info)
                cur_logger('Job ended successfully: %s', updatedJob)
                if self.toilMetrics:
                    self.toilMetrics.logCompletedJob(updatedJob)
            else:
                logger.warn('Job failed with exit value %i: %s',
                            result, updatedJob)
            self.processFinishedJob(jobID, result, wallTime=wallTime)

    def _processLostJobs(self):
        """Process jobs that have gone awry"""
        # In the case that there is nothing happening (no updated jobs to
        # gather for rescueJobsFrequency seconds) check if there are any jobs
        # that have run too long (see self.reissueOverLongJobs) or which have
        # gone missing from the batch system (see self.reissueMissingJobs)
        if ((time.time() - self.timeSinceJobsLastRescued) >= self.config.rescueJobsFrequency):
            # We only rescue jobs every N seconds, and when we have apparently
            # exhausted the current jobGraph supply
            self.reissueOverLongJobs()
            logger.info("Reissued any over long jobs")

            hasNoMissingJobs = self.reissueMissingJobs()
            if hasNoMissingJobs:
                self.timeSinceJobsLastRescued = time.time()
            else:
                # This means we'll try again in a minute, providing things are quiet
                self.timeSinceJobsLastRescued += 60
            logger.info("Rescued any (long) missing jobs")


    def innerLoop(self):
        """
        The main loop for processing jobs by the leader.
        """
        self.timeSinceJobsLastRescued = time.time()

        while self.toilState.updatedJobs or \
              self.getNumberOfJobsIssued() or \
              self.serviceManager.jobsIssuedToServiceManager:

            if self.toilState.updatedJobs:
                self._processReadyJobs()

            # deal with service-related jobs
            self._startServiceJobs()
            self._processJobsWithRunningServices()

            # check in with the batch system
            updatedJobTuple = self.batchSystem.getUpdatedBatchJob(maxWait=2)
            if updatedJobTuple is not None:
                self._gatherUpdatedJobs(updatedJobTuple)
            else:
                self._processLostJobs()

            # Check on the associated threads and exit if a failure is detected
            self.statsAndLogging.check()
            self.serviceManager.check()
            # the cluster scaler object will only be instantiated if autoscaling is enabled
            if self.clusterScaler is not None:
                self.clusterScaler.check()

            # Check for deadlocks
            self.checkForDeadlocks()

        logger.info("Finished the main loop: no jobs left to run.")

        # Consistency check the toil state
        assert self.toilState.updatedJobs == set()
        assert self.toilState.successorCounts == {}
        assert self.toilState.successorJobStoreIDToPredecessorJobs == {}
        assert self.toilState.serviceJobStoreIDToPredecessorJob == {}
        assert self.toilState.servicesIssued == {}
        # assert self.toilState.jobsToBeScheduledWithMultiplePredecessors # These are not properly emptied yet
        # assert self.toilState.hasFailedSuccessors == set() # These are not properly emptied yet

    def checkForDeadlocks(self):
        """
        Checks if the system is deadlocked running service jobs.
        """
        totalRunningJobs = len(self.batchSystem.getRunningBatchJobIDs())
        totalServicesIssued = self.serviceJobsIssued + self.preemptableServiceJobsIssued
        # If there are no updated jobs and at least some jobs running
        if totalServicesIssued >= totalRunningJobs and len(self.toilState.updatedJobs) == 0 and totalRunningJobs > 0:
            serviceJobs = [x for x in list(self.jobBatchSystemIDToIssuedJob.keys()) if isinstance(self.jobBatchSystemIDToIssuedJob[x], ServiceJobNode)]
            runningServiceJobs = set([x for x in serviceJobs if self.serviceManager.isRunning(self.jobBatchSystemIDToIssuedJob[x])])
            assert len(runningServiceJobs) <= totalRunningJobs

            # If all the running jobs are active services then we have a potential deadlock
            if len(runningServiceJobs) == totalRunningJobs:
                # We wait self.config.deadlockWait seconds before declaring the system deadlocked
                if self.potentialDeadlockedJobs != runningServiceJobs:
                    self.potentialDeadlockedJobs = runningServiceJobs
                    self.potentialDeadlockTime = time.time()
                elif time.time() - self.potentialDeadlockTime >= self.config.deadlockWait:
                    raise DeadlockException("The system is service deadlocked - all %d running jobs are active services" % totalRunningJobs)
            else:
                # We have observed non-service jobs running, so reset the potential deadlock
                self.potentialDeadlockedJobs = set()
                self.potentialDeadlockTime = 0
        else:
            # We have observed non-service jobs running, so reset the potential deadlock
            self.potentialDeadlockedJobs = set()
            self.potentialDeadlockTime = 0

    def issueJob(self, jobNode):
        """
        Add a job to the queue of jobs
        """
        jobNode.command = ' '.join((resolveEntryPoint('_toil_worker'),
                                    jobNode.jobName,
                                    self.jobStoreLocator,
                                    jobNode.jobStoreID))
        # jobBatchSystemID is an int that is an incremented counter for each job
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobNode)
        self.jobBatchSystemIDToIssuedJob[jobBatchSystemID] = jobNode
        if jobNode.preemptable:
            # len(jobBatchSystemIDToIssuedJob) should always be greater than or equal to preemptableJobsIssued,
            # so increment this value after the job is added to the issuedJob dict
            self.preemptableJobsIssued += 1
        cur_logger = (logger.debug if jobNode.jobName.startswith(CWL_INTERNAL_JOBS)
                      else logger.info)
        cur_logger("Issued job %s with job batch system ID: "
                   "%s and cores: %s, disk: %s, and memory: %s",
                   jobNode, str(jobBatchSystemID), int(jobNode.cores),
                   bytes2human(jobNode.disk), bytes2human(jobNode.memory))
        if self.toilMetrics:
            self.toilMetrics.logIssuedJob(jobNode)
            self.toilMetrics.logQueueSize(self.getNumberOfJobsIssued())

    def issueJobs(self, jobs):
        """
        Add a list of jobs, each represented as a jobNode object
        """
        for job in jobs:
            self.issueJob(job)

    def issueServiceJob(self, jobNode):
        """
        Issue a service job, putting it on a queue if the maximum number of service
        jobs to be scheduled has been reached.
        """
        if jobNode.preemptable:
            self.preemptableServiceJobsToBeIssued.append(jobNode)
        else:
            self.serviceJobsToBeIssued.append(jobNode)
        self.issueQueingServiceJobs()

    def issueQueingServiceJobs(self):
        """
        Issues any queuing service jobs up to the limit of the maximum allowed.
        """
        while len(self.serviceJobsToBeIssued) > 0 and self.serviceJobsIssued < self.config.maxServiceJobs:
            self.issueJob(self.serviceJobsToBeIssued.pop())
            self.serviceJobsIssued += 1
        while len(self.preemptableServiceJobsToBeIssued) > 0 and self.preemptableServiceJobsIssued < self.config.maxPreemptableServiceJobs:
            self.issueJob(self.preemptableServiceJobsToBeIssued.pop())
            self.preemptableServiceJobsIssued += 1

    def getNumberOfJobsIssued(self, preemptable=None):
        """
        Gets number of jobs that have been added by issueJob(s) and not
        removed by removeJob

        :param None or boolean preemptable: If none, return all types of jobs.
          If true, return just the number of preemptable jobs. If false, return
          just the number of non-preemptable jobs.
        """
        if preemptable is None:
            return len(self.jobBatchSystemIDToIssuedJob)
        elif preemptable:
            return self.preemptableJobsIssued
        else:
            assert len(self.jobBatchSystemIDToIssuedJob) >= self.preemptableJobsIssued
            return len(self.jobBatchSystemIDToIssuedJob) - self.preemptableJobsIssued


    def removeJob(self, jobBatchSystemID):
        """
        Removes a job from the system.
        """
        assert jobBatchSystemID in self.jobBatchSystemIDToIssuedJob
        jobNode = self.jobBatchSystemIDToIssuedJob[jobBatchSystemID]
        if jobNode.preemptable:
            # len(jobBatchSystemIDToIssuedJob) should always be greater than or equal to preemptableJobsIssued,
            # so decrement this value before removing the job from the issuedJob map
            assert self.preemptableJobsIssued > 0
            self.preemptableJobsIssued -= 1
        del self.jobBatchSystemIDToIssuedJob[jobBatchSystemID]
        # If service job
        if jobNode.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Decrement the number of services
            if jobNode.preemptable:
                self.preemptableServiceJobsIssued -= 1
            else:
                self.serviceJobsIssued -= 1

        return jobNode

    def getJobs(self, preemptable=None):
        jobs = self.jobBatchSystemIDToIssuedJob.values()
        if preemptable is not None:
            jobs = [job for job in jobs if job.preemptable == preemptable]
        return jobs

    def killJobs(self, jobsToKill):
        """
        Kills the given set of jobs and then sends them for processing
        """
        if len(jobsToKill) > 0:
            self.batchSystem.killBatchJobs(jobsToKill)
            for jobBatchSystemID in jobsToKill:
                self.processFinishedJob(jobBatchSystemID, 1)

    #Following functions handle error cases for when jobs have gone awry with the batch system.

    def reissueOverLongJobs(self):
        """
        Check each issued job - if it is running for longer than desirable
        issue a kill instruction.
        Wait for the job to die then we pass the job to processFinishedJob.
        """
        maxJobDuration = self.config.maxJobDuration
        jobsToKill = []
        if maxJobDuration < 10000000:  # We won't bother doing anything if rescue time > 16 weeks.
            runningJobs = self.batchSystem.getRunningBatchJobIDs()
            for jobBatchSystemID in list(runningJobs.keys()):
                if runningJobs[jobBatchSystemID] > maxJobDuration:
                    logger.warn("The job: %s has been running for: %s seconds, more than the "
                                "max job duration: %s, we'll kill it",
                                str(self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID),
                                str(runningJobs[jobBatchSystemID]),
                                str(maxJobDuration))
                    jobsToKill.append(jobBatchSystemID)
            self.killJobs(jobsToKill)

    def reissueMissingJobs(self, killAfterNTimesMissing=3):
        """
        Check all the current job ids are in the list of currently running batch system jobs.
        If a job is missing, we mark it as so, if it is missing for a number of runs of
        this function (say 10).. then we try deleting the job (though its probably lost), we wait
        then we pass the job to processFinishedJob.
        """
        runningJobs = set(self.batchSystem.getIssuedBatchJobIDs())
        jobBatchSystemIDsSet = set(list(self.jobBatchSystemIDToIssuedJob.keys()))
        #Clean up the reissueMissingJobs_missingHash hash, getting rid of jobs that have turned up
        missingJobIDsSet = set(list(self.reissueMissingJobs_missingHash.keys()))
        for jobBatchSystemID in missingJobIDsSet.difference(jobBatchSystemIDsSet):
            self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
            logger.warn("Batch system id: %s is no longer missing", str(jobBatchSystemID))
        assert runningJobs.issubset(jobBatchSystemIDsSet) #Assert checks we have
        #no unexpected jobs running
        jobsToKill = []
        for jobBatchSystemID in set(jobBatchSystemIDsSet.difference(runningJobs)):
            jobStoreID = self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID
            if jobBatchSystemID in self.reissueMissingJobs_missingHash:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] += 1
            else:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] = 1
            timesMissing = self.reissueMissingJobs_missingHash[jobBatchSystemID]
            logger.warn("Job store ID %s with batch system id %s is missing for the %i time",
                        jobStoreID, str(jobBatchSystemID), timesMissing)
            if self.toilMetrics:
                self.toilMetrics.logMissingJob()
            if timesMissing == killAfterNTimesMissing:
                self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
                jobsToKill.append(jobBatchSystemID)
        self.killJobs(jobsToKill)
        return len( self.reissueMissingJobs_missingHash ) == 0 #We use this to inform
        #if there are missing jobs

    def processRemovedJob(self, issuedJob, resultStatus):
        if resultStatus != 0:
            logger.warn("Despite the batch system claiming failure the "
                        "job %s seems to have finished and been removed", issuedJob)
        self._updatePredecessorStatus(issuedJob.jobStoreID)

    def processFinishedJob(self, batchSystemID, resultStatus, wallTime=None):
        """
        Function reads a processed jobGraph file and updates its state.
        """
        jobNode = self.removeJob(batchSystemID)
        jobStoreID = jobNode.jobStoreID
        if wallTime is not None and self.clusterScaler is not None:
            self.clusterScaler.addCompletedJob(jobNode, wallTime)
        if self.jobStore.exists(jobStoreID):
            logger.debug("Job %s continues to exist (i.e. has more to do)", jobNode)
            try:
                jobGraph = self.jobStore.load(jobStoreID)
            except NoSuchJobException:
                # Avoid importing AWSJobStore as the corresponding extra might be missing
                if self.jobStore.__class__.__name__ == 'AWSJobStore':
                    # We have a ghost job - the job has been deleted but a stale read from
                    # SDB gave us a false positive when we checked for its existence.
                    # Process the job from here as any other job removed from the job store.
                    # This is a temporary work around until https://github.com/BD2KGenomics/toil/issues/1091
                    # is completed
                    logger.warn('Got a stale read from SDB for job %s', jobNode)
                    self.processRemovedJob(jobNode, resultStatus)
                    return
                else:
                    raise
            if jobGraph.logJobStoreFileID is not None:
                with jobGraph.getLogFileHandle(self.jobStore) as logFileStream:
                    # more memory efficient than read().striplines() while leaving off the
                    # trailing \n left when using readlines()
                    # http://stackoverflow.com/a/15233739
                    StatsAndLogging.logWithFormatting(jobStoreID, logFileStream, method=logger.warn,
                                                      message='The job seems to have left a log file, indicating failure: %s' % jobGraph)
                if self.config.writeLogs or self.config.writeLogsGzip:
                    with jobGraph.getLogFileHandle(self.jobStore) as logFileStream:
                        StatsAndLogging.writeLogFiles(jobGraph.chainedJobs, logFileStream, self.config)
            if resultStatus != 0:
                # If the batch system returned a non-zero exit code then the worker
                # is assumed not to have captured the failure of the job, so we
                # reduce the retry count here.
                if jobGraph.logJobStoreFileID is None:
                    logger.warn("No log file is present, despite job failing: %s", jobNode)
                jobGraph.setupJobAfterFailure(self.config)
                self.jobStore.update(jobGraph)
            elif jobStoreID in self.toilState.hasFailedSuccessors:
                # If the job has completed okay, we can remove it from the list of jobs with failed successors
                self.toilState.hasFailedSuccessors.remove(jobStoreID)

            self.toilState.updatedJobs.add((jobGraph, resultStatus)) #Now we know the
            #jobGraph is done we can add it to the list of updated jobGraph files
            logger.debug("Added job: %s to active jobs", jobGraph)
        else:  #The jobGraph is done
            self.processRemovedJob(jobNode, resultStatus)

    @staticmethod
    def getSuccessors(jobGraph, alreadySeenSuccessors, jobStore):
        """
        Gets successors of the given job by walking the job graph recursively.
        Any successor in alreadySeenSuccessors is ignored and not traversed.
        Returns the set of found successors. This set is added to alreadySeenSuccessors.
        """
        successors = set()
        def successorRecursion(jobGraph):
            # For lists of successors
            for successorList in jobGraph.stack:

                # For each successor in list of successors
                for successorJobNode in successorList:

                    # If successor not already visited
                    if successorJobNode.jobStoreID not in alreadySeenSuccessors:

                        # Add to set of successors
                        successors.add(successorJobNode.jobStoreID)
                        alreadySeenSuccessors.add(successorJobNode.jobStoreID)

                        # Recurse if job exists
                        # (job may not exist if already completed)
                        if jobStore.exists(successorJobNode.jobStoreID):
                            successorRecursion(jobStore.load(successorJobNode.jobStoreID))

        successorRecursion(jobGraph) # Recurse from jobGraph

        return successors

    def processTotallyFailedJob(self, jobGraph):
        """
        Processes a totally failed job.
        """
        # Mark job as a totally failed job
        self.toilState.totalFailedJobs.add(JobNode.fromJobGraph(jobGraph))
        if self.toilMetrics:
            self.toilMetrics.logFailedJob(jobGraph)

        if jobGraph.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob: # Is
            # a service job
            logger.debug("Service job is being processed as a totally failed job: %s", jobGraph)

            predecesssorJobGraph = self.toilState.serviceJobStoreIDToPredecessorJob[jobGraph.jobStoreID]

            # This removes the service job as a service of the predecessor
            # and potentially makes the predecessor active
            self._updatePredecessorStatus(jobGraph.jobStoreID)

            # Remove the start flag, if it still exists. This indicates
            # to the service manager that the job has "started", this prevents
            # the service manager from deadlocking while waiting
            self.jobStore.deleteFile(jobGraph.startJobStoreID)

            # Signal to any other services in the group that they should
            # terminate. We do this to prevent other services in the set
            # of services from deadlocking waiting for this service to start properly
            if predecesssorJobGraph.jobStoreID in self.toilState.servicesIssued:
                self.serviceManager.killServices(self.toilState.servicesIssued[predecesssorJobGraph.jobStoreID], error=True)
                logger.debug("Job: %s is instructing all the services of its parent job to quit", jobGraph)

            self.toilState.hasFailedSuccessors.add(predecesssorJobGraph.jobStoreID) # This ensures that the
            # job will not attempt to run any of it's successors on the stack
        else:
            # Is a non-service job
            assert jobGraph.jobStoreID not in self.toilState.servicesIssued

            # Traverse failed job's successor graph and get the jobStoreID of new successors.
            # Any successor already in toilState.failedSuccessors will not be traversed
            # All successors traversed will be added to toilState.failedSuccessors and returned
            # as a set (unseenSuccessors).
            unseenSuccessors = self.getSuccessors(jobGraph, self.toilState.failedSuccessors,
                                                  self.jobStore)
            logger.debug("Found new failed successors: %s of job: %s", " ".join(
                         unseenSuccessors), jobGraph)

            # For each newly found successor
            for successorJobStoreID in unseenSuccessors:

                # If the successor is a successor of other jobs that have already tried to schedule it
                if successorJobStoreID in self.toilState.successorJobStoreIDToPredecessorJobs:

                    # For each such predecessor job
                    # (we remove the successor from toilState.successorJobStoreIDToPredecessorJobs to avoid doing
                    # this multiple times for each failed predecessor)
                    for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(successorJobStoreID):

                        # Reduce the predecessor job's successor count.
                        self.toilState.successorCounts[predecessorJob.jobStoreID] -= 1

                        # Indicate that it has failed jobs.
                        self.toilState.hasFailedSuccessors.add(predecessorJob.jobStoreID)
                        logger.debug("Marking job: %s as having failed successors (found by "
                                     "reading successors failed job)", predecessorJob)

                        # If the predecessor has no remaining successors, add to list of active jobs
                        assert self.toilState.successorCounts[predecessorJob.jobStoreID] >= 0
                        if self.toilState.successorCounts[predecessorJob.jobStoreID] == 0:
                            self.toilState.updatedJobs.add((predecessorJob, 0))

                            # Remove the predecessor job from the set of jobs with successors.
                            self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

            # If the job has predecessor(s)
            if jobGraph.jobStoreID in self.toilState.successorJobStoreIDToPredecessorJobs:

                # For each predecessor of the job
                for predecessorJobGraph in self.toilState.successorJobStoreIDToPredecessorJobs[jobGraph.jobStoreID]:

                    # Mark the predecessor as failed
                    self.toilState.hasFailedSuccessors.add(predecessorJobGraph.jobStoreID)
                    logger.debug("Totally failed job: %s is marking direct predecessor: %s "
                                 "as having failed jobs", jobGraph, predecessorJobGraph)

                self._updatePredecessorStatus(jobGraph.jobStoreID)

    def _updatePredecessorStatus(self, jobStoreID):
        """
        Update status of predecessors for finished successor job.
        """
        if jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Is a service job
            predecessorJob = self.toilState.serviceJobStoreIDToPredecessorJob.pop(jobStoreID)
            self.toilState.servicesIssued[predecessorJob.jobStoreID].pop(jobStoreID)
            if len(self.toilState.servicesIssued[predecessorJob.jobStoreID]) == 0: # Predecessor job has
                # all its services terminated
                self.toilState.servicesIssued.pop(predecessorJob.jobStoreID) # The job has no running services
                self.toilState.updatedJobs.add((predecessorJob, 0)) # Now we know
                # the job is done we can add it to the list of updated job files

        elif jobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            #We have reach the root job
            assert len(self.toilState.updatedJobs) == 0
            assert len(self.toilState.successorJobStoreIDToPredecessorJobs) == 0
            assert len(self.toilState.successorCounts) == 0
            logger.debug("Reached root job %s so no predecessors to clean up" % jobStoreID)

        else:
            # Is a non-root, non-service job
            logger.debug("Cleaning the predecessors of %s" % jobStoreID)

            # For each predecessor
            for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(jobStoreID):

                # Reduce the predecessor's number of successors by one to indicate the
                # completion of the jobStoreID job
                self.toilState.successorCounts[predecessorJob.jobStoreID] -= 1

                # If the predecessor job is done and all the successors are complete
                if self.toilState.successorCounts[predecessorJob.jobStoreID] == 0:

                    # Remove it from the set of jobs with active successors
                    self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

                    if predecessorJob.jobStoreID not in self.toilState.hasFailedSuccessors:
                        # Pop stack at this point, as we can get rid of its successors
                        predecessorJob.stack.pop()

                    # Now we know the job is done we can add it to the list of updated job files
                    assert predecessorJob not in self.toilState.updatedJobs
                    self.toilState.updatedJobs.add((predecessorJob, 0))

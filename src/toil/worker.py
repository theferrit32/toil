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

from __future__ import absolute_import, print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import map
from builtins import filter
import os
import sys
import copy
import random
import json

import tempfile
import traceback
import time
import socket
import logging
import shutil
from threading import Thread

try:
    import cPickle as pickle
except ImportError:
    import pickle

from toil.lib.expando import MagicExpando
from toil.common import Toil, safeUnpickleFromStream
from toil.fileStore import FileStore
from toil import logProcessContext
from toil.job import Job
from toil.lib.bioio import setLogLevel
from toil.lib.bioio import getTotalCpuTime
from toil.lib.bioio import getTotalCpuTimeAndMemoryUsage
import signal

logger = logging.getLogger(__name__)

def nextChainableJobGraph(jobGraph, jobStore):
    """Returns the next chainable jobGraph after this jobGraph if one
    exists, or None if the chain must terminate.
    """
    #If no more jobs to run or services not finished, quit
    if len(jobGraph.stack) == 0 or len(jobGraph.services) > 0 or jobGraph.checkpoint != None:
        logger.debug("Stopping running chain of jobs: length of stack: %s, services: %s, checkpoint: %s",
                     len(jobGraph.stack), len(jobGraph.services), jobGraph.checkpoint != None)
        return None

    #Get the next set of jobs to run
    jobs = jobGraph.stack[-1]
    assert len(jobs) > 0

    #If there are 2 or more jobs to run in parallel we quit
    if len(jobs) >= 2:
        logger.debug("No more jobs can run in series by this worker,"
                    " it's got %i children", len(jobs)-1)
        return None

    #We check the requirements of the jobGraph to see if we can run it
    #within the current worker
    successorJobNode = jobs[0]
    if successorJobNode.memory > jobGraph.memory:
        logger.debug("We need more memory for the next job, so finishing")
        return None
    if successorJobNode.cores > jobGraph.cores:
        logger.debug("We need more cores for the next job, so finishing")
        return None
    if successorJobNode.disk > jobGraph.disk:
        logger.debug("We need more disk for the next job, so finishing")
        return None
    if successorJobNode.preemptable != jobGraph.preemptable:
        logger.debug("Preemptability is different for the next job, returning to the leader")
        return None
    if successorJobNode.predecessorNumber > 1:
        logger.debug("The jobGraph has multiple predecessors, we must return to the leader.")
        return None

    # Load the successor jobGraph
    successorJobGraph = jobStore.load(successorJobNode.jobStoreID)

    # Somewhat ugly, but check if job is a checkpoint job and quit if
    # so
    if successorJobGraph.command.startswith( "_toil " ):
        #Load the job
        successorJob = Job._loadJob(successorJobGraph.command, jobStore)

        # Check it is not a checkpoint
        if successorJob.checkpoint:
            logger.debug("Next job is checkpoint, so finishing")
            return None

    # Made it through! This job is chainable.
    return successorJobGraph

def workerScript(jobStore, config, jobName, jobStoreID, redirectOutputToLogFile=True):
    """
    Worker process script, runs a job. 
    
    :param str jobName: The "job name" (a user friendly name) of the job to be run
    :param str jobStoreLocator: Specifies the job store to use
    :param str jobStoreID: The job store ID of the job to be run
    :param bool redirectOutputToLogFile: Redirect standard out and standard error to a log file
    """
    logging.basicConfig()

    ##########################################
    #Create the worker killer, if requested
    ##########################################

    logFileByteReportLimit = config.maxLogFileSize

    if config.badWorker > 0 and random.random() < config.badWorker:
        def badWorker():
            #This will randomly kill the worker process at a random time 
            time.sleep(config.badWorkerFailInterval * random.random())
            os.kill(os.getpid(), signal.SIGKILL) #signal.SIGINT)
            #TODO: FIX OCCASIONAL DEADLOCK WITH SIGINT (tested on single machine)
        t = Thread(target=badWorker)
        # Ideally this would be a daemon thread but that causes an intermittent (but benign)
        # exception similar to the one described here:
        # http://stackoverflow.com/questions/20596918/python-exception-in-thread-thread-1-most-likely-raised-during-interpreter-shutd
        # Our exception is:
        #    Exception in thread Thread-1 (most likely raised during interpreter shutdown):
        #    <type 'exceptions.AttributeError'>: 'NoneType' object has no attribute 'kill'
        # This attribute error is caused by the call os.kill() and apparently unavoidable with a
        # daemon
        t.start()

    ##########################################
    #Load the environment for the jobGraph
    ##########################################
    
    #First load the environment for the jobGraph.
    with jobStore.readSharedFileStream("environment.pickle") as fileHandle:
        environment = safeUnpickleFromStream(fileHandle)
    for i in environment:
        if i not in ("TMPDIR", "TMP", "HOSTNAME", "HOSTTYPE"):
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(':'):
            if e != '':
                sys.path.append(e)

    setLogLevel(config.logLevel)

    toilWorkflowDir = Toil.getWorkflowDir(config.workflowID, config.workDir)

    ##########################################
    #Setup the temporary directories.
    ##########################################
        
    # Dir to put all this worker's temp files in.
    localWorkerTempDir = tempfile.mkdtemp(dir=toilWorkflowDir)
    os.chmod(localWorkerTempDir, 0o755)

    ##########################################
    #Setup the logging
    ##########################################

    #This is mildly tricky because we don't just want to
    #redirect stdout and stderr for this Python process; we want to redirect it
    #for this process and all children. Consequently, we can't just replace
    #sys.stdout and sys.stderr; we need to mess with the underlying OS-level
    #file descriptors. See <http://stackoverflow.com/a/11632982/402891>
    
    #When we start, standard input is file descriptor 0, standard output is
    #file descriptor 1, and standard error is file descriptor 2.

    #What file do we want to point FDs 1 and 2 to?
    tempWorkerLogPath = os.path.join(localWorkerTempDir, "worker_log.txt")

    if redirectOutputToLogFile:
        # Save the original stdout and stderr (by opening new file descriptors
        # to the same files)
        origStdOut = os.dup(1)
        origStdErr = os.dup(2)

        # Open the file to send stdout/stderr to.
        logFh = os.open(tempWorkerLogPath, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

        # Replace standard output with a descriptor for the log file
        os.dup2(logFh, 1)

        # Replace standard error with a descriptor for the log file
        os.dup2(logFh, 2)

        # Since we only opened the file once, all the descriptors duped from
        # the original will share offset information, and won't clobber each
        # others' writes. See <http://stackoverflow.com/a/5284108/402891>. This
        # shouldn't matter, since O_APPEND seeks to the end of the file before
        # every write, but maybe there's something odd going on...

        # Close the descriptor we used to open the file
        os.close(logFh)

    debugging = logging.getLogger().isEnabledFor(logging.DEBUG)
    ##########################################
    #Worker log file trapped from here on in
    ##########################################

    workerFailed = False
    statsDict = MagicExpando()
    statsDict.jobs = []
    statsDict.workers.logsToMaster = []
    blockFn = lambda : True
    listOfJobs = [jobName]
    try:

        #Put a message at the top of the log, just to make sure it's working.
        logger.info("---TOIL WORKER OUTPUT LOG---")
        sys.stdout.flush()
        
        logProcessContext(config)

        ##########################################
        #Load the jobGraph
        ##########################################
        
        jobGraph = jobStore.load(jobStoreID)
        listOfJobs[0] = str(jobGraph)
        logger.debug("Parsed job wrapper")
        
        ##########################################
        #Cleanup from any earlier invocation of the jobGraph
        ##########################################
        
        if jobGraph.command == None:
            logger.debug("Wrapper has no user job to run.")
            # Cleanup jobs already finished
            f = lambda jobs : [z for z in [[y for y in x if jobStore.exists(y.jobStoreID)] for x in jobs] if len(z) > 0]
            jobGraph.stack = f(jobGraph.stack)
            jobGraph.services = f(jobGraph.services)
            logger.debug("Cleaned up any references to completed successor jobs")

        #This cleans the old log file which may 
        #have been left if the job is being retried after a job failure.
        oldLogFile = jobGraph.logJobStoreFileID
        if oldLogFile != None:
            jobGraph.logJobStoreFileID = None
            jobStore.update(jobGraph) #Update first, before deleting any files
            jobStore.deleteFile(oldLogFile)

        ##########################################
        # If a checkpoint exists, restart from the checkpoint
        ##########################################

        # The job is a checkpoint, and is being restarted after previously completing
        if jobGraph.checkpoint != None:
            logger.debug("Job is a checkpoint")
            # If the checkpoint still has extant jobs in its
            # (flattened) stack and services, its subtree didn't
            # complete properly. We handle the restart of the
            # checkpoint here, removing its previous subtree.
            if len([i for l in jobGraph.stack for i in l]) > 0 or len(jobGraph.services) > 0:
                logger.debug("Checkpoint has failed.")
                # Reduce the retry count
                assert jobGraph.remainingRetryCount >= 0
                jobGraph.remainingRetryCount = max(0, jobGraph.remainingRetryCount - 1)
                jobGraph.restartCheckpoint(jobStore)
            # Otherwise, the job and successors are done, and we can cleanup stuff we couldn't clean
            # because of the job being a checkpoint
            else:
                logger.debug("The checkpoint jobs seems to have completed okay, removing any checkpoint files to delete.")
                #Delete any remnant files
                list(map(jobStore.deleteFile, list(filter(jobStore.fileExists, jobGraph.checkpointFilesToDelete))))

        ##########################################
        #Setup the stats, if requested
        ##########################################
        
        if config.stats:
            startClock = getTotalCpuTime()

        startTime = time.time()
        while True:
            ##########################################
            #Run the jobGraph, if there is one
            ##########################################
            
            if jobGraph.command is not None:
                assert jobGraph.command.startswith( "_toil " )
                logger.debug("Got a command to run: %s" % jobGraph.command)
                #Load the job
                job = Job._loadJob(jobGraph.command, jobStore)
                # If it is a checkpoint job, save the command
                if job.checkpoint:
                    jobGraph.checkpoint = jobGraph.command

                # Create a fileStore object for the job
                fileStore = FileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                      caching=not config.disableCaching)
                with job._executor(jobGraph=jobGraph,
                                   stats=statsDict if config.stats else None,
                                   fileStore=fileStore):
                    with fileStore.open(job):
                        # Get the next block function and list that will contain any messages
                        blockFn = fileStore._blockFn

                        job._runner(jobGraph=jobGraph, jobStore=jobStore, fileStore=fileStore)

                # Accumulate messages from this job & any subsequent chained jobs
                statsDict.workers.logsToMaster += fileStore.loggingMessages

            else:
                #The command may be none, in which case
                #the jobGraph is either a shell ready to be deleted or has
                #been scheduled after a failure to cleanup
                logger.debug("No user job to run, so finishing")
                break
            
            if FileStore._terminateEvent.isSet():
                raise RuntimeError("The termination flag is set")

            ##########################################
            #Establish if we can run another jobGraph within the worker
            ##########################################
            successorJobGraph = nextChainableJobGraph(jobGraph, jobStore)
            if successorJobGraph is None or config.disableChaining:
                # Can't chain any more jobs.
                break

            ##########################################
            #We have a single successor job that is not a checkpoint job.
            #We transplant the successor jobGraph command and stack
            #into the current jobGraph object so that it can be run
            #as if it were a command that were part of the current jobGraph.
            #We can then delete the successor jobGraph in the jobStore, as it is
            #wholly incorporated into the current jobGraph.
            ##########################################

            # add the successor to the list of jobs run
            listOfJobs.append(str(successorJobGraph))

            #Clone the jobGraph and its stack
            jobGraph = copy.deepcopy(jobGraph)
            
            #Remove the successor jobGraph
            jobGraph.stack.pop()

            #Transplant the command and stack to the current jobGraph
            jobGraph.command = successorJobGraph.command
            jobGraph.stack += successorJobGraph.stack
            # include some attributes for better identification of chained jobs in
            # logging output
            jobGraph.unitName = successorJobGraph.unitName
            jobGraph.jobName = successorJobGraph.jobName
            assert jobGraph.memory >= successorJobGraph.memory
            assert jobGraph.cores >= successorJobGraph.cores
            
            #Build a fileStore to update the job
            fileStore = FileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                  caching=not config.disableCaching)

            #Update blockFn
            blockFn = fileStore._blockFn

            #Add successorJobGraph to those to be deleted
            fileStore.jobsToDelete.add(successorJobGraph.jobStoreID)
            
            #This will update the job once the previous job is done
            fileStore._updateJobWhenDone()            
            
            #Clone the jobGraph and its stack again, so that updates to it do
            #not interfere with this update
            jobGraph = copy.deepcopy(jobGraph)
            
            logger.debug("Starting the next job")
        
        ##########################################
        #Finish up the stats
        ##########################################
        if config.stats:
            totalCPUTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            statsDict.workers.time = str(time.time() - startTime)
            statsDict.workers.clock = str(totalCPUTime - startClock)
            statsDict.workers.memory = str(totalMemoryUsage)

        # log the worker log path here so that if the file is truncated the path can still be found
        if redirectOutputToLogFile:
            logger.info("Worker log can be found at %s. Set --cleanWorkDir to retain this log", localWorkerTempDir)
        
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds", time.time() - startTime)
    
    ##########################################
    #Trapping where worker goes wrong
    ##########################################
    except: #Case that something goes wrong in worker
        traceback.print_exc()
        logger.error("Exiting the worker because of a failed job on host %s", socket.gethostname())
        FileStore._terminateEvent.set()
    
    ##########################################
    #Wait for the asynchronous chain of writes/updates to finish
    ########################################## 
       
    blockFn() 
    
    ##########################################
    #All the asynchronous worker/update threads must be finished now, 
    #so safe to test if they completed okay
    ########################################## 
    
    if FileStore._terminateEvent.isSet():
        jobGraph = jobStore.load(jobStoreID)
        jobGraph.setupJobAfterFailure(config)
        workerFailed = True

    ##########################################
    #Cleanup
    ##########################################

    # Close the worker logging
    # Flush at the Python level
    sys.stdout.flush()
    sys.stderr.flush()
    if redirectOutputToLogFile:
        # Flush at the OS level
        os.fsync(1)
        os.fsync(2)

        # Close redirected stdout and replace with the original standard output.
        os.dup2(origStdOut, 1)

        # Close redirected stderr and replace with the original standard error.
        os.dup2(origStdErr, 2)

        # sys.stdout and sys.stderr don't need to be modified at all. We don't
        # need to call redirectLoggerStreamHandlers since they still log to
        # sys.stderr

        # Close our extra handles to the original standard output and standard
        # error streams, so we don't leak file handles.
        os.close(origStdOut)
        os.close(origStdErr)

    # Now our file handles are in exactly the state they were in before.

    #Copy back the log file to the global dir, if needed
    if workerFailed:
        jobGraph.logJobStoreFileID = jobStore.getEmptyFileStoreID(jobGraph.jobStoreID)
        jobGraph.chainedJobs = listOfJobs
        with jobStore.updateFileStream(jobGraph.logJobStoreFileID) as w:
            with open(tempWorkerLogPath, "r") as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit !=0:
                    if logFileByteReportLimit > 0:
                        f.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                    elif logFileByteReportLimit < 0:
                        f.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
                w.write(f.read().encode('utf-8')) # TODO load file using a buffer
        jobStore.update(jobGraph)

    elif debugging and redirectOutputToLogFile:  # write log messages
        with open(tempWorkerLogPath, 'r') as logFile:
            if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit != 0:
                if logFileByteReportLimit > 0:
                    logFile.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                elif logFileByteReportLimit < 0:
                    logFile.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
            logMessages = logFile.read().splitlines()
        statsDict.logs.names = listOfJobs
        statsDict.logs.messages = logMessages

    if (debugging or config.stats or statsDict.workers.logsToMaster) and not workerFailed:  # We have stats/logging to report back
        jobStore.writeStatsAndLogging(json.dumps(statsDict, ensure_ascii=True))

    #Remove the temp dir
    cleanUp = config.cleanWorkDir
    if cleanUp == 'always' or (cleanUp == 'onSuccess' and not workerFailed) or (cleanUp == 'onError' and workerFailed):
        shutil.rmtree(localWorkerTempDir)
    
    #This must happen after the log file is done with, else there is no place to put the log
    if (not workerFailed) and jobGraph.command == None and len(jobGraph.stack) == 0 and len(jobGraph.services) == 0:
        # We can now safely get rid of the jobGraph
        jobStore.delete(jobGraph.jobStoreID)

def main(argv=None):
    if argv is None:
        argv = sys.argv

    # Parse input args
    jobName = argv[1]
    jobStoreLocator = argv[2]
    jobStoreID = argv[3]

    ##########################################
    #Load the jobStore/config file
    ##########################################

    # Try to monkey-patch boto early so that credentials are cached.
    try:
        import boto
    except ImportError:
        pass
    else:
        # boto is installed, monkey patch it now
        from toil.lib.ec2Credentials import enable_metadata_credential_caching
        enable_metadata_credential_caching()

    jobStore = Toil.resumeJobStore(jobStoreLocator)
    config = jobStore.config

    # Call the worker
    workerScript(jobStore, config, jobName, jobStoreID)

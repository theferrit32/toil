from __future__ import absolute_import
from past.builtins import map
import logging
import os
import pipes
from toil import subprocess
import docker
import base64
import time
import requests

from docker.errors import create_api_error_from_http_exception
from docker.errors import ContainerError
from docker.errors import ImageNotFound
from docker.errors import APIError
from docker.errors import NotFound
from docker.errors import DockerException
from docker.utils.types import LogConfig
from docker.api.container import ContainerApiMixin

from toil.lib.retry import retry
from docker import client
from pwd import getpwuid

from toil.test import timeLimit

logger = logging.getLogger(__name__)

FORGO = 0
STOP = 1
RM = 2

def dockerPredicate(e):
    """
    Used to ensure Docker exceptions are retried if appropriate

    :param e: Exception
    :return: True if e retriable, else False
    """
    if not isinstance(e, subprocess.CalledProcessError):
        return False
    if e.returncode == 125:
        return True

def dockerCheckOutput(*args, **kwargs):
    """
    Deprecated.  Runs subprocessDockerCall() using 'subprocess.check_output()'.

    Provided for backwards compatibility with a previous implementation that
    used 'subprocess.check_output()'.  This has since been supplanted and
    apiDockerCall() is recommended.
    """
    logger.warn("WARNING: dockerCheckOutput() using subprocess.check_output() "
                "is deprecated, please switch to apiDockerCall().")
    return subprocessDockerCall(*args, checkOutput=True, **kwargs)

def dockerCall(*args, **kwargs):
    """
    Deprecated.  Runs subprocessDockerCall() using 'subprocess.check_output()'.

    Provided for backwards compatibility with a previous implementation that
    used 'subprocess.check_call()'.  This has since been supplanted and
    apiDockerCall() is recommended.
    """
    logger.warn("WARNING: dockerCall() using subprocess.check_output() "
                "is deprecated, please switch to apiDockerCall().")
    return subprocessDockerCall(*args, checkOutput=False, **kwargs)

def subprocessDockerCall(job,
                         tool,
                         parameters=None,
                         workDir=None,
                         dockerParameters=None,
                         checkOutput=True,
                         outfile=None,
                         errfile=None,
                         defer=None):
    """
    Deprecated.  Calls Docker using subprocess.check_output().

    Assumes `docker` is on the PATH.  Uses Toil's defer functionality to ensure
    containers are shutdown even in case of job or pipeline failure

    Example of using dockerCall in toil to index a FASTA file with SAMtools:
        def toil_job(job):
            work_dir = job.fileStore.getLocalTempDir()
            path = job.fileStore.readGlobalFile(ref_id, os.path.join(
                                                          work_dir, 'ref.fasta')
            parameters = ['faidx', path]
            dockerCall(job, tool='quay.io/ucgc_cgl/samtools:latest',
                       work_dir=work_dir, parameters=parameters)

    :param toil.Job.job job: The Job instance for the calling function.
    :param str tool: Name of the Docker image to be used
                     (e.g. quay.io/ucsc_cgl/samtools).
    :param list[str] parameters: Command line arguments to be passed.
           If list of lists: list[list[str]], then treat as successive commands
           chained with pipe.
    :param str workDir: Directory to mount into the container via `-v`.
                        Destination convention is '/data'.
    :param list[str] dockerParameters: Parameters to pass to Docker.
             Default parameters are `--rm`, `--log-driver none`, and the
             mountpoint `-v work_dir:/data` where /data is the destination
             convention.
             These defaults are removed if docker_parmaters is passed,
             so be sure to pass them if they are desired.
    :param file outfile: Pipe output of Docker call to file handle
    :param file errfile: Pipe standard error of Docker call to file handle
    :param int defer: What action should be taken on the container upon job
                      completion?
           FORGO (0) will leave the container untouched.
           STOP (1) will attempt to stop the container with `docker stop`
           (useful for debugging).
           RM (2) will stop the container and then forcefully remove it from the
           system using `docker rm -f`. This is the default behavior if defer is
           set to None.
    """
    if parameters is None:
        parameters = []
    if workDir is None:
        workDir = os.getcwd()

    # Setup the outgoing subprocess call for docker
    baseDockerCall = ['docker', 'run']
    if dockerParameters:
        baseDockerCall += dockerParameters
    else:
        baseDockerCall += ['--rm', '--log-driver', 'none', '-v',
                           os.path.abspath(workDir) + ':/data']

    # Ensure the user has passed a valid value for defer
    assert defer in (None, FORGO, STOP, RM)

    # Get container name which is needed for _dockerKill
    try:
        if any('--name' in x for x in baseDockerCall):
            if any('--name=' in x for x in baseDockerCall):
                containerName = [x.split('=')[1]
                                 for x in baseDockerCall if '--name' in x][0]
            else:
                containerName = baseDockerCall[
                    baseDockerCall.index('--name') + 1]
        else:
            containerName = getContainerName(job)
            baseDockerCall.extend(['--name', containerName])
    except ValueError:
        containerName = getContainerName(job)
        baseDockerCall.extend(['--name', containerName])
    except IndexError:
        raise RuntimeError(
            "Couldn't parse Docker's `--name=` option, check parameters: " +
            str(dockerParameters))

    # Defer the container on-exit action
    if '--rm' in baseDockerCall and defer is None:
        defer = RM
    if '--rm' in baseDockerCall and defer is not RM:
        logger.warn('--rm being passed to docker call but defer not set to '
                    'dockerCall.RM, defer set to: ' + str(defer))
    job.defer(_dockerKill, containerName, action=defer)
    # Defer the permission fixing function which will run after this job.
    # We call this explicitly later on in this function,
    # but we defer it as well to handle unexpected job failure.
    job.defer(_fixPermissions, tool, workDir)

    # Make subprocess call

    # If parameters is list of lists, treat each list as separate command and chain with pipes
    if len(parameters) > 0 and type(parameters[0]) is list:
        # When piping, all arguments now get merged into a single string to bash -c.
        # We try to support spaces in paths by wrapping them all in quotes first.
        chain_params = [' '.join(p) for p in [list(map(pipes.quote, q)) for q in parameters]]
        # Use bash's set -eo pipefail to detect and abort on a failure in any command in the chain
        call = baseDockerCall + ['--entrypoint', '/bin/bash',  tool, '-c',
                                 'set -eo pipefail && {}'.format(' | '.join(chain_params))]
    else:
        call = baseDockerCall + [tool] + parameters
    logger.info("Calling docker with " + repr(call))

    params = {}
    if outfile:
        params['stdout'] = outfile
    if errfile:
        params['stderr'] = errfile
    if checkOutput:
        callMethod = subprocess.check_output
    else:
        callMethod = subprocess.check_call

    for attempt in retry(predicate=dockerPredicate):
        with attempt:
            out = callMethod(call, **params)

    _fixPermissions(tool=tool, workDir=workDir)
    return out

def apiDockerCall(job,
                  image,
                  parameters=None,
                  deferParam=None,
                  volumes=None,
                  working_dir=None,
                  containerName=None,
                  entrypoint=None,
                  detach=False,
                  log_config=None,
                  auto_remove=None,
                  remove=False,
                  user=None,
                  environment=None,
                  stdout=None,
                  stderr=False,
                  streamfile=None,
                  **kwargs):
    """
    A toil wrapper for the python docker API.

    Docker API Docs: https://docker-py.readthedocs.io/en/stable/index.html
    Docker API Code: https://github.com/docker/docker-py

    This implements docker's python API within toil so that calls are run as
    jobs, with the intention that failed/orphaned docker jobs be handled
    appropriately.

    Example of using dockerCall in toil to index a FASTA file with SAMtools:
    def toil_job(job):
        working_dir = job.fileStore.getLocalTempDir()
        path = job.fileStore.readGlobalFile(ref_id,
                                          os.path.join(working_dir, 'ref.fasta')
        parameters = ['faidx', path]
        apiDockerCall(job,
                      image='quay.io/ucgc_cgl/samtools:latest',
                      working_dir=working_dir,
                      parameters=parameters)
                      
    Note that when run with detatch=False, or with detatch=True and stdout=True
    or stderr=True, this is a blocking call. When run with detatch=True and
    without output capture, the container is started and returned without
    waiting for it to finish.

    :param toil.Job.job job: The Job instance for the calling function.
    :param str image: Name of the Docker image to be used.
                     (e.g. 'quay.io/ucsc_cgl/samtools:latest')
    :param list[str] parameters: A list of string elements.  If there are
                                 multiple elements, these will be joined with
                                 spaces.  This handling of multiple elements
                                 provides backwards compatibility with previous
                                 versions which called docker using
                                 subprocess.check_call().
                                 **If list of lists: list[list[str]], then treat
                                 as successive commands chained with pipe.
    :param str working_dir: The working directory.
    :param int deferParam: Action to take on the container upon job completion.
           FORGO (0) leaves the container untouched and running.
           STOP (1) Sends SIGTERM, then SIGKILL if necessary to the container.
           RM (2) Immediately send SIGKILL to the container. This is the default
           behavior if defer is set to None.
    :param str name: The name/ID of the container.
    :param str entrypoint: Prepends commands sent to the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool detach: Run the container in detached mode. (equivalent to '-d')
    :param bool stdout: Return logs from STDOUT when detach=False (default: True).
                        Block and capture stdout to a file when detach=True
                        (default: False). Output capture defaults to output.log,
                        and can be specified with the "streamfile" kwarg.
    :param bool stderr: Return logs from STDERR when detach=False (default: False).
                        Block and capture stderr to a file when detach=True
                        (default: False). Output capture defaults to output.log,
                        and can be specified with the "streamfile" kwarg.
    :param str streamfile: Collect container output to this file if detach=True and 
                        stderr and/or stdout are True. Defaults to "output.log".
    :param dict log_config: Specify the logs to return from the container.  See:
                      https://docker-py.readthedocs.io/en/stable/containers.html
    :param bool remove: Remove the container on exit or not.
    :param str user: The container will be run with the privileges of
                     the user specified.  Can be an actual name, such
                     as 'root' or 'lifeisaboutfishtacos', or it can be
                     the uid or gid of the user ('0' is root; '1000' is
                     an example of a less privileged uid or gid), or a
                     complement of the uid:gid (RECOMMENDED), such as
                     '0:0' (root user : root group) or '1000:1000'
                     (some other user : some other user group).
    :param environment: Allows one to set environment variables inside of the
                        container, such as:
    :param kwargs: Additional keyword arguments supplied to the docker API's
                   run command.  The list is 75 keywords total, for examples
                   and full documentation see:
                   https://docker-py.readthedocs.io/en/stable/containers.html
                   
    :returns: Returns the standard output/standard error text, as requested, when 
              detatch=False. Returns the underlying
              docker.models.containers.Container object from the Docker API when
              detatch=True.
    """

    # make certain that files have the correct permissions
    thisUser = os.getuid()
    thisGroup = os.getgid()
    if user is None:
        user = str(thisUser) + ":" + str(thisGroup)

    if containerName is None:
        containerName = getContainerName(job)

    if working_dir is None:
        working_dir = os.getcwd()

    if volumes is None:
        volumes = {working_dir: {'bind': '/data', 'mode': 'rw'}}

    if parameters is None:
        parameters = []

    # If 'parameters' is a list of lists, treat each list as a separate command
    # and chain with pipes.
    if len(parameters) > 0 and type(parameters[0]) is list:
        if entrypoint is None:
            entrypoint = ['/bin/bash', '-c']
        chain_params = \
            [' '.join((pipes.quote(arg) for arg in command)) \
             for command in parameters]
        command = ' | '.join(chain_params)
        pipe_prefix = "set -eo pipefail && "
        command = [pipe_prefix + command]
        logger.debug("Calling docker with: " + repr(command))

    # If 'parameters' is a normal list, join all elements into a single string
    # element.
    # Example: ['echo','the', 'Oread'] becomes: ['echo the Oread']
    # Note that this is still a list, and the docker API prefers this as best
    # practice:
    # http://docker-py.readthedocs.io/en/stable/containers.html
    elif len(parameters) > 0 and type(parameters) is list:
        command = ' '.join(parameters)
        logger.debug("Calling docker with: " + repr(command))

    # If the 'parameters' lists are empty, they are respecified as None, which
    # tells the API to simply create and run the container
    else:
        entrypoint = None
        command = None

    working_dir = os.path.abspath(working_dir)

    # Ensure the user has passed a valid value for deferParam
    assert deferParam in (None, FORGO, STOP, RM), \
        'Please provide a valid value for deferParam.'

    client = docker.from_env(version='auto')

    if deferParam == STOP:
        job.defer(dockerStop, containerName, client)

    if deferParam == FORGO:
        remove = False
    elif deferParam == RM:
        remove = True
        job.defer(dockerKill, containerName, client)
    elif remove is True:
        job.defer(dockerKill, containerName, client)

    if auto_remove is None:
        auto_remove = remove

    try:
        if detach is False:
            # When detach is False, this returns stdout normally:
            # >>> client.containers.run("ubuntu:latest", "echo hello world")
            # 'hello world\n'
            if stdout is None:
                stdout = True
            out = client.containers.run(image=image,
                                        command=command,
                                        working_dir=working_dir,
                                        entrypoint=entrypoint,
                                        name=containerName,
                                        detach=False,
                                        volumes=volumes,
                                        auto_remove=auto_remove,
                                        stdout=stdout,
                                        stderr=stderr,
                                        remove=remove,
                                        log_config=log_config,
                                        user=user,
                                        environment=environment,
                                        **kwargs)

            return out
        else:
            if (stdout or stderr) and log_config is None:
                logger.warn('stdout or stderr specified, but log_config is not set.  '
                            'Defaulting to "journald".')
                log_config = dict(type='journald')

            if stdout is None:
                stdout = False

            # When detach is True, this returns a container object:
            # >>> client.containers.run("bfirsh/reticulate-splines", detach=True)
            # <Container '45e6d2de7c54'>
            container = client.containers.run(image=image,
                                              command=command,
                                              working_dir=working_dir,
                                              entrypoint=entrypoint,
                                              name=containerName,
                                              detach=True,
                                              volumes=volumes,
                                              auto_remove=auto_remove,
                                              stdout=stdout,
                                              stderr=stderr,
                                              remove=remove,
                                              log_config=log_config,
                                              user=user,
                                              environment=environment,
                                              **kwargs)
            if stdout or stderr:
                if streamfile is None:
                    streamfile = 'output.log'
                for line in container.logs(stdout=stdout, stderr=stderr, stream=True):
                    # stream=True makes this loop blocking; we will loop until
                    # the container stops and there is no more output.
                    with open(streamfile, 'w') as f:
                        f.write(line)
                        
            # If we didn't capture output, the caller will need to .wait() on
            # the container to know when it is done. Even if we did capture
            # output, the caller needs the container to get at the exit code.
            return container
            
    except ContainerError:
        logger.error("Docker had non-zero exit.  Check your command: " + \
                      repr(command))
        raise
    except ImageNotFound:
        logger.error("Docker image not found.")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error("The server returned an error.")
        raise create_api_error_from_http_exception(e)
    except:
        raise

def dockerKill(container_name, client, gentleKill=False):
    """
    Immediately kills a container.  Equivalent to "docker kill":
    https://docs.docker.com/engine/reference/commandline/kill/
    :param container_name: Name of the container being killed.
    :param client: The docker API client object to call.
    """
    try:
        this_container = client.containers.get(container_name)
        while this_container.status == 'running':
            if gentleKill is False:
                client.containers.get(container_name).kill()
            else:
                client.containers.get(container_name).stop()
            this_container = client.containers.get(container_name)
    except NotFound:
        logger.debug("Attempted to stop container, but container != exist: ",
                      container_name)
    except requests.exceptions.HTTPError as e:
        logger.debug("Attempted to stop container, but server gave an error: ",
                      container_name)
        raise create_api_error_from_http_exception(e)

def dockerStop(container_name, client):
    """
    Gracefully kills a container.  Equivalent to "docker stop":
    https://docs.docker.com/engine/reference/commandline/stop/
    :param container_name: Name of the container being stopped.
    :param client: The docker API client object to call.
    """
    dockerKill(container_name, client, gentleKill=True)

def containerIsRunning(container_name):
    """
    Checks whether the container is running or not.
    :param container_name: Name of the container being checked.
    :returns: True if status is 'running', False if status is anything else,
    and None if the container does not exist.
    """
    client = docker.from_env(version='auto')
    try:
        this_container = client.containers.get(container_name)
        if this_container.status == 'running':
            return True
        else:
            # this_container.status == 'exited', 'restarting', or 'paused'
            return False
    except NotFound:
        return None
    except requests.exceptions.HTTPError as e:
        logger.debug("Server error attempting to call container: ",
                      container_name)
        raise create_api_error_from_http_exception(e)

def getContainerName(job):
    """Create a random string including the job name, and return it."""
    return '--'.join([str(job),
                      base64.b64encode(os.urandom(9), '-_')])\
                      .replace("'", '').replace('"', '').replace('_', '')


def _dockerKill(containerName, action):
    """
    Deprecated.  Kills the specified container.
    :param str containerName: The name of the container created by docker_call
    :param int action: What action should be taken on the container?
    """
    running = containerIsRunning(containerName)
    if running is None:
        # This means that the container doesn't exist.  We will see this if the
        # container was run with --rm and has already exited before this call.
        logger.info('The container with name "%s" appears to have already been '
                    'removed.  Nothing to '
                  'do.', containerName)
    else:
        if action in (None, FORGO):
            logger.info('The container with name %s continues to exist as we '
                        'were asked to forgo a '
                      'post-job action on it.', containerName)
        else:
            logger.info('The container with name %s exists. Running '
                        'user-specified defer functions.',
                         containerName)
            if running and action >= STOP:
                logger.info('Stopping container "%s".', containerName)
                for attempt in retry(predicate=dockerPredicate):
                    with attempt:
                        subprocess.check_call(['docker', 'stop', containerName])
            else:
                logger.info('The container "%s" was not found to be running.',
                            containerName)
            if action >= RM:
                # If the container was run with --rm, then stop will most likely
                # remove the container.  We first check if it is running then
                # remove it.
                running = containerIsRunning(containerName)
                if running is not None:
                    logger.info('Removing container "%s".', containerName)
                    for attempt in retry(predicate=dockerPredicate):
                        with attempt:
                            subprocess.check_call(['docker', 'rm', '-f',
                                                   containerName])
                else:
                    logger.info('Container "%s" was not found on the system.'
                                'Nothing to remove.',
                                 containerName)

def _fixPermissions(tool, workDir):
    """
    Deprecated.

    Fix permission of a mounted Docker directory by reusing the tool to change
    ownership.  Docker natively runs as a root inside the container, and files
    written to the mounted directory are implicitly owned by root.
    :param list baseDockerCall: Docker run parameters
    :param str tool: Name of tool
    :param str workDir: Path of work directory to recursively chown
    """
    if os.geteuid() == 0:
        # we're running as root so this chown is redundant
        return

    baseDockerCall = ['docker', 'run', '--log-driver=none',
                      '-v', os.path.abspath(workDir) + ':/data', '--rm',
                      '--entrypoint=chown']
    stat = os.stat(workDir)
    command = baseDockerCall + [tool] + ['-R', '{}:{}'.format(stat.st_uid,
                                                              stat.st_gid),
                                         '/data']
    for attempt in retry(predicate=dockerPredicate):
        with attempt:
            subprocess.check_call(command)

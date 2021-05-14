# -*- coding: UTF-8 -*-

import os
import threading
import psutil
import time
from dataclasses import dataclass
import pickle
import signal


@dataclass
class Statistics:
    deadline_reached: int = 0
    still_is_alive: int = 0
    subprocess_ok: int = 0


statistics = Statistics()


def _job_is_alive(job_pid):
    if not psutil.pid_exists(job_pid):
        return False
    try:
        status = psutil.Process(job_pid).status()

    except Exception:
        return True
    is_running = status in [
        psutil.STATUS_RUNNING,
        psutil.STATUS_SLEEPING,
        psutil.STATUS_WAITING,
    ]
    return is_running


def _job_is_dead(job_pid):
    status = psutil.STATUS_DEAD
    if not psutil.pid_exists(job_pid):
        return True, status
    try:
        status = psutil.Process(job_pid).status()

    except Exception:
        return False, status
    is_dead = status in [psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]
    return is_dead, status


def _join_process(job_pid, timeout):
    time_start = time.time()
    # Typical processing time is 100ms I want to reduce latency impact
    # 10ms looks ok
    polling_time = min(0.1 * timeout, 0.010)
    while time.time() - time_start < timeout and _job_is_alive(job_pid):
        time.sleep(polling_time)
        continue


class AsyncRead(threading.Thread):
    def __init__(self, pipe_read):
        # calling superclass init
        threading.Thread.__init__(self)
        self.fd_read = os.fdopen(pipe_read, "rb")
        self.error, self.results = None, {}

    def run(self):
        try:
            self.results = pickle.load(self.fd_read)
        except Exception as e:
            self.error = f"Pickle failed {e}"
        self.fd_read.close()


class AsyncCall:
    def __init__(self, logger):
        self.logger = logger

    def __call__(self, timeout, func, data):

        pipe_read, pipe_write = os.pipe()
        results, error = {}, None

        job_pid = os.fork()
        is_child = job_pid == 0

        start_time = time.time()
        if is_child:
            os.close(pipe_read)
            # URL extraction is here: call extract_links_binary_multiprocess()
            try:
                func(data, results)
            except Exception as e:
                results["exception"] = f"Call to {func} failed: {e}"
            # Copy the results to the pipe. This is a blocking call if there is
            # too much data and  pipe_read is full
            fd_write = os.fdopen(pipe_write, "wb")
            pickle.dump(results, fd_write)
            fd_write.flush()
            self.logger.debug(f"Pickle dump results {results}")
            fd_write.close()

            # Hasta la vista
            # pylint: disable=W0212
            os._exit(0)

        # From here on there is only the parent
        os.close(pipe_write)
        # Read the pipe in the background
        async_read = AsyncRead(pipe_read)
        async_read.start()
        # Wait for the child to complete: polling+deadline
        _join_process(job_pid, 0.8 * timeout)

        # Try to kill the subprocess no matter what. Killing the process will close the pipe_write
        # Only the pipe's 64KB buffer survives. Hopefully async_read collected the data
        self._kill_bill(job_pid, timeout, start_time)

        # Get the data collected by the background thread
        async_read.join()
        error, results = async_read.error, async_read.results

        return error, results

    def _kill_bill(self, job_pid, timeout, start_time):
        elapsed = time.time() - start_time
        if elapsed > timeout:
            statistics.deadline_reached += 1

        is_alive = _job_is_alive(job_pid)
        if is_alive:
            statistics.still_is_alive += 1
            error = f"Deadline: {timeout}s reached, elapsed {elapsed}s, {job_pid}"
            self._logger.info(error)
        else:
            statistics.subprocess_ok += 1

        # Try to kill the subprocess no matter what
        # It will close the pipe
        try:
            os.kill(job_pid, signal.SIGKILL)
        except Exception as e:
            if is_alive:
                self._logger.error(f"Failed to kill {job_pid} {e}")

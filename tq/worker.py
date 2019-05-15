import os
import _pickle
import pickle
import signal

from time import sleep
from datetime import datetime
from functools import partial

from .logger import get_logger
from .utils import get_next_ts, ts, should_run_fallback
from .que import Queue

from .job import JobStatus, Job


loads = partial(_pickle.loads, protocol=pickle.HIGHEST_PROTOCOL)
default_ttl = 10


logger = get_logger('tq')


class Worker(object):
    def __init__(self, queues, connection, name=None):
        self.working = True
        self.connection = connection
        self.qnames = queues
        self.queues = [Queue(connection, queue) for queue in queues]
        self.name = type(self).__name__ if not name else name
        self.pid = os.getpid()


    def set_signals(self):
        logger.info('{0} started at {1} with pid {2}\nwith queues={3}\n'.format(
            self.name,datetime.now(), self.pid, self.qnames))

        pid = self.pid

        def graceful_exit(signum, frame):
            logger.info('Shutting down worker - {0} ({1})'.
                        format(self.name, pid))

            self.working = False
            os._exit(0)

        signal.signal(signal.SIGTERM, graceful_exit)
        signal.signal(signal.SIGINT, graceful_exit)

    def run(self):
        self.set_signals()

        while self.working:
            job = Queue.deque_any(self.queues, self.connection)
            self.perform(job)

    def perform(self, job, fallback=False):
        fork = os.fork()
        if fork == 0:
            success = self.perform_job(job, fallback)
            os._exit(int(not success))
        else:
            os.waitpid(fork, 0)

    def perform_job(self, job, fallback):
        try:
            logger.info('Performing {}'.format(job.id))

            started_at = ts(datetime.now())

            job.perform(run_fallback=fallback)
            pipe = self.connection.pipeline()
            pipe.multi()

            job.set_status(JobStatus.Finished, pipe)
            job.save(pipeline=pipe)

            success = True

            finished_at = ts(datetime.now())

            logger.info('successfully performed job, took {} seconds'
                        .format((finished_at - started_at)))

        except Exception as e:
            logger.error('failed to perform {}\n log-trace:\n {}'.
                         format(job.id, e))
            success = False

        return success


class ScheduledWorker(Worker):
    def __init__(self, queues, connection, latency=1):
        super().__init__(queues, connection, type(self).__name__)

        queue = queues[0]

        logger.info('Listening for jobs on {}\n'.format(queue))

        self.queue = Queue(connection, queue)
        self.latency = latency

    def reschedule(self, job, next_ts, pipeline):
        logger.info('Job-{} rescheduled at {}'.format(job.id, next_ts))
        self.queue.requeue_scheduled(job, next_ts, pipeline)

    def run(self):
        self.set_signals()

        def wait():
            sleep(self.latency)

        while self.working:
            query_at, jobs = self.queue.get_scheduled()

            if not jobs:
                wait()
                continue

            logger.info('Found {} new jobs on {}'.format(len(jobs),
                        self.queue.name.split(':')[-1]))

            for key in jobs:
                job = Job.fetch(key, self.connection)

                run_fallback = should_run_fallback(query_at, job)
                self.perform(job, run_fallback)

                job.refresh()

                next_runtime = get_next_ts(job)
                can_run_again = next_runtime is not None

                pipeline = self.connection.pipeline()

                if job.reschedulable and can_run_again:
                    job.ran_at = query_at
                    self.reschedule(job, next_runtime, pipeline)
                    continue

                self.queue.delete_scheduled(key, pipeline)
                job.expire(pipeline)
                pipeline.execute()

                logger.info('Job-{} removed from queue'.format(job.id))

            wait()


class FailedWorker(ScheduledWorker):
    latency = 30

    def __init__(self, queues, connection):
        super().__init__(queues, connection, self.latency)


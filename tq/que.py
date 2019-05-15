from datetime import datetime
from uuid import uuid4

from tq.job import Job, JobStatus
from tq.utils import to_text, ts


class Queue(object):
    def __init__(self, connection, name):
        self.connection = connection
        self._name = 'tq:queue:{}'.format(name)

    def enqueue(self, method, args=None, kwargs=None, exec_info=None,
                fallback=None, fallback_info=None):

        job = Job.create(job_id=str(uuid4()), method=method, args=args,
                         kwargs=kwargs, exec_info=exec_info,
                         fallback=fallback, fallback_info=fallback_info,
                         connection=self.connection)

        with self.connection.pipeline() as pipe:
            pipe.multi()
            self.add_job(pipe, job)
            job.set_status(JobStatus.Queued, pipeline=pipe)
            job.save(pipeline=pipe)

        return job

    def add_job(self, pipe, job):
        name = self.name
        if job.scheduled_at:
            pipe.zadd(name, job.scheduled_at, job.id)
            return
        pipe.rpush(name, job.id)

    @classmethod
    def deque_any(cls, queues, connection):
        keys = [queue.name for queue in queues]

        # pop jobs from queues
        result = connection.blpop(keys)

        queue, job_id = result
        job_id = to_text(job_id)

        job = Job.fetch(job_id, connection)
        return job

    def get_scheduled(self):
        now = datetime.utcnow()
        now = ts(now)

        keys = self.connection.zrangebyscore(self.name, 0, now)
        jobs = [to_text(key) for key in keys]

        return now, jobs

    def requeue_scheduled(self, job, timestamp, pipeline):
        ''' requeue for next scheduled time '''
        pipeline.zadd(self.name, timestamp, job.id)

        schedule = {'scheduled_at': timestamp}
        exec_info = {**job.exec_info, **schedule}

        job.exec_info = exec_info
        job.save(pipeline)
        return job

    def delete_scheduled(self, job_id, pipeline=None):
        connection = pipeline or self.connection
        connection.zrem(self.name, job_id)

    @property
    def name(self):
        return self._name

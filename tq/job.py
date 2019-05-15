import _pickle
import pickle
from enum import Enum
from functools import partial

from tq.utils import (decode_hash, decode)


dumps = partial(_pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = _pickle.loads

job_ttl = (60 * 60) * 5


class JobStatus(Enum):
    Queued = 'queued'
    Finished = 'finished'


class Job(object):
    '''Meta Data for Job to be executed'''
    @classmethod
    def create(cls, job_id, method, args, kwargs, connection,
               fallback=None, exec_info=None, fallback_info=None):
        job = cls(job_id, connection=connection)
        job.method = method
        job.fallback = fallback
        job.args = args or ()
        job.kwargs = kwargs or {}
        job._exec_info = exec_info or {}
        job._fb_info = fallback_info or {}
        return job

    def __init__(self, job_id, connection):
        self.id = job_id
        self.kwargs = None
        self.args = None
        self.method = None
        self.fallback = None
        self._result = None
        self._status = None
        self._exec_info = None
        self._fb_info = None
        self.connection = connection

    def to_dict(self):
        job = dict()
        job['id'] = self.id
        job['data'] = self.data()
        job['result'] = dumps(self._result)
        job['state'] = dumps(self._status)
        return job

    def data(self):
        return dumps((self.args, self.kwargs, self.method, self.fallback,
                      self._exec_info, self._fb_info))

    def resolve_data(self, dump):
        self.args, self.kwargs, self.method, \
            self.fallback, self._exec_info, self._fb_info = loads(dump)

    @classmethod
    def fetch(cls, key, connection):
        job = cls(job_id=key, connection=connection)

        if not job.refresh():
            return None

        return job

    def refresh(self):
        key = self.connection.hgetall(self.id)
        data = decode_hash(key)

        if not data:
            return None

        self.resolve_data(data['data'])
        self.id = decode(data['id'])
        self._result = loads(data['result'])
        self._status = loads(data['state'])

        return True

    def set_status(self, status, pipeline=None):
        con = pipeline or self.connection
        con.hset(self.id, 'state', dumps(status))
        self._status = JobStatus.Queued

    def expire(self, pipeline=None):
        connection = pipeline or self.connection
        connection.expire(self.id, job_ttl)

    def delete(self, pipeline=None):
        connection = pipeline or self.connection
        connection.delete(self.id)

    def save(self, pipeline=None):
        pipeline = pipeline or self.connection.pipeline()
        pipeline.hmset(self.id, self.to_dict())
        pipeline.execute()

    def perform(self, run_fallback=False):
        '''Executes the job, if run_fallback, runs the fallback
           method..
        '''
        info_method = self.method_info

        if run_fallback:
            if self.fallback:
                info_method = self.fb_info

        func, args, kwargs = info_method()
        self._result = func(*args, **kwargs)
        return self._result

    @property
    def result(self):
        if self._result:
            return self._result

        result = self.connection.hget(self.id, 'result')

        if result:
            self._result = loads(result)

        return self._result

    @property
    def state(self):
        return self._status

    @property
    def state(self):
        return self._status

    @property
    def scheduled_at(self):
        return self._exec_info.get('scheduled_at')

    @property
    def scheduled_days(self):
        return self._exec_info.get('days')

    @property
    def reschedulable(self):
        if self.every_hour:
            return True

        has_days = self.scheduled_days is not None
        has_slots = len(self.timeslots) > 1
        return has_days or has_slots

    @property
    def has_date(self):
        return self.exec_info.get('date')

    @property
    def every_hour(self):
        return self._exec_info.get('every_hour')

    @property
    def ran_at(self):
        return self._exec_info.get('ran_at')

    @property
    def timeslots(self):
        return self._exec_info.get('timeslots')

    @property
    def timezone(self):
        return self._exec_info['timezone']

    @ran_at.setter
    def ran_at(self, _ran_at):
        self._exec_info['ran_at'] = _ran_at

    @property
    def exec_info(self):
        return self._exec_info

    def method_info(self):
        return self.method, self.args, self.kwargs

    @exec_info.setter
    def exec_info(self, exec_info):
        self._exec_info = exec_info

    def id(self):
        return self.id

import os
from redis import StrictRedis

rc = StrictRedis.from_url(os.environ.get('REDIS_URL'))

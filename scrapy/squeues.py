"""
Scheduler queues
"""

import logging
import marshal
import os
import pickle
import random
from abc import ABC, abstractmethod

from queuelib import queue

from scrapy.exceptions import NotConfigured
from scrapy.utils.reqser import request_to_dict, request_from_dict


logger = logging.getLogger(__name__)


def _with_mkdir(queue_class):

    class DirectoriesCreated(queue_class):

        def __init__(self, path, *args, **kwargs):
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)

            super(DirectoriesCreated, self).__init__(path, *args, **kwargs)

    return DirectoriesCreated


def _serializable_queue(queue_class, serialize, deserialize):

    class SerializableQueue(queue_class):

        def push(self, obj):
            s = serialize(obj)
            super(SerializableQueue, self).push(s)

        def pop(self):
            s = super(SerializableQueue, self).pop()
            if s:
                return deserialize(s)

    return SerializableQueue


def _scrapy_serialization_queue(queue_class):

    class ScrapyRequestQueue(queue_class):

        def __init__(self, crawler, key):
            self.spider = crawler.spider
            super(ScrapyRequestQueue, self).__init__(key)

        @classmethod
        def from_crawler(cls, crawler, key, *args, **kwargs):
            return cls(crawler, key)

        def push(self, request):
            request = request_to_dict(request, self.spider)
            return super(ScrapyRequestQueue, self).push(request)

        def pop(self):
            request = super(ScrapyRequestQueue, self).pop()

            if not request:
                return None

            request = request_from_dict(request, self.spider)
            return request

    return ScrapyRequestQueue


def _scrapy_non_serialization_queue(queue_class):

    class ScrapyRequestQueue(queue_class):
        @classmethod
        def from_crawler(cls, crawler, *args, **kwargs):
            return cls()

    return ScrapyRequestQueue


def _pickle_serialize(obj):
    try:
        return pickle.dumps(obj, protocol=4)
    # Both pickle.PicklingError and AttributeError can be raised by pickle.dump(s)
    # TypeError is raised from parsel.Selector
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise ValueError(str(e)) from e


class _RedisQueue(ABC):

    def __init__(self, path):
        try:
            import redis  # noqa: F401
        except ImportError:
            raise NotConfigured('missing redis library')

        self.client = redis.Redis(
            host=self.spider.settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_HOST'],
            port=self.spider.settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_PORT'],
            db=self.spider.settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_DB'],
        )

        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)

        if os.path.exists(self.queue_file_path):
            with open(self.queue_file_path) as f:
                self.queue_name = f.read()
        else:
            prefix = self.spider.settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_PREFIX']
            if not prefix:
                prefix = "scrapy-{}".format(random.randint(0, 2**32-1))
            self.queue_name = "{}-{}".format(prefix, path)

        logger.debug("Using redis queue '%s'", self.queue_name)

    def push(self, string):
        self.client.lpush(self.queue_name, string)

    @abstractmethod
    def pop(self):
        pass

    def close(self):
        # Serialize the state of the queue.
        with open(self.queue_file_path, 'w') as f:
            f.write(self.queue_name)
        self.client.close()

    def __len__(self):
        return self.client.llen(self.queue_name)

    @property
    def queue_file_path(self):
        return os.path.join(self.path, 'redis.queue')


class _FifoRedisQueue(_RedisQueue):

    def pop(self):
        return self.client.rpop(self.queue_name)


class _LifoRedisQueue(_RedisQueue):

    def pop(self):
        return self.client.lpop(self.queue_name)


PickleFifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.FifoDiskQueue),
    _pickle_serialize,
    pickle.loads
)
PickleLifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.LifoDiskQueue),
    _pickle_serialize,
    pickle.loads
)
PickleFifoRedisQueueNonRequest = _serializable_queue(
    _with_mkdir(_FifoRedisQueue),
    _pickle_serialize,
    pickle.loads
)
PickleLifoRedisQueueNonRequest = _serializable_queue(
    _with_mkdir(_LifoRedisQueue),
    _pickle_serialize,
    pickle.loads
)
MarshalFifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.FifoDiskQueue),
    marshal.dumps,
    marshal.loads
)
MarshalLifoDiskQueueNonRequest = _serializable_queue(
    _with_mkdir(queue.LifoDiskQueue),
    marshal.dumps,
    marshal.loads
)

PickleFifoDiskQueue = _scrapy_serialization_queue(
    PickleFifoDiskQueueNonRequest
)
PickleLifoDiskQueue = _scrapy_serialization_queue(
    PickleLifoDiskQueueNonRequest
)
PickleFifoRedisQueue = _scrapy_serialization_queue(
    PickleFifoRedisQueueNonRequest
)
PickleLifoRedisQueue = _scrapy_serialization_queue(
    PickleLifoRedisQueueNonRequest
)
MarshalFifoDiskQueue = _scrapy_serialization_queue(
    MarshalFifoDiskQueueNonRequest
)
MarshalLifoDiskQueue = _scrapy_serialization_queue(
    MarshalLifoDiskQueueNonRequest
)
FifoMemoryQueue = _scrapy_non_serialization_queue(queue.FifoMemoryQueue)
LifoMemoryQueue = _scrapy_non_serialization_queue(queue.LifoMemoryQueue)

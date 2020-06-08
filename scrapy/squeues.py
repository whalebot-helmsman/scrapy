"""
Scheduler queues
"""

import json
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
            self.crawler = crawler
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

    def __init__(self, path, settings=None):
        try:
            import redis  # noqa: F401
        except ImportError:
            raise NotConfigured('missing redis library')

        # If called from from_crawler() method, self.crawler is set.
        # If called from from_settings() method, settings is given.
        if not settings:
            settings = self.crawler.settings

        self.client = redis.Redis(
            host=settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_HOST'],
            port=settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_PORT'],
            db=settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_DB'],
        )

        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.info = self._loadinfo(settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_PREFIX'])

        logger.debug("Using redis queue '%s'", self.info['queue_name'])

    @classmethod
    def from_settings(cls, settings, path):
        return cls(path, settings)

    def push(self, string):
        self.client.lpush(self.info['queue_name'], string)

    @abstractmethod
    def pop(self):
        pass

    def close(self):
        self._saveinfo(self.info)
        if len(self) == 0:
            self._cleanup()
        self.client.close()

    def _loadinfo(self, prefix):
        infopath = self._infopath()
        if os.path.exists(infopath):
            with open(infopath) as f:
                info = json.load(f)
        else:
            if not prefix:
                prefix = "scrapy-{}".format(random.randint(0, 2**32 - 1))
            info = {
                'queue': 'redis',
                'queue_name': "{}-{}".format(prefix, self.path)
            }
        return info

    def _saveinfo(self, info):
        # Serialize the state of the queue if it is not empty.
        with open(self._infopath(), 'w') as f:
            json.dump(info, f)

    def _infopath(self):
        return os.path.join(self.path, 'info.json')

    def _cleanup(self):
        logger.debug("Removing queue info path '%s'" % self._infopath())
        os.remove(self._infopath())
        if not os.listdir(self.path):
            os.rmdir(self.path)

    def __len__(self):
        return self.client.llen(self.info['queue_name'])


class _FifoRedisQueue(_RedisQueue):

    def pop(self):
        return self.client.rpop(self.info['queue_name'])


class _LifoRedisQueue(_RedisQueue):

    def pop(self):
        return self.client.lpop(self.info['queue_name'])


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

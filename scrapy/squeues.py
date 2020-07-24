"""
Scheduler queues
"""

import logging
import marshal
import os
import pickle
from abc import ABC, abstractmethod

from queuelib import queue

from scrapy.exceptions import NotConfigured, SerializationError, TransientError
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

        def __init__(self, path, settings=None, *args, **kwargs):
            self.settings = settings
            super(SerializableQueue, self).__init__(path, *args, **kwargs)

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
            super(ScrapyRequestQueue, self).__init__(key, crawler.settings)

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
        raise SerializationError(str(e)) from e


class _RedisQueue(ABC):
    clients = {}

    def __init__(self, path):
        try:
            import redis  # noqa: F401
        except ImportError:
            raise NotConfigured('missing redis library')

        prefix = self._get_required_setting('SCHEDULER_EXTERNAL_QUEUE_REDIS_PREFIX')
        self.queue_name = "{}-{}".format(prefix, path)

        url = self._get_required_setting('SCHEDULER_EXTERNAL_QUEUE_REDIS_URL')
        if url not in self.clients:
            # Note: We set the instance variable here.
            # All RedisQueue objects share the same client object.
            _RedisQueue.clients[url] = redis.Redis.from_url(url)
        self.client = _RedisQueue.clients[url]
        self.client.ping()

        self.size = len(self)
        if self.size > 0:
            logger.debug("Using redis at '%s' with existing queue '%s' (size: %d)",
                         url, self.queue_name, self.size)
        else:
            logger.debug("Using redis at '%s' with new queue '%s'", url, self.queue_name)

    def _get_required_setting(self, name):
        value = self.settings.get(name)
        if value is None:
            raise NotConfigured(
                'When the SCHEDULER_DISK_QUEUE setting is defined as '
                '{queue}, the {name} setting must also be defined.'.format(
                    queue=repr(self.settings['SCHEDULER_DISK_QUEUE']),
                    name=name,
                )
            )
        return value

    def push(self, string):
        from redis.exceptions import ConnectionError, TimeoutError
        try:
            self.client.lpush(self.queue_name, string)
            self.size += 1
        except (ConnectionError, TimeoutError) as ex:
            raise TransientError("Failed to push to queue", ex)

    def pop(self):
        from redis.exceptions import ConnectionError, TimeoutError
        try:
            string = self._pop()
            self.size -= 1
            return string
        except (ConnectionError, TimeoutError) as ex:
            logger.error("Failed to pop from queue: %s", str(ex), exc_info=True)
            return None

    @abstractmethod
    def _pop(self):
        pass

    def close(self):
        self.client.close()

    def __len__(self):
        from redis.exceptions import ConnectionError, TimeoutError
        # In case there is a connection error, use the cached queue size.
        # This allows a clean shutdown of Scrapy if there is a connection
        # problem.
        try:
            return self.client.llen(self.queue_name)
        except (ConnectionError, TimeoutError):
            return self.size


class _FifoRedisQueue(_RedisQueue):

    def _pop(self):
        return self.client.rpop(self.queue_name)


class _LifoRedisQueue(_RedisQueue):

    def _pop(self):
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

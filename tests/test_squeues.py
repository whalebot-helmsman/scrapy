import os
import pickle
import pytest
import sys

from queuelib.tests import test_queue as t
import redis.exceptions

from scrapy.squeues import (
    MarshalFifoDiskQueueNonRequest as MarshalFifoDiskQueue,
    MarshalLifoDiskQueueNonRequest as MarshalLifoDiskQueue,
    PickleFifoDiskQueueNonRequest as PickleFifoDiskQueue,
    PickleLifoDiskQueueNonRequest as PickleLifoDiskQueue,
    PickleFifoRedisQueueNonRequest as PickleFifoRedisQueue,
    PickleLifoRedisQueueNonRequest as PickleLifoRedisQueue,
)
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.settings import Settings
from tests.mockserver import RedisServer

import logging
logger = logging.getLogger(__name__)


class TestItem(Item):
    name = Field()


def _test_procesor(x):
    return x + x


class TestLoader(ItemLoader):
    default_item_class = TestItem
    name_out = staticmethod(_test_procesor)


def nonserializable_object_test(self):
    q = self.queue()
    self.assertRaises(ValueError, q.push, lambda x: x)
    # Selectors should fail (lxml.html.HtmlElement objects can't be pickled)
    sel = Selector(text='<html><body><p>some text</p></body></html>')
    self.assertRaises(ValueError, q.push, sel)


class FifoDiskQueueTestMixin:

    def test_serialize(self):
        q = self.queue()
        q.push('a')
        q.push(123)
        q.push({'a': 'dict'})
        self.assertEqual(q.pop(), 'a')
        self.assertEqual(q.pop(), 123)
        self.assertEqual(q.pop(), {'a': 'dict'})

    test_nonserializable_object = nonserializable_object_test


class MarshalFifoDiskQueueTest(t.FifoDiskQueueTest, FifoDiskQueueTestMixin):
    chunksize = 100000

    def queue(self):
        return MarshalFifoDiskQueue(self.qpath, chunksize=self.chunksize)


class ChunkSize1MarshalFifoDiskQueueTest(MarshalFifoDiskQueueTest):
    chunksize = 1


class ChunkSize2MarshalFifoDiskQueueTest(MarshalFifoDiskQueueTest):
    chunksize = 2


class ChunkSize3MarshalFifoDiskQueueTest(MarshalFifoDiskQueueTest):
    chunksize = 3


class ChunkSize4MarshalFifoDiskQueueTest(MarshalFifoDiskQueueTest):
    chunksize = 4


class PickleFifoDiskQueueTest(t.FifoDiskQueueTest, FifoDiskQueueTestMixin):

    chunksize = 100000

    def queue(self):
        return PickleFifoDiskQueue(self.qpath, chunksize=self.chunksize)

    def test_serialize_item(self):
        q = self.queue()
        i = TestItem(name='foo')
        q.push(i)
        i2 = q.pop()
        assert isinstance(i2, TestItem)
        self.assertEqual(i, i2)

    def test_serialize_loader(self):
        q = self.queue()
        loader = TestLoader()
        q.push(loader)
        loader2 = q.pop()
        assert isinstance(loader2, TestLoader)
        assert loader2.default_item_class is TestItem
        self.assertEqual(loader2.name_out('x'), 'xx')

    def test_serialize_request_recursive(self):
        q = self.queue()
        r = Request('http://www.example.com')
        r.meta['request'] = r
        q.push(r)
        r2 = q.pop()
        assert isinstance(r2, Request)
        self.assertEqual(r.url, r2.url)
        assert r2.meta['request'] is r2

    def test_non_pickable_object(self):
        q = self.queue()
        try:
            q.push(lambda x: x)
        except ValueError as exc:
            if hasattr(sys, "pypy_version_info"):
                self.assertIsInstance(exc.__context__, pickle.PicklingError)
            else:
                self.assertIsInstance(exc.__context__, AttributeError)
        sel = Selector(text='<html><body><p>some text</p></body></html>')
        try:
            q.push(sel)
        except ValueError as exc:
            self.assertIsInstance(exc.__context__, TypeError)


class ChunkSize1PickleFifoDiskQueueTest(PickleFifoDiskQueueTest):
    chunksize = 1


class ChunkSize2PickleFifoDiskQueueTest(PickleFifoDiskQueueTest):
    chunksize = 2


class ChunkSize3PickleFifoDiskQueueTest(PickleFifoDiskQueueTest):
    chunksize = 3


class ChunkSize4PickleFifoDiskQueueTest(PickleFifoDiskQueueTest):
    chunksize = 4


class LifoDiskQueueTestMixin:

    def test_serialize(self):
        q = self.queue()
        q.push('a')
        q.push(123)
        q.push({'a': 'dict'})
        self.assertEqual(q.pop(), {'a': 'dict'})
        self.assertEqual(q.pop(), 123)
        self.assertEqual(q.pop(), 'a')

    test_nonserializable_object = nonserializable_object_test


class MarshalLifoDiskQueueTest(t.LifoDiskQueueTest, LifoDiskQueueTestMixin):

    def queue(self):
        return MarshalLifoDiskQueue(self.qpath)


@pytest.mark.redis
class PickleLifoDiskQueueTest(t.LifoDiskQueueTest, LifoDiskQueueTestMixin):

    def queue(self):
        return PickleLifoDiskQueue(self.qpath)

    def test_serialize_item(self):
        q = self.queue()
        i = TestItem(name='foo')
        q.push(i)
        i2 = q.pop()
        assert isinstance(i2, TestItem)
        self.assertEqual(i, i2)

    def test_serialize_loader(self):
        q = self.queue()
        loader = TestLoader()
        q.push(loader)
        loader2 = q.pop()
        assert isinstance(loader2, TestLoader)
        assert loader2.default_item_class is TestItem
        self.assertEqual(loader2.name_out('x'), 'xx')

    def test_serialize_request_recursive(self):
        q = self.queue()
        r = Request('http://www.example.com')
        r.meta['request'] = r
        q.push(r)
        r2 = q.pop()
        assert isinstance(r2, Request)
        self.assertEqual(r.url, r2.url)
        assert r2.meta['request'] is r2


class RedisDiskQueueTestMixin:

    def get_settings(self):
        settings = Settings()
        settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_URL'] = self.redis_server.url
        return settings

    def setUp(self):
        self.redis_server = RedisServer()
        self.redis_server.start()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.redis_server.stop()

    def test_cleanup(self):
        """Test queue dir is not created"""
        q = self.queue()
        values = [b'0', b'1', b'2', b'3', b'4']
        assert not os.path.exists(self.qpath)
        for x in values:
            q.push(x)

        for x in values:
            q.pop()
        q.close()
        assert not os.path.exists(self.qpath)


@pytest.mark.redis
class RedisFifoDiskQueueTest(t.FifoTestMixin, RedisDiskQueueTestMixin,
                             t.PersistentTestMixin, t.QueuelibTestCase):

    def queue(self):
        return PickleFifoRedisQueue(self.qpath, self.get_settings())


@pytest.mark.redis
class RedisLifoDiskQueueTest(t.LifoTestMixin, RedisDiskQueueTestMixin,
                             t.PersistentTestMixin, t.QueuelibTestCase):

    def queue(self):
        return PickleLifoRedisQueue(self.qpath, self.get_settings())


class RedisQueueErrorTest(t.QueuelibTestCase):

    def test_missing_setting(self):
        """NotConfigured exception is raised if one of the required settings
        for Redis is None."""
        for setting in ['URL', 'PREFIX']:
            settings = Settings()
            key = 'SCHEDULER_EXTERNAL_QUEUE_REDIS_' + setting
            settings[key] = None
            with pytest.raises(NotConfigured) as excinfo:
                PickleFifoRedisQueue(self.qpath, settings)
            assert key in str(excinfo.value)

    def test_connection_error_length_0(self):
        """In case of a connection error, the length of the queue is 0."""
        settings = Settings()
        settings['SCHEDULER_EXTERNAL_QUEUE_REDIS_URL'] = (
            'redis://hostname.invalid:6379/0'
        )
        q = PickleFifoRedisQueue(self.qpath, settings)
        assert len(q) == 0
        with pytest.raises(redis.exceptions.ConnectionError):
            q.client.ping()
        assert len(q) == 0

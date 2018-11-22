import shutil
import tempfile
import unittest

from scrapy.core.queues import SCHEDULER_SLOT_META_KEY, scheduler_slot
from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import DummyStatsCollector

class MockCrawler:
    def __init__(self, settings):
        self.settings = Settings(settings)
        self.stats = DummyStatsCollector(self)


class SchedulerHandler:
    crawler_settings = dict(LOG_UNSERIALIZABLE_REQUESTS=False,
                            SCHEDULER_DISK_QUEUE='scrapy.squeues.PickleLifoDiskQueue',
                            SCHEDULER_MEMORY_QUEUE='scrapy.squeues.LifoMemoryQueue',
                            SCHEDULER_PRIORITY_QUEUE='queuelib.PriorityQueue',
                            JOBDIR=None,
                            DUPEFILTER_CLASS='scrapy.dupefilters.BaseDupeFilter')

    def create_scheduler(self):
        mock_crawler = MockCrawler(self.crawler_settings)
        self.scheduler = Scheduler.from_crawler(mock_crawler)
        self.scheduler.open(Spider(name='spider'))

    def close_scheduler(self):
        self.scheduler.close('finished')

    def setUp(self):
        self.create_scheduler()

    def tearDown(self):
        self.close_scheduler()


class BaseSchedulerInMemoryTester(SchedulerHandler):
    def test_length(self):
        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        self.scheduler.enqueue_request(Request("http://foo.com/a"))
        self.scheduler.enqueue_request(Request("http://foo.com/a"))

        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 2)

    def test_dequeue(self):
        _URLS = {"http://foo.com/a", "http://foo.com/b", "http://foo.com/c"}
        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        urls = set()
        while self.scheduler.has_pending_requests():
            urls.add(self.scheduler.next_request().url)

        self.assertEqual(urls, _URLS)

    def test_dequeue_priorities(self):
        _PRIORITIES = [("http://foo.com/a", -2),
                       ("http://foo.com/d", 1),
                       ("http://foo.com/b", -1),
                       ("http://foo.com/c", 0),
                       ("http://foo.com/e", 2)]

        for url, priority in _PRIORITIES:
            self.scheduler.enqueue_request(Request(url, priority=priority))

        priorities = list()
        while self.scheduler.has_pending_requests():
            priorities.append(self.scheduler.next_request().priority)

        self.assertEqual(priorities, sorted([x[1] for x in _PRIORITIES], key=lambda x: -x))


class BaseSchedulerOnDiskTester(SchedulerHandler):

    def setUp(self):
        self.old_jobdir = self.crawler_settings['JOBDIR']
        self.directory = tempfile.mkdtemp()
        self.crawler_settings['JOBDIR'] = self.directory

        self.create_scheduler()

    def tearDown(self):
        self.close_scheduler()

        shutil.rmtree(self.directory)
        self.directory = None
        self.crawler_settings['JOBDIR'] = self.old_jobdir
        self.old_jobdir = None

    def test_length(self):
        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        self.scheduler.enqueue_request(Request("http://foo.com/a"))
        self.scheduler.enqueue_request(Request("http://foo.com/a"))

        self.close_scheduler()
        self.create_scheduler()

        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 2)

    def test_dequeue(self):
        _URLS = {"http://foo.com/a", "http://foo.com/b", "http://foo.com/c"}
        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        self.close_scheduler()
        self.create_scheduler()

        urls = set()
        while self.scheduler.has_pending_requests():
            urls.add(self.scheduler.next_request().url)

        self.assertEqual(urls, _URLS)

    def test_dequeue_priorities(self):
        _PRIORITIES = [("http://foo.com/a", -2),
                       ("http://foo.com/d", 1),
                       ("http://foo.com/c", 0),
                       ("http://foo.com/b", -1),
                       ("http://foo.com/e", 2)]

        for url, priority in _PRIORITIES:
            self.scheduler.enqueue_request(Request(url, priority=priority))

        self.close_scheduler()
        self.create_scheduler()

        priorities = list()
        while self.scheduler.has_pending_requests():
            priorities.append(self.scheduler.next_request().priority)

        self.assertEqual(priorities, sorted([x[1] for x in _PRIORITIES], key=lambda x: -x))


class TestSchedulerInMemory(BaseSchedulerInMemoryTester, unittest.TestCase):
    pass


class TestSchedulerOnDisk(BaseSchedulerOnDiskTester, unittest.TestCase):
    pass


class TestSchedulerWithRoundRobinInMemory(BaseSchedulerInMemoryTester, unittest.TestCase):
    crawler_settings = dict(LOG_UNSERIALIZABLE_REQUESTS=False,
                            SCHEDULER_DISK_QUEUE='scrapy.squeues.PickleLifoDiskQueue',
                            SCHEDULER_MEMORY_QUEUE='scrapy.squeues.LifoMemoryQueue',
                            SCHEDULER_PRIORITY_QUEUE='scrapy.core.queues.RoundRobinQueue',
                            JOBDIR=None,
                            DUPEFILTER_CLASS='scrapy.dupefilters.BaseDupeFilter')

    def test_round_robin(self):
        _SLOTS = [("http://foo.com/a", 'a'),
                  ("http://foo.com/b", 'a'),
                  ("http://foo.com/c", 'b'),
                  ("http://foo.com/d", 'b'),
                  ("http://foo.com/d", 'c'),
                  ("http://foo.com/e", 'c')]

        for url, slot in _SLOTS:
            request = Request(url, meta={SCHEDULER_SLOT_META_KEY: slot})
            self.scheduler.enqueue_request(request)

        slots = list()
        while self.scheduler.has_pending_requests():
            slots.append(scheduler_slot(self.scheduler.next_request()))

        for i in range(0, len(_SLOTS), 2):
            self.assertNotEqual(slots[i], slots[i+1])


class TestSchedulerWithRoundRobinOnDisk(BaseSchedulerOnDiskTester, unittest.TestCase):
    crawler_settings = dict(LOG_UNSERIALIZABLE_REQUESTS=False,
                            SCHEDULER_DISK_QUEUE='scrapy.squeues.PickleLifoDiskQueue',
                            SCHEDULER_MEMORY_QUEUE='scrapy.squeues.LifoMemoryQueue',
                            SCHEDULER_PRIORITY_QUEUE='scrapy.core.queues.RoundRobinQueue',
                            JOBDIR=None,
                            DUPEFILTER_CLASS='scrapy.dupefilters.BaseDupeFilter')

    def test_round_robin(self):
        _SLOTS = [("http://foo.com/a", 'a'),
                  ("http://foo.com/b", 'a'),
                  ("http://foo.com/c", 'b'),
                  ("http://foo.com/d", 'b'),
                  ("http://foo.com/d", 'c'),
                  ("http://foo.com/e", 'c')]

        for url, slot in _SLOTS:
            request = Request(url, meta={SCHEDULER_SLOT_META_KEY: slot})
            self.scheduler.enqueue_request(request)

        self.close_scheduler()
        self.create_scheduler()

        slots = list()
        while self.scheduler.has_pending_requests():
            slots.append(scheduler_slot(self.scheduler.next_request()))

        for i in range(0, len(_SLOTS), 2):
            self.assertNotEqual(slots[i], slots[i+1])

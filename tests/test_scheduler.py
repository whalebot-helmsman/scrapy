import unittest

from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from scrapy.settings import Settings
import scrapy.settings.default_settings as DEFAULT_SETTINGS
from scrapy.statscollectors import DummyStatsCollector

class MockCrawler:
    def __init__(self, settings):
        self.settings = Settings(settings)
        self.stats = DummyStatsCollector(self)


class MockSpider:
    pass


class BaseSchedulerInMemoryTester:
    scheduler_cls = None
    crawler_settings = None

    def setUp(self):
        mock_crawler = MockCrawler(self.crawler_settings)
        self.scheduler = self.scheduler_cls.from_crawler(mock_crawler)
        self.scheduler.open(MockSpider)

    def tearDown(self):
        self.scheduler.close('finished')

    def testLength(self):
        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        self.scheduler.enqueue_request(Request("http://foo.com/a"))
        self.scheduler.enqueue_request(Request("http://foo.com/a"))

        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 2)

    def testDequeue(self):
        _URLS = {"http://foo.com/a", "http://foo.com/b", "http://foo.com/c"}
        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        urls = set()
        while self.scheduler.has_pending_requests():
            urls.add(self.scheduler.next_request().url)

        self.assertEqual(urls, _URLS)

    def testDequeuePriorities(self):
        _PRIORITIES = {"http://foo.com/a": 0,
                       "http://foo.com/b": 1,
                       "http://foo.com/c": 2}

        for url, priority in _PRIORITIES.items():
            self.scheduler.enqueue_request(Request(url, priority=priority))

        priorities = list()
        while self.scheduler.has_pending_requests():
            priorities.append(self.scheduler.next_request().priority)

        self.assertEqual(priorities, sorted(_PRIORITIES.values(), key=lambda x: -x))



class TestSchedulerInMemory(BaseSchedulerInMemoryTester, unittest.TestCase):
    scheduler_cls = Scheduler
    crawler_settings = dict(LOG_UNSERIALIZABLE_REQUESTS=False,
                            SCHEDULER_DISK_QUEUE=DEFAULT_SETTINGS.SCHEDULER_DISK_QUEUE,
                            SCHEDULER_MEMORY_QUEUE=DEFAULT_SETTINGS.SCHEDULER_MEMORY_QUEUE,
                            SCHEDULER_PRIORITY_QUEUE=DEFAULT_SETTINGS.SCHEDULER_PRIORITY_QUEUE,
                            JOBDIR=None,
                            DUPEFILTER_CLASS='scrapy.dupefilters.BaseDupeFilter')



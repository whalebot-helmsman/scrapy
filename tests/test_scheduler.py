import contextlib
import shutil
import tempfile
import unittest

from scrapy.pqueues import SCHEDULER_SLOT_META_KEY, scheduler_slot
from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import DummyStatsCollector

class MockCrawler:
    def __init__(self, priority_queue_cls, jobdir):

        settings = dict(LOG_UNSERIALIZABLE_REQUESTS=False,
                       SCHEDULER_DISK_QUEUE='scrapy.squeues.PickleLifoDiskQueue',
                       SCHEDULER_MEMORY_QUEUE='scrapy.squeues.LifoMemoryQueue',
                       SCHEDULER_PRIORITY_QUEUE=priority_queue_cls,
                       JOBDIR=jobdir,
                       DUPEFILTER_CLASS='scrapy.dupefilters.BaseDupeFilter')

        self.settings = Settings(settings)
        self.stats = DummyStatsCollector(self)


class SchedulerHandler:
    priority_queue_cls = None
    jobdir = None

    def create_scheduler(self):
        mock_crawler = MockCrawler(self.priority_queue_cls, self.jobdir)
        self.scheduler = Scheduler.from_crawler(mock_crawler)
        self.scheduler.open(Spider(name='spider'))

    def close_scheduler(self):
        self.scheduler.close('finished')

    def setUp(self):
        self.create_scheduler()

    def tearDown(self):
        self.close_scheduler()


_PRIORITIES = [("http://foo.com/a", -2),
               ("http://foo.com/d", 1),
               ("http://foo.com/b", -1),
               ("http://foo.com/c", 0),
               ("http://foo.com/e", 2)]


_URLS = {"http://foo.com/a", "http://foo.com/b", "http://foo.com/c"}


class BaseSchedulerInMemoryTester(SchedulerHandler):
    def test_length(self):
        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), len(_URLS))

    def test_dequeue(self):
        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        urls = set()
        while self.scheduler.has_pending_requests():
            urls.add(self.scheduler.next_request().url)

        self.assertEqual(urls, _URLS)

    def test_dequeue_priorities(self):
        for url, priority in _PRIORITIES:
            self.scheduler.enqueue_request(Request(url, priority=priority))

        priorities = list()
        while self.scheduler.has_pending_requests():
            priorities.append(self.scheduler.next_request().priority)

        self.assertEqual(priorities, sorted([x[1] for x in _PRIORITIES], key=lambda x: -x))


class BaseSchedulerOnDiskTester(SchedulerHandler):

    def setUp(self):
        self.jobdir = tempfile.mkdtemp()
        self.create_scheduler()

    def tearDown(self):
        self.close_scheduler()

        shutil.rmtree(self.jobdir)
        self.jobdir = None

    def test_length(self):
        self.assertFalse(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), 0)

        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        self.close_scheduler()
        self.create_scheduler()

        self.assertTrue(self.scheduler.has_pending_requests())
        self.assertEqual(len(self.scheduler), len(_URLS))

    def test_dequeue(self):
        for url in _URLS:
            self.scheduler.enqueue_request(Request(url))

        self.close_scheduler()
        self.create_scheduler()

        urls = set()
        while self.scheduler.has_pending_requests():
            urls.add(self.scheduler.next_request().url)

        self.assertEqual(urls, _URLS)

    def test_dequeue_priorities(self):
        for url, priority in _PRIORITIES:
            self.scheduler.enqueue_request(Request(url, priority=priority))

        self.close_scheduler()
        self.create_scheduler()

        priorities = list()
        while self.scheduler.has_pending_requests():
            priorities.append(self.scheduler.next_request().priority)

        self.assertEqual(priorities, sorted([x[1] for x in _PRIORITIES], key=lambda x: -x))


class TestSchedulerInMemory(BaseSchedulerInMemoryTester, unittest.TestCase):
    priority_queue_cls = 'queuelib.PriorityQueue'


class TestSchedulerOnDisk(BaseSchedulerOnDiskTester, unittest.TestCase):
    priority_queue_cls = 'queuelib.PriorityQueue'


_SLOTS = [("http://foo.com/a", 'a'),
          ("http://foo.com/b", 'a'),
          ("http://foo.com/c", 'b'),
          ("http://foo.com/d", 'b'),
          ("http://foo.com/d", 'c'),
          ("http://foo.com/e", 'c')]


class TestSchedulerWithRoundRobinInMemory(BaseSchedulerInMemoryTester, unittest.TestCase):
    priority_queue_cls = 'scrapy.pqueues.RoundRobinQueue'

    def test_round_robin(self):
        for url, slot in _SLOTS:
            request = Request(url, meta={SCHEDULER_SLOT_META_KEY: slot})
            self.scheduler.enqueue_request(request)

        slots = list()
        while self.scheduler.has_pending_requests():
            slots.append(scheduler_slot(self.scheduler.next_request()))

        for i in range(0, len(_SLOTS), 2):
            self.assertNotEqual(slots[i], slots[i+1])

    def test_is_meta_set(self):
        url = "http://foo.com/a"
        request = Request(url)
        if SCHEDULER_SLOT_META_KEY in request.meta:
            del request.meta[SCHEDULER_SLOT_META_KEY]
        self.scheduler.enqueue_request(request)
        self.assertIsNotNone(request.meta.get(SCHEDULER_SLOT_META_KEY, None))


class TestSchedulerWithRoundRobinOnDisk(BaseSchedulerOnDiskTester, unittest.TestCase):
    priority_queue_cls = 'scrapy.pqueues.RoundRobinQueue'

    def test_round_robin(self):
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

    def test_is_meta_set(self):
        url = "http://foo.com/a"
        request = Request(url)
        if SCHEDULER_SLOT_META_KEY in request.meta:
            del request.meta[SCHEDULER_SLOT_META_KEY]
        self.scheduler.enqueue_request(request)

        self.close_scheduler()
        self.create_scheduler()

        self.assertIsNotNone(request.meta.get(SCHEDULER_SLOT_META_KEY, None))


@contextlib.contextmanager
def mkdtemp():
    dir = tempfile.mkdtemp()
    yield dir
    shutil.rmtree(dir)


def _migration():

    with mkdtemp() as tmp_dir:
        prev_scheduler_handler = SchedulerHandler()
        prev_scheduler_handler.priority_queue_cls = 'queuelib.PriorityQueue'
        prev_scheduler_handler.jobdir = tmp_dir

        prev_scheduler_handler.create_scheduler()
        for url in _URLS:
            prev_scheduler_handler.scheduler.enqueue_request(Request(url))
        prev_scheduler_handler.close_scheduler()

        next_scheduler_handler = SchedulerHandler()
        next_scheduler_handler.priority_queue_cls = 'scrapy.pqueues.RoundRobinQueue'
        next_scheduler_handler.jobdir = tmp_dir

        next_scheduler_handler.create_scheduler()


class TestMigration(unittest.TestCase):
    def test_migration(self):
        self.assertRaises(ValueError, _migration)

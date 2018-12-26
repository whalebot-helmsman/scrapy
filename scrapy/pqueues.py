import hashlib
import logging
from collections import namedtuple
from six.moves.urllib.parse import urlparse

from queuelib import PriorityQueue

from scrapy.core.downloader import Downloader
from scrapy.http import Request
from scrapy.signals import request_reached_downloader, response_downloaded
from scrapy.utils.httpobj import urlparse_cached


logger = logging.getLogger(__name__)


SCHEDULER_SLOT_META_KEY = Downloader.DOWNLOAD_SLOT


def _get_from_request(request, key, default=None):
    if isinstance(request, dict):
        return request.get(key, default)

    if isinstance(request, Request):
        return getattr(request, key, default)

    raise ValueError('Bad type of request "%s"' % (request.__class__, ))


def _scheduler_slot_read(request, default=None):
    return request.meta.get(SCHEDULER_SLOT_META_KEY, default)


def _scheduler_slot_write(request, slot):
    request.meta[SCHEDULER_SLOT_META_KEY] = slot


def _scheduler_slot(request):
    meta = _get_from_request(request, "meta", {})
    slot = meta.get(SCHEDULER_SLOT_META_KEY, None)

    if slot is not None:
        return slot

    if isinstance(request, dict):
        url = request.get('url', None)
        slot = urlparse(url).hostname or ''
    elif isinstance(request, Request):
        slot = urlparse_cached(request).hostname or ''

    meta[SCHEDULER_SLOT_META_KEY] = slot

    return slot


def _path_safe(text):
    """ Return a filesystem-safe version of a string ``text`` """
    pathable_slot = "".join([c if c.isalnum() or c in '-._' else '_'
                             for c in text])
    # as we replace some letters we can get collision for different slots
    # add we add unique part
    unique_slot = hashlib.md5(text.encode('utf8')).hexdigest()
    return '-'.join([pathable_slot, unique_slot])


class PrioritySlot(namedtuple("PrioritySlot", ["priority", "slot"])):
    """ ``(priority, slot)`` tuple which uses a path-safe slot name
    when converting to str """
    def __str__(self):
        return '%s_%s' % (self.priority, _path_safe(str(self.slot)))


class PriorityAsTupleQueue(PriorityQueue):
    """
    Python structures is not directly (de)serialized (to)from json.
    We need this modified queue to transform custom structure (from)to
    json serializable structures
    """
    def __init__(self, qfactory, startprios=()):
        startprios = [PrioritySlot(priority=p[0], slot=p[1])
                      for p in startprios]
        super(PriorityAsTupleQueue, self).__init__(
            qfactory=qfactory,
            startprios=startprios)


class SlotBasedPriorityQueue(object):

    def __init__(self, qfactory, startprios={}):
        self.pqueues = dict()     # slot -> priority queue
        self.qfactory = qfactory  # factory for creating new internal queues

        if not startprios:
            return

        if not isinstance(startprios, dict):
            raise ValueError("Looks like your priorities file malforfemed. "
                             "Possible reason: You run scrapy with previous "
                             "version. Interrupted it. Updated scrapy. And "
                             "run again.")

        for slot, prios in startprios.items():
            self.pqueues[slot] = PriorityAsTupleQueue(self.qfactory, prios)

    def pop_slot(self, slot):
        queue = self.pqueues[slot]
        request = queue.pop()
        if len(queue) == 0:
            del self.pqueues[slot]
        return request

    def push_slot(self, request, priority):
        slot = _scheduler_slot(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = PriorityAsTupleQueue(self.qfactory)
        queue = self.pqueues[slot]
        queue.push(request, PrioritySlot(priority=priority, slot=slot))
        return slot

    def close(self):
        startprios = dict()
        for slot, queue in self.pqueues.items():
            prios = queue.close()
            startprios[slot] = prios
        self.pqueues.clear()
        return startprios

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0


class DownloaderAwarePriorityQueue(SlotBasedPriorityQueue):

    _DOWNLOADER_AWARE_PQ_ID = 'DOWNLOADER_AWARE_PQ_ID'

    @classmethod
    def from_crawler(cls, crawler, qfactory, startprios={}):
        return cls(crawler, qfactory, startprios)

    def __init__(self, crawler, qfactory, startprios={}):
        ip_concurrency_key = 'CONCURRENT_REQUESTS_PER_IP'
        ip_concurrency = crawler.settings.getint(ip_concurrency_key, 0)

        if ip_concurrency > 0:
            raise ValueError('"%s" does not support %s=%d' % (self.__class__,
                                                              ip_concurrency_key,
                                                              ip_concurrency))

        super(DownloaderAwarePriorityQueue, self).__init__(qfactory,
                                                           startprios)
        self._slots = {slot: 0 for slot in self.pqueues}
        crawler.signals.connect(self.on_response_download,
                                signal=response_downloaded)
        crawler.signals.connect(self.on_request_reached_downloader,
                                signal=request_reached_downloader)

    def mark(self, request):
        meta = _get_from_request(request, 'meta', None)
        if not isinstance(meta, dict):
            raise ValueError('No meta attribute in %s' % (request, ))
        meta[self._DOWNLOADER_AWARE_PQ_ID] = id(self)

    def check_mark(self, request):
        return request.meta.get(self._DOWNLOADER_AWARE_PQ_ID, None) == id(self)

    def pop(self):
        slots = [(d, s) for s, d in self._slots.items() if s in self.pqueues]

        if not slots:
            return

        slot = min(slots)[1]
        request = self.pop_slot(slot)
        self.mark(request)
        return request

    def push(self, request, priority):
        slot = self.push_slot(request, priority)
        if slot not in self._slots:
            self._slots[slot] = 0

    def on_response_download(self, response, request, spider):
        if not self.check_mark(request):
            return

        slot = _scheduler_slot_read(request)
        if slot not in self._slots or self._slots[slot] <= 0:
            raise ValueError('Get response for wrong slot "%s"' % (slot, ))
        self._slots[slot] = self._slots[slot] - 1
        if self._slots[slot] == 0 and slot not in self.pqueues:
            del self._slots[slot]

    def on_request_reached_downloader(self, request, spider):
        if not self.check_mark(request):
            return

        slot = _scheduler_slot_read(request)
        self._slots[slot] = self._slots.get(slot, 0) + 1

    def close(self):
        self._slots.clear()
        return super(DownloaderAwarePriorityQueue, self).close()

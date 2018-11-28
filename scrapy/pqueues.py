from collections import deque
import hashlib
import logging
from six.moves.urllib.parse import urlparse

from queuelib import PriorityQueue

from scrapy.core.downloader import Downloader
from scrapy.http import Request


logger = logging.getLogger(__name__)


SCHEDULER_SLOT_META_KEY = Downloader.DOWNLOAD_SLOT


def _get_from_request(request, key, default=None):
    if isinstance(request, dict):
        return request.get(key, default)

    if isinstance(request, Request):
        return getattr(request, key, default)

    raise ValueError('Bad type of request "%s"' % (request.__class__, ))


def scheduler_slot(request):
    meta = _get_from_request(request, 'meta', dict())
    slot = meta.get(SCHEDULER_SLOT_META_KEY, None)

    if slot is None:
        url = _get_from_request(request, 'url')
        slot = urlparse(url).hostname or ''
        meta[SCHEDULER_SLOT_META_KEY] = slot

    return str(slot)


def _slot_as_path(slot):
    pathable_slot = "".join([c if c.isalnum() or c in '-._' else '_' for c in slot])

    """
        as we replace some letters we can get collision for different slots
        add we add unique part
    """
    unique_slot = hashlib.md5(slot.encode('utf8')).hexdigest()

    return '-'.join([pathable_slot, unique_slot])


class PriorityAsTupleQueue(PriorityQueue):
    """
        Tuple is serialized into json as a list, which is unhashable. We need
        this modified class to directly convert it to tuple.
    """
    def __init__(self, qfactory, startprios=()):
        super(PriorityAsTupleQueue, self).__init__(
                qfactory,
                [tuple(p) for p in startprios]
                )


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
        is_empty = len(queue) == 0
        if is_empty:
            del self.pqueues[slot]

        return request, is_empty

    def push_slot(self, request, priority):
        slot = scheduler_slot(request)
        is_new = False
        if slot not in self.pqueues:
            is_new = True
            self.pqueues[slot] = PriorityAsTupleQueue(self.qfactory)
        self.pqueues[slot].push(request, (priority, _slot_as_path(slot)))
        return slot, is_new

    def close(self):
        startprios = dict()
        for slot, queue in self.pqueues.items():
            prios = queue.close()
            startprios[slot] = prios
        self.pqueues.clear()
        return startprios

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0


class RoundRobinPriorityQueue(SlotBasedPriorityQueue):

    def __init__(self, qfactory, startprios={}):
        super(RoundRobinPriorityQueue, self).__init__(qfactory, startprios)
        self._slots = deque()
        for slot in self.pqueues:
            self._slots.append(slot)

    def push(self, request, priority):
        slot, is_new = self.push_slot(request, priority)
        if is_new:
            self._slots.append(slot)

    def pop(self):
        if not self._slots:
            return

        slot = self._slots.popleft()
        request, is_empty = self.pop_slot(slot)

        if not is_empty:
            self._slots.append(slot)

        return request

    def close(self):
        self._slots.clear()
        return super(RoundRobinPriorityQueue, self).close()


class DownloaderAwarePriorityQueue(SlotBasedPriorityQueue):

    @classmethod
    def from_crawler(cls, crawler, qfactory, startprios={}):
        return cls(crawler, qfactory, startprios)

    def __init__(self, crawler, qfactory, startprios={}):
        super(DownloaderAwarePriorityQueue, self).__init__(qfactory, startprios)
        self._slots = {slot: 0 for slot in self.pqueues}

    def pop(self):
        slots = [(s, d) for s,d in self._slots.items() if s in self.pqueues]
        slots.sort(key=lambda p:(p[1], p[0]))

        if not slots:
            return

        slot = slots[0][0]
        request, _ = self.pop_slot(slot)
        return request

    def push(self, request, priority):
        slot, _ = self.push_slot(request, priority)
        if slot not in self._slots:
            self._slots[slot] = 0

    def on_response_download(self, response, request, spider):
        slot = scheduler_slot(request)
        if slot not in self._slots or self._slots[slot] <= 0:
            raise ValueError('Get response for wrong slot "%s"' % (slot, ))
        self._slots[slot] = self._slots[slot] - 1
        if self._slots[slot] == 0 and slot not in self.pqueues:
            del self._slots[slot]

    def on_request_reached_downloader(self, request, spider):
        slot = scheduler_slot(request)
        self._slots[slot] = self._slots.get(slot, 0) + 1

    def close(self):
        self._slots.clear()
        return super(DownloaderAwarePriorityQueue, self).close()

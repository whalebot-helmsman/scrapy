from collections import deque
import hashlib
import logging

from queuelib import PriorityQueue


logger = logging.getLogger(__name__)


SCHEDULER_SLOT_META_KEY = 'downloader_slot'


def scheduler_slot(request):
    meta = dict()
    if isinstance(request, dict):
        meta = request.get('meta', dict())
    else:
        meta = getattr(request, 'meta', dict())

    slot = meta.get(SCHEDULER_SLOT_META_KEY, None)
    return str(slot)


def _make_file_safe(string):
    """
    Make string file safe but readable.
    """
    clean_string = "".join([c if c.isalnum() or c in '-._' else '_' for c in string])
    hash_string = hashlib.md5(string.encode('utf8')).hexdigest()
    return "{}-{}".format(clean_string[:40], hash_string[:10])


def _transform_priority(priority):
    return str(2**32 + priority)


class RoundRobinQueue:

    def __init__(self, qfactory, startprios={}):
        self._slots = deque()
        self.pqueues = dict()     # slot -> priority queue
        self.qfactory = qfactory  # factory for creating new internal queues

        if not startprios:
            return

        for slot, prios in startprios.items():
            self._slots.append(slot)
            self.pqueues = PriorityQueue(self.qfactory, prios)

    def push(self, request, priority):

        slot = scheduler_slot(request)
        logger.debug('Push "%s"', slot)
        if slot not in self.pqueues:
            self.pqueues[slot] = PriorityQueue(self.qfactory)
            self._slots.append(slot)
        priority = '-'.join([_transform_priority(priority), _make_file_safe(slot)])
        self.pqueues[slot].push(request, priority)

    def pop(self):
        if not self._slots:
            return
        slot = self._slots.popleft()
        logger.debug('Pop "%s"', slot)
        queue = self.pqueues[slot]
        request = queue.pop()

        if len(queue):
            self._slots.append(slot)
        else:
            del self.pqueues[slot]
        return request

    def close(self):
        startprios = dict()
        for slot, queue in self.pqueues.items():
            prios = queue.close()
            startprios[slot] = prios
        self.pqueues.clear()
        self._slots.clear()
        return startprios

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0

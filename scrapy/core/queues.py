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


_VERY_BIG = 2**32
def _get_priority(slot, priority):
    """
        We want to provide some kind of fake priority for PriorityQueue.
        It should meet next requirements:

        - should be writeable/readable to/from json without special handlers
        - should be comparable
        - should be hashasble
        - should be different for different slots
        - should be in same order as priority
        - should be useable as a path
    """

    """
        we add _VERY_BIG to number so negative numbers are positive and their
        string representation is smaller
    """
    priority_part = str(_VERY_BIG + priority)

    """
        made slot value be used as a path
    """
    pathable_slot = "".join([c if c.isalnum() or c in '-._' else '_' for c in slot])

    """
        as we replace some letters we can get collision for different slots
        add we add unique part
    """
    unique_slot = hashlib.md5(slot.encode('utf8')).hexdigest()

    return '-'.join([priority_part, pathable_slot, unique_slot])



class RoundRobinQueue:

    def __init__(self, qfactory, startprios={}):
        self._slots = deque()
        self.pqueues = dict()     # slot -> priority queue
        self.qfactory = qfactory  # factory for creating new internal queues

        if not startprios:
            return

        if not isinstance(startprios, dict):
            raise ValueError("Looks like your priorities file malforfemed. ",
                             "Possible reason: You run scrapy with previous ",
                             "version. Interrupted it. Updated scrapy. And ",
                             "run again.")

        for slot, prios in startprios.items():
            self._slots.append(slot)
            self.pqueues[slot] = PriorityQueue(self.qfactory, prios)

    def push(self, request, priority):

        slot = scheduler_slot(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = PriorityQueue(self.qfactory)
            self._slots.append(slot)
        self.pqueues[slot].push(request, _get_priority(slot, priority))

    def pop(self):
        if not self._slots:
            return
        slot = self._slots.popleft()
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

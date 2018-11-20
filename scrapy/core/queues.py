from collections import deque
import logging
import uuid
import os.path


from queuelib import PriorityQueue
from queuelib.queue import (
        FifoDiskQueue,
        LifoDiskQueue,
        FifoSQLiteQueue,
        LifoSQLiteQueue,
        )


logger = logging.getLogger(__name__)


def unique_files_queue(queue_class):

    class UniqueFilesQueue(queue_class):
        def __init__(self, path):
            path = path + "-" + uuid.uuid4().hex
            while os.path.exists(path):
                path = path + "-" + uuid.uuid4().hex

            super().__init__(path)

    return UniqueFilesQueue


UniqueFileFifoDiskQueue = unique_files_queue(FifoDiskQueue)
UniqueFileLifoDiskQueue = unique_files_queue(LifoDiskQueue)
UniqueFileFifoSQLiteQueue = unique_files_queue(FifoSQLiteQueue)
UniqueFileLifoSQLiteQueue = unique_files_queue(LifoSQLiteQueue)


SCHEDULER_SLOT_META_KEY = 'downloader_slot'


def scheduler_slot(request):
    meta = dict()
    if isinstance(request, dict):
        meta = request.get('meta', dict())
    else:
        meta = getattr(request, 'meta', dict())

    slot = meta.get(SCHEDULER_SLOT_META_KEY, None)
    return slot


class RoundRobinQueue:

    def __init__(self, qfactory, startprios=()):
        self._slots = deque()
        self.pqueues = dict()     # slot -> priority queue
        self.qfactory = qfactory  # factory for creating new internal queues

        for slot, prios in startprios.items():
            self._slots.append(slot)
            self.pqueues = PriorityQueue(self.qfactory, prios)

    def push(self, request, priority):

        slot = scheduler_slot(request)
        logger.debug('Push "%s"', slot)
        if slot not in self.pqueues:
            self.pqueues[slot] = PriorityQueue(self.qfactory)
            self._slots.append(slot)
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

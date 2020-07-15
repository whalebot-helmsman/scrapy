.. _topics-queues:

======
Queues
======

Scrapy uses queues to schedule requests. By default, a memory-based queue is
used but if :ref:`crawls should be paused and resumed <topics-jobs>`, using an
external queue (disk queue) is necessary. The setting
:setting:`SCHEDULER_DISK_QUEUE` determines the type of disk queue that will be
used by the scheduler.

If you want to use your own disk queue, it has to conform to the following
interface:

.. class:: MyExternalQueue

   .. method:: __init__(self, path)

      The constructor receives the ``path`` argument which identifies the
      queue. The constructor can also access ``self.settings`` which contains
      the currently active settings.

      The constructor is expected to verify the arguments and the relevant
      settings and raise a ``ValueError`` in case of an error. This may
      involve opening a connection to a remote service.

      :raises ValueError: If ``path`` or a queue-specific setting is invalid.

   .. method:: push(self, string)

      Pushes the string ``string`` on the queue and increases the cached
      number of elements.

      If the push fails because of a temporary problem (e.g. the connection
      was dropped), a ``TransientError`` should be raised. This causes the
      caller to fall back to a memory queue.

      :raises TransientError: If pushing to the queue failed due to a
          temporary error.

   .. method:: pop(self)

      Pops a string from the queue and decreases the cached number of
      elements. In case of a temporary problem, ``None`` is returned (and the
      number of elements is not decreased).

      It is up to the queue implementation to decide if the most recently
      pushed value (LIFO) or the least recently pushed value (FIFO) is
      returned.

      .. note::
         In case of a temporary error, the method must not raise an exception
         but return ``None`` instead.

   .. method:: close(self)

      Releases internal handlers (e.g. closes file or socket).

   .. method:: __len__(self)

      Returns the number of elements in the queue. If the number of elements
      cannot be determined (e.g. because of a connection problem), the cached
      number of elements is returned.

      .. note::
         In case of a temporary error, the method must not raise an exception
         but return the cached number of elements instead.

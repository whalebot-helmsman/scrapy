"""
Helper functions for serializing (and deserializing) requests.
"""
import inspect
from typing import Optional

from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy.utils.python import to_unicode
from scrapy.utils.misc import load_object


def request_to_dict(request: Request, spider: Optional[Spider] = None):
    """Convert *request* into a :class:`dict`.

    If *spider* is not ``None``, find out the name of the spider methods used
    as request callback and errback, and replace the callables with their names
    in the result.
    """
    cb = request.callback
    if callable(cb):
        cb = _find_method(spider, cb)
    eb = request.errback
    if callable(eb):
        eb = _find_method(spider, eb)
    d = {
        'url': to_unicode(request.url),  # urls should be safe (safe_string_url)
        'callback': cb,
        'errback': eb,
        'method': request.method,
        'headers': dict(request.headers),
        'body': request.body,
        'cookies': request.cookies,
        'meta': request.meta,
        '_encoding': request._encoding,
        'priority': request.priority,
        'dont_filter': request.dont_filter,
        'flags': request.flags,
        'cb_kwargs': request.cb_kwargs,
    }
    if type(request) is not Request:
        d['_class'] = request.__module__ + '.' + request.__class__.__name__
    return d


def request_from_dict(d: dict, spider: Optional[Spider] = None) -> Request:
    """Create a :class:`~scrapy.http.Request` object from *d*.

    If *spider* is not ``None``, resolve the callback and errback attributes
    to spider methods with the same name.
    """
    cb = d['callback']
    if cb and spider:
        cb = _get_method(spider, cb)
    eb = d['errback']
    if eb and spider:
        eb = _get_method(spider, eb)
    request_cls = load_object(d['_class']) if '_class' in d else Request
    return request_cls(
        url=to_unicode(d['url']),
        callback=cb,
        errback=eb,
        method=d['method'],
        headers=d['headers'],
        body=d['body'],
        cookies=d['cookies'],
        meta=d['meta'],
        encoding=d['_encoding'],
        priority=d['priority'],
        dont_filter=d['dont_filter'],
        flags=d.get('flags'),
        cb_kwargs=d.get('cb_kwargs'),
    )


def _find_method(obj, func):
    # Only instance methods contain ``__func__``
    if obj and hasattr(func, '__func__'):
        members = inspect.getmembers(obj, predicate=inspect.ismethod)
        for name, obj_func in members:
            # We need to use __func__ to access the original
            # function object because instance method objects
            # are generated each time attribute is retrieved from
            # instance.
            #
            # Reference: The standard type hierarchy
            # https://docs.python.org/3/reference/datamodel.html
            if obj_func.__func__ is func.__func__:
                return name
    raise ValueError(f"Function {func} is not an instance method in: {obj}")


def _get_method(obj, name):
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        raise ValueError(f"Method {name!r} not found in: {obj}")

# coding=utf-8
from pava.decorator.decorator_impl.type_check_decorator import type_check
from pava.utils.object_utils import NoneType

"""
HTTP请求结果的请求方包装对象
"""


class HTTPRequestResponse(object):
    @type_check(None, [str, NoneType], [int, NoneType], None)
    def __init__(self, url, status_code, content):
        self.url = url
        self.status_code = status_code
        self.content = content

    def is_success(self):
        return str(self.status_code).startswith('2')

    def is_failed(self):
        return not str(self.status_code).startswith('2')

    def get_content(self):
        if self.is_failed():
            raise Exception(
                "[request_response] Cannot trans to python object, http status code is %s, url: %s" % (
                    self.status_code, self.url
                )
            )
        return self.content

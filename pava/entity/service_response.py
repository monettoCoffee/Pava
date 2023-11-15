# coding=utf-8
from pava.utils.object_utils import *

from pava.decorator.decorator_impl.type_check_decorator import type_check

"""
业务HTTP接口返回的 通用对象
"""


class ServiceResponse(object):
    @type_check(None, int, [str, NoneType], None)
    def __init__(self, service_code=None, service_message=None, data=None):
        self.service_code = service_code
        self.service_message = service_message
        self.data = data

    @type_check(None, int)
    def set_service_code(self, service_code):
        self.service_code = service_code
        return self

    @type_check(None, [str, NoneType])
    def set_service_message(self, service_message):
        if str_is_blank(service_message):
            service_message = ""
        self.service_message = service_message
        return self

    def set_data(self, data):
        self.data = data
        return self

    @classmethod
    @type_check(None, dict)
    def from_dict(cls, dict_object):
        service_response = ServiceResponse()
        service_response.set_service_code(get_int_value(dict_object.get("service_code", None)))
        service_response.set_service_message(dict_object.get("service_message", None))
        service_response.set_data(dict_object.get("data", None))
        return service_response

    def is_success(self):
        return ServiceResponse.success().service_code == self.service_code

    def is_failed(self):
        return ServiceResponse.failed().service_code == self.service_code

    @classmethod
    @type_check(None, [str, NoneType], None)
    def success(cls, service_message=None, data=None):
        if str_is_blank(service_message):
            service_message = "Success!"
        return ServiceResponse(service_code=2000, service_message=service_message, data=data)

    @classmethod
    @type_check(None, [str, NoneType], None)
    def failed(cls, service_message=None, data=None):
        if str_is_blank(service_message):
            service_message = "Failed!"
        return ServiceResponse(service_code=5000, service_message=service_message, data=data)

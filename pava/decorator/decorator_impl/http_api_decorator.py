# coding=utf-8
from functools import wraps

from pava.decorator.base_decorator import BaseDecorator
from pava.entity.service_response import ServiceResponse
from pava.utils.object_utils import *

"""
注解在 HTTP API 上
用于将返回的任何类型的对象 转化为 json
并且将下划线命令方式转化为驼峰的命名方式
"""


class HttpAPI(BaseDecorator):
    @classmethod
    def execute(cls, execute_result):
        if execute_result is None:
            return execute_result
        dict_object = object_to_dict(execute_result)
        dict_object = underscore_to_camel(dict_object)
        return dict_object


def http_api(function):
    @wraps(function)
    def http_api_wrapper(*args, **kwargs):
        return_object = function(*args, **kwargs)
        return HttpAPI.execute(return_object)

    return http_api_wrapper


if __name__ == '__main__':
    @http_api
    def test_function():
        return ServiceResponse.success()


    res = test_function()
    pass

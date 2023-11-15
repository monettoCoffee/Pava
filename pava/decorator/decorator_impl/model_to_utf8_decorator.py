# coding=utf-8
from functools import wraps

from pava.decorator.base_decorator import BaseDecorator
from pava.utils.object_utils import *

"""
这个注解很危险 使用前请确保 对象的结构是清晰的
这个注解很危险 使用前请确保 对象的结构是清晰的
这个注解很危险 使用前请确保 对象的结构是清晰的

SQLAlchemy 在 SQLite 中运行结束后 返回的 TEXT 为 Unicode 类型
这里转化为 UTF-8
@model_to_utf8
"""


class ModelToUTF8(BaseDecorator):
    @classmethod
    def execute(cls, execute_result):
        if execute_result is None:
            return execute_result
        return trans_object_properties_to_utf8(execute_result)


def model_to_utf8(function):
    @wraps(function)
    def model_to_utf8_wrapper(*args, **kwargs):
        execute_result = function(*args, **kwargs)
        return ModelToUTF8.execute(execute_result)

    return model_to_utf8_wrapper


if __name__ == '__main__':
    a = [{"111": {"inline": "inline"}}, {"222": "bbb"}]
    a = json_to_object(object_to_json(a))
    # a = ModelToUTF8.execute(a)
    pass

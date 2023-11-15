# coding=utf-8
from abc import ABCMeta
from abc import abstractmethod

"""
装饰器基类
为方便链式执行 每个装饰器需要继承 BaseDecorator 类 并实现 execute 方法
"""


class BaseDecorator(object):
    __metaclass__ = ABCMeta

    @classmethod
    @abstractmethod
    def execute(cls, *args, **kwargs):
        pass

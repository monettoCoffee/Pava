# coding=utf-8
from abc import ABCMeta, abstractmethod

"""
MQ 的消费者接口
"""


class AbstractMQConsumer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self, *args, **kwargs):
        pass

    @abstractmethod
    def _get_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def _do_consume(self, *args, **kwargs):
        pass

    @abstractmethod
    def _do_exception(self, *args, **kwargs):
        pass

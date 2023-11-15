# coding=utf-8
from abc import ABCMeta, abstractmethod

"""
MQ 的生产者接口
"""


class AbstractMQProducer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def support(self, *args, **kwargs):
        pass

    @abstractmethod
    def produce_message(self, *args, **kwargs):
        pass

# coding=utf-8
from abc import ABCMeta, abstractmethod

"""
MQ 的服务接口
"""


class AbstractMQBroker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def commit_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def consume_failed(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_message_by_uuid(self, *args, **kwargs):
        pass

    @abstractmethod
    def hold_message(self, *args, **kwargs):
        pass

# coding=utf-8

from abc import ABCMeta, abstractmethod


class AbstractThreadPoolDomain(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_running_task_quantity(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_pool_worker_size(self, *args, **kwargs):
        pass

    @abstractmethod
    def add_task(self, *args, **kwargs):
        pass

    @abstractmethod
    def after_execute_callback_(self, *args, **kwargs):
        pass

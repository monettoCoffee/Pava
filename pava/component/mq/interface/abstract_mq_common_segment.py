# coding=utf-8
from abc import ABCMeta, abstractmethod

"""
MQ Ext Segment 抽象接口
"""


class AbstractMQBrokerCommonSegment(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_message(self, message_text, producer):
        """
        :type message_text: str
        :type producer: str
        """
        pass

    @abstractmethod
    def get_message(self, consumer, max_consume_time):
        pass

    @abstractmethod
    def update_message(self, mq_message):
        pass

    @abstractmethod
    def hold_message(self, message_uuid, hold_consume_time):
        pass

    @abstractmethod
    def commit_message(self, message_uuid):
        pass

    @abstractmethod
    def delete_message(self, message_uuid, expect_message_status=None):
        pass

    @abstractmethod
    def fetch_message_by_uuid(self, message_uuid):
        pass

    @abstractmethod
    def consume_failed(self, message_id, max_failed_times, retry_times_interval):
        pass

    @abstractmethod
    def get_db_path(self):
        pass

    @abstractmethod
    def recover_message(self, recover_message_uuid_list):
        pass

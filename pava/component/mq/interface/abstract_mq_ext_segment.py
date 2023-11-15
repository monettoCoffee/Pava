# coding=utf-8
from abc import ABCMeta, abstractmethod

"""
MQ Ext Segment 抽象接口
"""


class AbstractMQBrokerExtSegment(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_message(self, mq_message, commit_log_file):
        pass

    @abstractmethod
    def delete_message_by_mq_message(self, mq_message, commit_log_file):
        pass

    @abstractmethod
    def delete_message_by_uuid(self, message_topic, message_uuid):
        pass

    @abstractmethod
    def update_message(self, mq_message, commit_log_file):
        pass

    @abstractmethod
    def scan_message(self, message_topic, every_page_quantity, page_number, message_status=None):
        pass

    @abstractmethod
    def get_recover_message(self):
        pass

    @abstractmethod
    def do_recover_message_status(self, recover_message_uuid_list):
        pass

    @abstractmethod
    def fetch_message_by_uuid(self, message_uuid):
        pass

    @abstractmethod
    def get_db_path(self):
        pass

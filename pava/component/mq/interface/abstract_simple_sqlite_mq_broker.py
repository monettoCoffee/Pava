# coding=utf-8
from abc import ABCMeta

from pava.component.mq.interface.abstract_mq_broker import AbstractMQBroker


class AbstractSimpleSQLiteMQBroker(AbstractMQBroker):
    __metaclass__ = ABCMeta

    def add_message(self, message_topic, message_text, producer=None):
        pass

    def get_message(self, message_topic, consumer=None, max_consume_time=3600):
        pass

    def commit_message(self, message_topic, message_id):
        pass

    def hold_message(self, message_topic, message_uuid, hold_consume_time=6000):
        pass

    def consume_failed(self, message_topic, message_uuid, max_failed_times, retry_times_interval=300):
        pass

    def delete_message(self, message_topic, message_id):
        pass

    def scan_message(self, message_topic=None, every_page_quantity=10, page_number=1):
        pass

    def fetch_message_by_uuid(self, message_id):
        pass

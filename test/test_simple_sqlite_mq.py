# coding=utf-8
import re
import time

from pava.utils.object_utils import main_thread_hang, str_list_compose, time_sleep

from pava.component.p_log import PLog

from pava.component.mq.core.simple_sqlite_mq_consumer import SimpleSQLiteMQConsumer
from pava.component.mq.core.simple_sqlite_mq_broker import SimpleSQLiteMQBroker

# MQ_PATH = "/Users/monetto/Documents/test_mq"
MQ_PATH = "/Users/bytedance/Documents/test_mq"


class TestConsumer(SimpleSQLiteMQConsumer):
    def __init__(self, mq_instance):
        SimpleSQLiteMQConsumer.__init__(self, mq_instance)
        self.register_consume_message("topic_a", self._consume_topic_a)
        self.register_consume_message("topic_b", self._consume_topic_b)

    def _consume_topic_a(self, mq_message):
        PLog.get().info("_consume_topic_a...")
        time_sleep(20)
        self._mq_instance.add_message("topic_b", "TEXT_B")

    def _consume_topic_b(self, mq_message):
        PLog.get().info("_consume_topic_b...")
        time_sleep(5)
        a = 10 / 0

    def _get_consumer(self, message_topic):
        return "TestConsumer"

    def _get_consume_expire_time(self, message_topic):
        return 10

    def _get_max_failed_times(self, message_topic):
        return 2

    def _get_retry_interval(self, message_topic):
        return 2


if __name__ == '__main__':
    mq = SimpleSQLiteMQBroker(MQ_PATH)
    mq.add_message("topic_a", "TEXT_A")
    mq_message = mq.get_message("topic_a")
    mq.commit_message(mq_message.message_topic, mq_message.message_uuid)
    main_thread_hang()
    pass

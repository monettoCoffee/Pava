# coding=utf-8
import threading
from abc import ABCMeta, abstractmethod
import time
from threading import Lock

from pava.component.mq.core.mq_message import MQMessage
from pava.component.mq.interface.abstract_simple_sqlite_mq_broker import AbstractSimpleSQLiteMQBroker

from pava.component.mq.interface.abstract_mq_consumer import AbstractMQConsumer
from pava.component.p_log import PLog
from pava.utils.async_utils import async_execute, cycle_execute
from pava.utils.object_utils import *


class SimpleSQLiteMQConsumer(AbstractMQConsumer):
    __metaclass__ = ABCMeta

    @type_check(None, None, [int, float])
    def __init__(self, mq_instance, hold_message_heart_beat=30):
        # 消费的主题合集 具有顺序性
        self._consume_topic_list = list()
        # 消费的主题 存储对应处理消息 Handler
        self._consume_topic_handler = dict()

        # 消息队列实体 或 消息队列代理对象
        self._mq_instance = mq_instance  # type: AbstractSimpleSQLiteMQBroker

        # 消费消息的线程
        self._consumer_thread = None
        # 消费消息时 维持消费关系的线程 专门用于发送 Hold Message 请求
        self._hold_message_thread = None

        # 消费消息时 多少秒发送一次 Hold Message 请求
        self._hold_message_heart_beat = hold_message_heart_beat
        # 消息操作同步锁 用于防止 Commit 消息后 仍然 Hold 消息的情况
        self._synchronize_message_lock = Lock()
        # 正在消费的消息 用于 Hold Message
        self._mq_message = None

        # 初始时间 当没有消息时休眠的时间 动态变化
        self._sleep_seconds = 1
        PLog.gets().info("[ConsumerTemplate] Consumer initial successfully")

    def start(self):
        cycle_execute("%s_hold_message" % id(self), self._hold_message, self._hold_message_heart_beat)
        async_execute(self._work)

    @type_check(None, str, None)
    def register_consume_message(self, message_topic, handler_function):
        self._consume_topic_list.insert(0, message_topic)
        self._consume_topic_handler[message_topic] = handler_function

        # 过期机制校验
        heart_beat_time = self._hold_message_heart_beat
        consume_expire_time = get_int_value(self._get_consume_expire_time(message_topic))
        if consume_expire_time != 0 and consume_expire_time < heart_beat_time:
            raise Exception("[ConsumerTemplate] Topic: %s expire time is %s, lower than heart beat %s" % (
                message_topic, consume_expire_time, heart_beat_time))

        PLog.gets().info("[ConsumerTemplate] Topic %s register successfully" % message_topic)

    @abstractmethod
    def _get_consumer(self, message_topic):
        pass

    @abstractmethod
    def _get_consume_expire_time(self, message_topic):
        pass

    @abstractmethod
    def _get_retry_interval(self, message_topic):
        pass

    @abstractmethod
    def _get_max_failed_times(self, message_topic):
        pass

    def _work(self):
        self._pre_consume_check()
        while True:
            self._consume_or_sleep()

    def _pre_consume_check(self):
        topic_list = self._consume_topic_list
        for topic in topic_list:
            topic_handler = self._consume_topic_handler.get(topic, None)
            if topic_handler is None:
                raise Exception("[ConsumerTemplate] Topic '%s' have not handler to consume it" % topic)

    def _get_message(self):
        # 获取 message 逻辑
        for topic in self._consume_topic_list:
            message = self._get_mq_message_by_topic(topic)
            if message is not None:
                return message
        return None

    def _get_mq_message_by_topic(self, message_topic):
        return self._mq_instance.get_message(
            message_topic=message_topic,
            consumer=self._get_consumer(message_topic),
            max_consume_time=get_int_value(self._get_consume_expire_time(message_topic))
        )

    def _consume_or_sleep(self):
        message = self._get_message()

        # 消费逻辑
        if message is None:
            # 如果没有消息 休眠时间逐渐增加
            if self._sleep_seconds < 10:
                self._sleep_seconds += 1

            PLog.gets().debug(
                "[ConsumerTemplate] No message can be consumed, consumer thread will be sleep %s ..." % self._sleep_seconds
            )

            # 休眠间隔时间后 继续尝试获取消息
            time_sleep(self._sleep_seconds)
            return

        # 存在消息 则消费消息
        self._handle_message(message)

        # 如果有消息存在 则消费间隔变为 0.1
        self._sleep_seconds = 0.1

        # 消费间隔
        time_sleep(self._sleep_seconds)

    def _handle_message(self, mq_message):
        message_topic = mq_message.message_topic  # type: str
        message_uuid = mq_message.message_uuid  # type: str
        PLog.gets().info("[ConsumerTemplate] Receive message: %s from %s, handle it..." % (message_uuid, message_topic))
        try:
            with self._synchronize_message_lock:
                self._mq_message = mq_message
            self._do_consume(mq_message)
            with self._synchronize_message_lock:
                self._mq_message = None
            self._mq_instance.commit_message(message_topic, message_uuid)
        except Exception as e:
            self._do_exception(mq_message, e)

    def _do_consume(self, message):
        self._consume_topic_handler[message.message_topic](message)

    def _do_exception(self, mq_message, exception_object):
        """
        :type mq_message: MQMessage
        :param exception_object:
        """
        with self._synchronize_message_lock:
            self._mq_message = None

        message_topic = mq_message.message_topic
        message_uuid = mq_message.message_uuid

        retry_times_interval = get_int_value(self._get_retry_interval(message_topic))

        failed_times = get_int_value(mq_message.failed_times)
        max_failed_times = get_int_value(self._get_max_failed_times(message_topic))

        # 重试次数过多 不可以继续重试的情况
        if failed_times >= max_failed_times:
            PLog.gets().exception(
                Exception(
                    "[SimpleSQLiteMQConsumer] Occurred exception when consume message topic: %s, message_uuid: %s, "
                    "maximum number of failed times: %s, this message cannot be consumed..." % (
                        message_topic, message_uuid, failed_times
                    ), exception_object)
            )
        # 可以继续重试的情况
        else:
            PLog.gets().exception(
                Exception(
                    "[SimpleSQLiteMQConsumer] Occurred exception when consume message topic: %s, message_uuid: %s, "
                    "will retry after %s seconds" % (
                        message_topic, message_uuid, retry_times_interval
                    ), exception_object)
            )

        # 将消息在 retry_interval 后进行重试
        self._mq_instance.consume_failed(
            message_topic=message_topic,
            message_uuid=message_uuid,
            max_failed_times=max_failed_times,
            retry_times_interval=retry_times_interval
        )

    def _hold_message(self):
        with self._synchronize_message_lock:
            mq_message = self._mq_message
            if mq_message is None:
                return
            self._do_hold(mq_message)

    def _do_hold(self, mq_message):
        """
        :type mq_message: MQMessage
        """
        message_topic = mq_message.message_topic
        try:
            consume_expire_time = self._get_consume_expire_time(message_topic)

            if PLog.log_level_is_debug():
                PLog.gets().debug("[ConsumerTemplate] hold message id: %s ..." % mq_message.message_uuid)

            self._mq_instance.hold_message(
                message_topic=mq_message.message_topic,
                message_uuid=mq_message.message_uuid,
                hold_consume_time=consume_expire_time)
        except Exception as e:
            PLog.gets().exception(
                Exception(
                    "[SimpleSQLiteMQConsumer] Occurred exception when hold message topic: %s, message_uuid: %s" % (
                        message_topic, mq_message.message_uuid
                    ), e)
            )

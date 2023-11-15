# coding=utf-8
from pava.utils.object_utils import NoneType

from pava.decorator.decorator_impl.type_check_decorator import type_check


class MQMessage(object):
    @type_check(None, int, str, str, int, int, int, str, int, int, str, str)
    def __init__(self, message_id=None, message_topic=None, message_text=None, message_status=None,
                 create_time=None, update_time=None, consumer=None, expire_time=None, failed_times=None, producer=None,
                 message_uuid=None):
        self.message_id = message_id
        self.message_topic = message_topic
        self.message_text = message_text
        self.message_status = message_status
        self.create_time = create_time
        self.update_time = update_time
        self.consumer = consumer
        self.expire_time = expire_time
        self.failed_times = failed_times
        self.producer = producer
        self.message_uuid = message_uuid

    @type_check(None, [int, NoneType])
    def set_message_id(self, message_id):
        self.message_id = message_id
        return self

    @type_check(None, [str, NoneType])
    def set_message_topic(self, message_topic):
        self.message_topic = message_topic
        return self

    @type_check(None, [str, NoneType])
    def set_message_text(self, message_text):
        self.message_text = message_text
        return self

    @type_check(None, [int, NoneType])
    def set_message_status(self, message_status):
        self.message_status = message_status
        return self

    @type_check(None, [int, NoneType])
    def set_create_time(self, create_time):
        self.create_time = create_time
        return self

    @type_check(None, [int, NoneType])
    def set_update_time(self, update_time):
        self.update_time = update_time
        return self

    @type_check(None, [str, NoneType])
    def set_consumer(self, consumer):
        self.consumer = consumer
        return self

    @type_check(None, [int, NoneType])
    def set_expire_time(self, expire_time):
        self.expire_time = expire_time
        return self

    @type_check(None, [int, NoneType])
    def set_failed_times(self, failed_times):
        self.failed_times = failed_times
        return self

    @type_check(None, [str, NoneType])
    def set_producer(self, producer):
        self.producer = producer
        return self

    @type_check(None, [str, NoneType])
    def set_message_uuid(self, message_uuid):
        self.message_uuid = message_uuid
        return self

    def copy(self):
        another_mq_message = MQMessage()
        another_mq_message.message_id = self.message_id
        another_mq_message.message_topic = self.message_topic
        another_mq_message.message_text = self.message_text
        another_mq_message.message_status = self.message_status
        another_mq_message.create_time = self.create_time
        another_mq_message.update_time = self.update_time
        another_mq_message.consumer = self.consumer
        another_mq_message.expire_time = self.expire_time
        another_mq_message.failed_times = self.failed_times
        another_mq_message.producer = self.producer
        another_mq_message.message_uuid = self.message_uuid
        return another_mq_message

    def to_dict(self):
        return {
            "message_id": self.message_id,
            "message_topic": self.message_topic,
            "message_text": self.message_text,
            "message_status": self.message_status,
            "create_time": self.create_time,
            "update_time": self.update_time,
            "consumer": self.consumer,
            "expire_time": self.expire_time,
            "failed_times": self.failed_times,
            "producer": self.producer,
            "message_uuid": self.message_uuid
        }

    @classmethod
    @type_check(None, dict)
    def from_dict(cls, dict_object):
        message = MQMessage()
        message.set_message_id(dict_object.get("message_id", None))
        message.set_message_topic(dict_object.get("message_topic", None))
        message.set_message_text(dict_object.get("message_text", None))
        message.set_message_status(dict_object.get("message_status", None))
        message.set_create_time(dict_object.get("create_time", None))
        message.set_update_time(dict_object.get("update_time", None))
        message.set_consumer(dict_object.get("consumer", None))
        message.set_expire_time(dict_object.get("expire_time", None))
        message.set_failed_times(dict_object.get("failed_times", None))
        message.set_producer(dict_object.get("producer", None))
        message.set_message_uuid(dict_object.get("message_uuid", None))
        return message


if __name__ == "__main__":
    d = dict()
    m = MQMessage.from_dict(d).to_dict()
    pass

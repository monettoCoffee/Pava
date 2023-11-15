# coding=utf-8
import bisect
import re
from threading import Lock

from pava.component.mq.core.commit_log import CommitLog
from pava.component.mq.interface.abstract_mq_broker import AbstractMQBroker

from pava.component.mq.core.simple_sqlite_mq_ext_segment import _get_simple_sqlite_mq_broker_ext_segment
from pava.component.mq.interface.abstract_mq_common_segment import AbstractMQBrokerCommonSegment
from pava.component.mq.interface.abstract_mq_ext_segment import AbstractMQBrokerExtSegment
from pava.component.thread_pool.core.thread_pool_executor_wrapper import ThreadPoolExecutorWrapper
from pava.utils.time_utils import get_current_timestamp
from pava.utils.web_utils import get_local_host_ip

from pava.entity.file_domain import FileDomain

from pava.component.mq.core.simple_sqlite_mq_common_segment import _get_simple_sqlite_mq_broker_common_segment
from pava.component.mq.core.mq_message import MQMessage
from pava.component.mq.interface.abstract_simple_sqlite_mq_broker import AbstractSimpleSQLiteMQBroker

from pava.component.p_log import PLog

from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.component.mq import *
from pava.utils.async_utils import cycle_execute, async_execute
from pava.utils.object_utils import *

from pava.decorator.decorator_impl.type_check_decorator import type_check

"""
简易的基于 SQLite 的消息队列
基于 SimpleSQLiteBrokerCommonSegment 包装了一层 以便外部使用 
以及 适配 SQLite特性
"""

SIMPLE_SQLITE_MQ_LOCK_KEY_PREFIX = "SimpleSQLiteMQWrapper_"
SIMPLE_SQLITE_MQ_LOCK_KEY_COUNT = 0
ALL_SQLITE_MQ_PATH = dict()


class SimpleSQLiteMQBroker(AbstractSimpleSQLiteMQBroker):
    @synchronized(SIMPLE_SQLITE_MQ_LOCK_KEY_PREFIX)
    def __init__(self, mq_path, recover_message_heart_beat=30):
        # 检查 MQ Path 是否合法有效
        self._path_check(mq_path)
        self._mq_path = mq_path
        self._segment_path = os.path.join(mq_path, "segment")
        FileDomain(self._segment_path).create_dir()
        self._commit_path = os.path.join(mq_path, "commit")
        FileDomain(self._commit_path).create_dir()

        # 避免重复生成 Segment
        self._generate_segment_lock = Lock()

        # 存放所有的 Topic Segment
        self._segment_dict = dict()

        # 生成供外部接入的 Ext Segment
        self._ext_segment = self._get_topic_segment(EXT_SQLITE_MQ_SEGMENT)  # type: AbstractMQBrokerExtSegment

        # 生成 归档器
        self._archiver_segment = self._get_topic_segment(
            ARCHIVER_SQLITE_MQ_SEGMENT
        )  # type: AbstractMQBrokerExtSegment

        self._ext_segment.archiver_segment_ = self._archiver_segment

        # 处理突然中断的 还没来得及处理的消息
        self._handle_log_commit_file()

        # 恢复消息线程 每 30 秒运行一次
        cycle_execute("%s_recover_message" % id(self), self._recover_message, recover_message_heart_beat)

        # 将 Commit 日志 一个一个 顺序执行 用于 Ext Segment
        self._order_log_executor = ThreadPoolExecutorWrapper(max_workers=1)

        PLog.gets().info("[SimpleSQLiteMQBroker] All message queue component start successfully")

    def _path_check(self, mq_path):
        """
        检查 MQ Path 是否合法有效
        """
        # 防止文件冲突 不可重复构建相同路径的 SQLite MQ
        if ALL_SQLITE_MQ_PATH.get(mq_path, False) is False:
            ALL_SQLITE_MQ_PATH[mq_path] = True
        else:
            raise Exception("[SimpleSQLiteMQBroker] MQ Path '%s' has been created!" % mq_path)

        # 对 MQ Path 路径进行校验
        PLog.gets().info("[SimpleSQLiteMQBroker] Start initial message queue component...")
        if str_is_blank(mq_path):
            raise Exception("[SimpleSQLiteMQBroker] MQ Path is blank when create sqlite mq connection")

    def _generate_topic_segment_by_message_topic(self, message_topic):
        """
        :rtype: AbstractMQBroker
        """
        with self._generate_segment_lock:
            # 并发情况下已经生成 Segment 就可以返回了
            sqlite_mq_segment_instance = self._segment_dict.get(message_topic, None)
            if sqlite_mq_segment_instance is not None:
                return sqlite_mq_segment_instance

            # 生成并发锁 Key
            global SIMPLE_SQLITE_MQ_LOCK_KEY_COUNT
            SIMPLE_SQLITE_MQ_LOCK_KEY_COUNT += 1
            # 如果创建多个 Broker 防止防重 Key 冲突
            mq_operation_lock_key = SIMPLE_SQLITE_MQ_LOCK_KEY_PREFIX + str(id(self)) + str(
                SIMPLE_SQLITE_MQ_LOCK_KEY_COUNT
            )

            # 判断是否为 Ext Segment
            ext_segment_inner_instance_bool = message_topic == EXT_SQLITE_MQ_SEGMENT \
                                              or message_topic == ARCHIVER_SQLITE_MQ_SEGMENT
            # 生成独立的 Topic SQLite 文件
            if ext_segment_inner_instance_bool:
                sqlite_mq_segment_instance = _get_simple_sqlite_mq_broker_ext_segment(
                    db_path=os.path.join(self._segment_path, message_topic + ".sqlite"),
                    mq_operation_lock_key=mq_operation_lock_key
                )
            else:
                sqlite_mq_segment_instance = _get_simple_sqlite_mq_broker_common_segment(
                    message_topic=message_topic,
                    db_path=os.path.join(self._segment_path, "%s_segment.sqlite" % message_topic),
                    commit_path=self._commit_path,
                    mq_operation_lock_key=mq_operation_lock_key
                )
            # 记录 Segment, Ext Segment 和 Archiver Segment 直接就能取到 无需加入 Segment Dict
            if not ext_segment_inner_instance_bool:
                self._segment_dict[message_topic] = sqlite_mq_segment_instance

            return sqlite_mq_segment_instance

    def _get_topic_segment(self, message_topic):
        """
        :rtype: AbstractMQBrokerCommonSegment or AbstractMQBrokerExtSegment
        """
        message_topic_segment = self._segment_dict.get(message_topic, None)
        if message_topic_segment is None:
            return self._generate_topic_segment_by_message_topic(message_topic)
        return message_topic_segment

    def _handle_log_commit_file(self):
        file_domain_list = FileDomain(self._commit_path).get_children_file_domain()
        if list_is_empty(file_domain_list):
            return

        commit_log_list = list()
        for file_domain in file_domain_list:
            # 正则表达式 筛选出 哪些是 Commit Log File
            if re.match(r'([\s\S]*)[/_]([\s\S]*)[/_]([\s\S]*)', file_domain.file_name) is None:
                continue
            try:
                mq_message_dict = json_to_python_object(file_domain.get_file_content())
                mq_message = MQMessage.from_dict(mq_message_dict)
                operation_index = get_int_value(file_domain.file_name.split("_")[0])
                bisect.insort(commit_log_list, CommitLog(
                    commit_log_id=operation_index,
                    commit_log_file=file_domain.file_absolute_path,
                    mq_message=mq_message))
            except Exception as e:
                PLog.gets().warning(
                    "[SimpleSQLiteMQBroker] Exception when read no committed log file! Exception '%s'" % str(e))
        self._sync_mq_message_data_from_common_segment(commit_log_list)
        PLog.gets().info(
            "[SimpleSQLiteMQBroker] Handle un-commit log file done")

    def _sync_mq_message_data_from_common_segment(self, commit_log_list):
        if list_is_empty(commit_log_list):
            return

        for commit_log in commit_log_list:  # type: CommitLog
            message_topic = commit_log.mq_message.message_topic
            message_uuid = commit_log.mq_message.message_uuid
            mq_message = self.fetch_message_by_uuid(message_topic, message_uuid)
            if mq_message is None:
                if commit_log.mq_message.message_status != MESSAGE_STATUS_DELETE and commit_log.mq_message.message_status != MESSAGE_STATUS_DONE:
                    PLog.gets().warning(
                        "[SimpleSQLiteMQBroker] Try to commit '%s', '%s', but cannot fetch message." % (
                            message_topic, message_uuid))
                self._ext_segment.delete_message_by_mq_message(commit_log.mq_message, commit_log.commit_log_file)
            else:
                mq_message.message_text = str_to_base64(mq_message.message_text)
                ext_mq_message = self._ext_segment.fetch_message_by_uuid(mq_message.message_uuid)
                if ext_mq_message is None:
                    self._ext_segment.add_message(mq_message, commit_log.commit_log_file)
                else:
                    self._ext_segment.update_message(mq_message, commit_log.commit_log_file)

    @type_check(None, str, str, [str, NoneType])
    def add_message(self, message_topic, message_text, producer=None):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when add message")
        message_topic_segment = self._get_topic_segment(message_topic)

        # Producer 使用 Broker IP 作为补缺
        if str_is_blank(producer):
            producer = get_local_host_ip()

        # 消息体按照 Base64 进行存储 防止 SQLite 乱码 或 无法处理
        new_message_text = str_to_base64(message_text)

        mq_message, log_file_name = message_topic_segment.add_message(new_message_text, producer)
        # 添加数据同步至 Ext Segment 任务
        self._order_log_executor.submit(self._ext_segment.add_message, mq_message.copy(), log_file_name)

        # 覆盖 Base64 的 Message Text
        mq_message.message_text = message_text
        if PLog.log_level_is_debug():
            PLog.gets().info(
                "[SimpleSQLiteMQBroker] Add message topic: %s, uuid: %s, message_text: %s successfully" % (
                    message_topic, mq_message.message_uuid, message_text)
            )
        else:
            PLog.gets().info(
                "[SimpleSQLiteMQBroker] Add message topic: %s, uuid: %s successfully" % (
                    message_topic, mq_message.message_uuid)
            )
        return mq_message

    @type_check(None, str, [str, None], [int, NoneType])
    def get_message(self, message_topic, consumer=None, max_consume_time=3600):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when get message")
        if str_is_blank(consumer):
            consumer = ""
        max_consume_time = get_int_value(max_consume_time)

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message, log_file_name = message_topic_segment.get_message(consumer, max_consume_time)

        if mq_message is not None:
            self._order_log_executor.submit(self._ext_segment.update_message, mq_message.copy(), log_file_name)
            self.base64_message_text_to_str(mq_message)
            PLog.gets().info(
                "[SimpleSQLiteMQBroker] Get message topic: %s, uuid: %s successfully" % (
                    message_topic, mq_message.message_uuid)
            )

        return mq_message

    @type_check(None, str, str, [int, NoneType])
    def hold_message(self, message_topic, message_uuid, hold_consume_time=6000):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when hold message")
        hold_consume_time = 6000 if get_int_value(hold_consume_time) == 0 else hold_consume_time

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message, log_file_name = message_topic_segment.hold_message(message_uuid, hold_consume_time)

        if mq_message is not None:
            self._order_log_executor.submit(self._ext_segment.update_message, mq_message, log_file_name)
            self.base64_message_text_to_str(mq_message)
            PLog.gets().debug(
                "[SimpleSQLiteMQBroker] Hold message topic: %s, uuid: %s successfully" % (message_topic, message_uuid)
            )
        return mq_message

    @type_check(None, str, str)
    def commit_message(self, message_topic, message_uuid):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when commit message")

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message, log_file_name = message_topic_segment.commit_message(message_uuid)

        self._order_log_executor.submit(self._ext_segment.delete_message_by_mq_message, mq_message.copy(),
                                        log_file_name)

        PLog.gets().info(
            "[SimpleSQLiteMQBroker] Commit message topic:%s, id:%s successfully" % (message_topic, message_uuid))
        self.base64_message_text_to_str(mq_message)
        return mq_message

    @type_check(None, str, str, bool)
    def delete_message(self, message_topic, message_uuid, is_archiver_read=False):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when delete message")

        if is_archiver_read:
            self._archiver_segment.delete_message_by_uuid(message_topic, message_uuid)
            return

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message, log_file_name = message_topic_segment.delete_message(message_uuid)

        self._order_log_executor.submit(self._ext_segment.delete_message_by_mq_message, mq_message.copy(),
                                        log_file_name)

        PLog.gets().info(
            "[SimpleSQLiteMQBroker] Delete message topic: %s, uuid: %s successfully" % (message_topic, message_uuid))
        self.base64_message_text_to_str(mq_message)
        return mq_message

    @type_check(None, str, [int, NoneType], [int, NoneType], [NoneType, bool])
    def scan_message(self, message_topic=None, every_page_quantity=10, page_number=1, is_archiver_read=False):
        every_page_quantity = 10 if get_int_value(every_page_quantity) == 0 else every_page_quantity
        page_number = 1 if get_int_value(page_number) == 0 else page_number
        page_number = get_int_value(page_number) - 1

        # 读取 Ext Segment 或者读取已经归档的数据
        if is_archiver_read:
            mq_message_list = self._archiver_segment.scan_message(message_topic, every_page_quantity, page_number)
        else:
            mq_message_list = self._ext_segment.scan_message(message_topic, every_page_quantity, page_number)

        if list_not_empty(mq_message_list):
            for mq_message in mq_message_list:
                self.base64_message_text_to_str(mq_message)
        return mq_message_list

    @type_check(None, str, str)
    def fetch_message_by_uuid(self, message_topic, message_uuid):
        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message = message_topic_segment.fetch_message_by_uuid(message_uuid)
        self.base64_message_text_to_str(mq_message)
        return mq_message

    @type_check(None, str, str, [int, NoneType], [int, NoneType])
    def consume_failed(self, message_topic, message_uuid, max_failed_times, retry_times_interval=300):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when dealing failed message")
        if get_int_value(retry_times_interval) == 0:
            retry_times_interval = 300

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message, log_file_name = message_topic_segment.consume_failed(message_uuid, max_failed_times,
                                                                         retry_times_interval)
        self._order_log_executor.submit(self._ext_segment.update_message, mq_message, log_file_name)

        if mq_message.failed_times >= max_failed_times:
            PLog.gets().info(
                "[SimpleSQLiteMQBroker] Failed to consume message topic: %s, uuid: %s, "
                "maximum number of retry times: %s, pending message..." % (
                    message_topic,
                    message_uuid,
                    max_failed_times
                )
            )
        else:
            PLog.gets().info(
                "[SimpleSQLiteMQBroker] Failed to consume message topic: %s, uuid: %s, retry after %s seconds "
                "(if message recovered)" % (
                    message_topic,
                    message_uuid,
                    retry_times_interval
                )
            )
        self.base64_message_text_to_str(mq_message)
        return mq_message

    def _recover_message(self):
        mq_message_list = self._ext_segment.get_recover_message()
        if list_is_empty(mq_message_list):
            return

        # 统计 都有哪些消息需要恢复
        total_recover_message_uuid_list = list()
        recover_message_dict = dict()
        for mq_message in mq_message_list:  # type: MQMessage
            message_topic = mq_message.message_topic
            recover_message_uuid_list = recover_message_dict.get(message_topic, None)
            if recover_message_uuid_list is None:
                recover_message_uuid_list = list()
                recover_message_dict[message_topic] = recover_message_uuid_list
            recover_message_uuid_list.append(mq_message.message_uuid)
            total_recover_message_uuid_list.append(mq_message.message_uuid)

        # 用每个 Segment 每个进行恢复
        recover_message_thread_list = list()
        for message_topic, message_uuid_list in recover_message_dict.items():
            message_topic_segment = self._get_topic_segment(message_topic)
            recover_message_thread_list.append(async_execute(message_topic_segment.recover_message, message_uuid_list))

        for task in recover_message_thread_list:
            task.result()

        # 最后恢复 Ext 状态
        self._ext_segment.do_recover_message_status(total_recover_message_uuid_list)

    @type_check(None, str, str, [NoneType, int], [NoneType, int], [NoneType, str])
    def lock_message(self, message_topic, message_uuid, message_status, max_consume_time=None, consumer=None):
        if str_is_blank(message_topic):
            raise Exception("[SimpleSQLiteMQBroker] message_topic cannot be blank when locked message")
        if str_is_blank(consumer):
            consumer = ""

        message_status = get_int_value(message_status)
        max_consume_time = get_int_value(max_consume_time)

        message_topic_segment = self._get_topic_segment(message_topic)
        mq_message = message_topic_segment.fetch_message_by_uuid(message_uuid)

        if mq_message.message_status == MESSAGE_STATUS_LOCKED:
            raise Exception(
                "[SimpleSQLiteMQBroker] message status LOCKED cannot be re-locked when locked '%s'" % message_uuid
            )

        update_time = get_current_timestamp()
        mq_message.update_time = update_time
        mq_message.message_status = message_status
        mq_message.consumer = consumer
        mq_message.expire_time = 0 if max_consume_time == 0 else update_time + max_consume_time

        log_file_name = message_topic_segment.update_message(mq_message)
        self._order_log_executor.submit(self._ext_segment.update_message, mq_message.copy(), log_file_name)
        return mq_message

    @type_check(None, [MQMessage, NoneType])
    def base64_message_text_to_str(self, mq_message):
        if mq_message is None:
            return
        message_text = mq_message.message_text
        if str_not_blank(message_text):
            mq_message.message_text = base64_to_str(message_text)

    def get_ext_segment_db_path(self):
        return self._ext_segment.get_db_path()

    def get_archiver_segment_db_path(self):
        return self._archiver_segment.get_db_path()

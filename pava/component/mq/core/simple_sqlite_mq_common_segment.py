# coding=utf-8
from pava.component.mq.interface.abstract_mq_common_segment import AbstractMQBrokerCommonSegment
from pava.utils.web_utils import get_local_host_ip

from pava.entity.file_domain import FileDomain

from pava.component.mq.core.mq_message import MQMessage
from pava.component.mq.core.sqlite_connection_pool import SQLiteConnectionPool

from pava.component.p_log import PLog

from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.component.mq import *
from pava.utils.time_utils import *

from pava.decorator.decorator_impl.type_check_decorator import type_check

"""
简易的基于 SQLite 的消息队列 内置的 Common Segment 结构
基本的原子操作 内置对象
不推荐直接使用
"""


def _get_simple_sqlite_mq_broker_common_segment(message_topic, db_path, commit_path, mq_operation_lock_key):
    class SimpleSQLiteBrokerCommonSegment(AbstractMQBrokerCommonSegment):

        def __init__(self):
            """
            初始化一个 Common Segment
            """
            # 由上层 Synchronized 装饰器保证不会出现脏数据
            self._connection_pool = SQLiteConnectionPool(db_path, capacity=4)
            self._db_path = db_path
            self._commit_path = commit_path
            self._message_topic = message_topic

            # 创建数据表 以及对应的索引
            connection, cursor = self._get_connection_with_transaction()
            cursor.execute(CREATE_SIMPLE_SQLITE_MQ_COMMON_SEGMENT_TABLE_SQL)
            cursor.execute(CREATE_INDEX_UUID)
            connection.commit()

            # 用于记录日志顺序
            self._execute_log_count = -1
            PLog.gets().info(
                "[SimpleSQLiteBrokerCommonSegment] Initial message queue '%s' "
                "storage successfully" % self._message_topic
            )

        @synchronized(mq_operation_lock_key)
        def _generate_log_count(self):
            """
            生成有序 int 使得 Commit Log File 是有序读取的 (Reboot时)
            :return:
            """
            self._execute_log_count += 1
            return self._execute_log_count

        @type_check(None, MQMessage)
        def _write_commit(self, mq_message):
            """
            根据 MQ Message 生成 Commit Log File 供 Ext 同步使用
            :type mq_message: MQMessage
            """
            log_file_name = "%s_%s_%s" % (
                self._generate_log_count(), mq_message.message_topic, mq_message.message_uuid
            )
            log_file_path = os.path.join(commit_path, log_file_name)
            FileDomain(log_file_path).write(object_to_json(mq_message))
            return log_file_path

        def _get_connection_with_transaction(self):
            """
            自动开始一个事务 返回链接 与 游标
            """
            connection = self._connection_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute(BEGIN_TRANSACTION)
            return connection, cursor

        @type_check(None, str)
        def _execute_sql(self, sql_str):
            """
            直接执行 SQL
            :param sql_str: SQL 语句字符串
            :return: 执行结果
            """
            connection = self._connection_pool.get_connection()
            cursor = connection.cursor()
            execute_result = cursor.execute(sql_str)
            connection.commit()
            return execute_result

        def _get_str_column(self, column_value):
            """
            将 Unicode 转化为 UTF-8 Str
            """
            return None if column_value is None else column_value.encode("utf8")

        @synchronized(mq_operation_lock_key)
        @type_check(None, str, str)
        def add_message(self, message_text, producer):
            """
            向 MQ 中添加消息
            :param producer: 消息生产者
            :param message_text: 提交的消息
            """
            if str_is_blank(message_text):
                message_text = ""

            # 记录生成时间 与 UUID
            create_time = get_current_timestamp()
            message_uuid = get_uuid()

            # 提前生成实体 一会记录日志要用
            mq_message = MQMessage(
                message_topic=self._message_topic,
                message_text=message_text,
                message_status=MESSAGE_STATUS_INIT,
                create_time=create_time,
                update_time=create_time,
                expire_time=0,
                failed_times=0,
                producer=producer,
                consumer='',
                message_uuid=message_uuid
            )

            # 拼接插入 SQL
            add_message_sql = ADD_MESSAGE_TO_COMMON_SEGMENT_TABLE_SQL % (
                message_text, MESSAGE_STATUS_INIT, create_time, mq_message.update_time, mq_message.expire_time,
                producer, mq_message.consumer, message_uuid
            )

            # 开启事务执行
            connection, cursor = self._get_connection_with_transaction()
            execute_result = cursor.execute(add_message_sql)

            # 获取 Message ID
            message_id = cursor.lastrowid
            mq_message.message_id = message_id

            if execute_result.rowcount == 0:
                raise Exception("[SimpleSQLiteBrokerSegment] Try add message %s, but failed. SQL: %s", add_message_sql)

            # 生成 Commit Log
            log_file_name = self._write_commit(mq_message)
            connection.commit()

            return mq_message, log_file_name

        @synchronized(mq_operation_lock_key)
        def get_message(self, consumer, max_consume_time):
            """
            获取 MQ 中指定 topic 的消息 返回最早入库且未被消费的
            :param consumer: 消息对应的消费者
            :param max_consume_time: 最大消费时间 如果一段时间后没有通知消费完成 那么就会将这个消息置为初始状态
            """
            # 首先查询是否有对应的 符合条件的消息
            execute_result = self._execute_sql(GET_COMMON_SEGMENT_MESSAGE_SQL).fetchone()

            if execute_result is None or len(execute_result) == 0:
                return None, None

            max_consume_time = get_int_value(max_consume_time)
            update_time = get_current_timestamp()

            mq_message = MQMessage(
                message_id=execute_result[0],
                message_topic=self._message_topic,
                message_text=self._get_str_column(execute_result[1]),
                message_status=MESSAGE_STATUS_LOCKED,
                create_time=execute_result[2],
                update_time=update_time,
                consumer=consumer if str_not_blank(consumer) else get_local_host_ip(),
                expire_time=0 if max_consume_time == 0 else update_time + max_consume_time,
                failed_times=execute_result[3],
                producer=self._get_str_column(execute_result[4]),
                message_uuid=self._get_str_column(execute_result[5])
            )

            # 尝试锁定这条消息
            log_file_name = self.update_message(mq_message)
            return mq_message, log_file_name

        def update_message(self, mq_message):
            """
            :type mq_message: MQMessage
            """
            update_message_sql = UPDATE_COMMON_SEGMENT_MESSAGE_SQL % (
                mq_message.message_status,
                mq_message.update_time,
                mq_message.consumer,
                mq_message.expire_time,
                mq_message.failed_times,
                mq_message.message_uuid
            )
            connection, cursor = self._get_connection_with_transaction()
            execute_result = cursor.execute(update_message_sql)

            if execute_result.rowcount == 0:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Try locked message uuid %s, but failed. SQL: %s" % (
                        mq_message.message_uuid, update_message_sql
                    )
                )
            log_file_name = self._write_commit(mq_message)
            connection.commit()
            return log_file_name

        @synchronized(mq_operation_lock_key)
        def hold_message(self, message_uuid, hold_consume_time):
            """
            保持消息 当消息太长时间没有进行提交 实际仍然在消费时 应该调用这个方法以防止消息回到初始化状态而重复消费
            :param message_uuid: 消息的 UUID
            :param hold_consume_time: 在当前时间的基础上 延长的消费时间
            """
            mq_message = self.fetch_message_by_uuid(message_uuid)  # type: MQMessage
            if mq_message is None:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Can't get message uuid %s status when hold message." % (
                        message_uuid)
                )

            if mq_message.message_status != MESSAGE_STATUS_LOCKED:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Invalid message status '%s' status when hold message, message uuid "
                    "'%s'." % (
                        mq_message.message_status, message_uuid)
                )

            update_time = get_current_timestamp()
            new_expire_time = mq_message.expire_time if hold_consume_time == 0 else update_time + hold_consume_time
            mq_message.update_time = update_time
            mq_message.expire_time = new_expire_time

            # 更新消息
            log_file_name = self.update_message(mq_message)
            return mq_message, log_file_name

        @synchronized(mq_operation_lock_key)
        def commit_message(self, message_uuid):
            """
            消费成功后提交消息
            """
            mq_message = self.fetch_message_by_uuid(message_uuid)
            if mq_message is None:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Can't get message uuid %s when commit message." % (
                        message_uuid)
                )

            if mq_message.message_status != MESSAGE_STATUS_LOCKED:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Invalid message status '%s' status when commit message, message id "
                    "'%s'." % (
                        mq_message.message_status, message_uuid)
                )

            mq_message.update_time = get_current_timestamp()
            mq_message.message_status = MESSAGE_STATUS_DONE

            # 更新消息
            log_file_name = self._delete_message_get_log_file(mq_message)
            return mq_message, log_file_name

        @synchronized(mq_operation_lock_key)
        def delete_message(self, message_uuid, expect_message_status=None):
            """
            删除消息 将消息状态变更为已删除
            :param message_uuid: 消息的 UUID
            :param expect_message_status: 期望消息状态 如果存在值 且 状态为期望状态时 才进行删除
            """
            mq_message = self.fetch_message_by_uuid(message_uuid)
            if mq_message is None:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Can't get message uuid %s when delete message." % (
                        message_uuid)
                )

            if mq_message.message_status == MESSAGE_STATUS_LOCKED:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Invalid message status is LOCKED when commit message, message id "
                    "'%s'." % (
                        message_uuid)
                )

            mq_message.message_status = MESSAGE_STATUS_DELETE
            mq_message.update_time = get_current_timestamp()
            log_file_name = self._delete_message_get_log_file(mq_message)

            return mq_message, log_file_name

        @type_check(None, MQMessage)
        def _delete_message_get_log_file(self, mq_message):
            """
            :type mq_message: MQMessage
            """
            connection, cursor = self._get_connection_with_transaction()
            execute_result = cursor.execute(DELETE_COMMON_SEGMENT_MESSAGE_SQL % mq_message.message_uuid)

            if execute_result.rowcount == 0:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Try _delete_message message uuid %s, but failed." % (
                        mq_message.message_uuid
                    )
                )
            log_file_name = self._write_commit(mq_message)
            connection.commit()
            return log_file_name

        @synchronized(mq_operation_lock_key)
        def fetch_message_by_uuid(self, message_uuid):
            """
            :rtype: MQMessage
            """

            fetch_message_sql = FETCH_COMMON_MESSAGE_BY_MESSAGE_UUID_SQL % message_uuid
            execute_result = self._execute_sql(fetch_message_sql).fetchone()
            if execute_result is None or len(execute_result) == 0:
                return None

            return MQMessage(
                message_topic=self._message_topic,
                message_id=execute_result[0],
                message_text=self._get_str_column(execute_result[1]),
                message_status=execute_result[2],
                create_time=execute_result[3],
                update_time=execute_result[4],
                expire_time=execute_result[5],
                consumer=self._get_str_column(execute_result[6]),
                failed_times=execute_result[7],
                producer=self._get_str_column(execute_result[8]),
                message_uuid=message_uuid
            )

        @synchronized(mq_operation_lock_key)
        def consume_failed(self, message_uuid, max_failed_times, retry_times_interval):
            mq_message = self.fetch_message_by_uuid(message_uuid)
            if mq_message is None:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Can't confirm message uuid %s status when dealing failed message" % (
                        message_uuid)
                )

            if mq_message.message_status != MESSAGE_STATUS_LOCKED:
                raise Exception(
                    "[SimpleSQLiteBrokerSegment] Message uuid %s status %s invalid when dealing failed message." % (
                        message_uuid, mq_message.message_status
                    )
                )

            current_timestamp = get_current_timestamp()
            mq_message.update_time = current_timestamp

            next_retry_times = mq_message.failed_times + 1
            if next_retry_times >= max_failed_times:
                # 达到消费重试次数上限 挂起消息
                mq_message.failed_times = next_retry_times
                mq_message.message_status = MESSAGE_STATUS_PENDING
                commit_log_file = self.update_message(mq_message)
                return mq_message, commit_log_file
            else:
                # 未达到消费上限 一定时间后重试消息
                mq_message.message_status = MESSAGE_STATUS_FAILED
                mq_message.failed_times = next_retry_times
                mq_message.expire_time = current_timestamp + retry_times_interval

                commit_log_file = self.update_message(mq_message)
                return mq_message, commit_log_file

        @synchronized(mq_operation_lock_key)
        def recover_message(self, recover_message_uuid_list):
            message_uuid_list_str = ""
            for message_uuid in recover_message_uuid_list:
                message_uuid_list_str += "'%s', " % message_uuid
            # 加入 Expire Time < CurrentTimeStamp 的目的是
            # 当一个消费者网络断线 而后又重新连接 实际消息还是在消费 刚刚提交 Hold Message
            # 这个时候 就要防止把这个消息恢复 导致其他消费者重复消费
            recover_message_sql = SEGMENT_RECOVER_MESSAGE_SQL % (
                message_uuid_list_str[:-2], get_current_timestamp()
            )
            self._execute_sql(recover_message_sql)

        @synchronized(mq_operation_lock_key)
        def fetch_message_by_uuid(self, message_uuid):
            execute_result = self._execute_sql(FETCH_COMMON_MESSAGE_BY_MESSAGE_UUID_SQL % message_uuid).fetchone()
            if list_is_empty(execute_result):
                return None

            return MQMessage(
                message_id=execute_result[0],
                message_topic=self._message_topic,
                message_text=self._get_str_column(execute_result[1]),
                message_status=execute_result[2],
                create_time=execute_result[3],
                update_time=execute_result[4],
                expire_time=execute_result[5],
                consumer=self._get_str_column(execute_result[6]),
                failed_times=execute_result[7],
                producer=self._get_str_column(execute_result[8]),
                message_uuid=message_uuid
            )

        def get_db_path(self):
            return self._db_path

    return SimpleSQLiteBrokerCommonSegment()

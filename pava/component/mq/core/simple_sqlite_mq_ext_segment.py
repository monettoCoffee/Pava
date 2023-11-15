# coding=utf-8

from pava.entity.file_domain import FileDomain

from pava.component.mq.core.mq_message import MQMessage
from pava.component.mq.core.sqlite_connection_pool import SQLiteConnectionPool
from pava.component.mq.interface.abstract_mq_ext_segment import AbstractMQBrokerExtSegment

from pava.component.p_log import PLog

from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.component.mq import *
from pava.utils.time_utils import *

"""
简易的基于 SQLite 的消息队列 内置的 Ext Segment 结构
主要是供外部感知 MQ 的变化
作为接口开放
"""


def _get_simple_sqlite_mq_broker_ext_segment(db_path, mq_operation_lock_key):
    class SimpleSQLiteBrokerExtSegment(AbstractMQBrokerExtSegment):

        def __init__(self):
            self._connection_pool = SQLiteConnectionPool(db_path, capacity=8)
            self._db_path = db_path

            # 如果当前对象为 Ext 对象 则 由 Broker 注入 Archiver
            self.archiver_segment_ = None  # type: SimpleSQLiteBrokerExtSegment

            # 创建 Ext 管理表 以及对应的索引
            connection, cursor = self._get_connection_with_transaction()
            cursor.execute(CREATE_SIMPLE_SQLITE_MQ_EXT_SEGMENT_TABLE_SQL)
            cursor.execute(CREATE_INDEX_UUID)
            cursor.execute(CREATE_INDEX_MESSAGE_TOPIC)
            cursor.execute(CREATE_INDEX_EXPIRE_TIME)
            cursor.execute(CREATE_INDEX_UPDATE_TIME)
            connection.commit()

            PLog.gets().info("[SimpleSQLiteBrokerExtSegment] Initial message queue storage successfully")

        def _get_connection_with_transaction(self):
            connection = self._connection_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute(BEGIN_TRANSACTION)
            return connection, cursor

        def _execute_sql(self, sql_str):
            connection = self._connection_pool.get_connection()
            cursor = connection.cursor()
            execute_result = cursor.execute(sql_str)
            connection.commit()
            return execute_result

        def _delete_commit_log_file(self, commit_log_file):
            # 执行成功后 删除 Commit Log File
            FileDomain(commit_log_file).delete()

        def _get_str_column(self, column_value):
            return None if column_value is None else column_value.encode("utf8")

        @synchronized(mq_operation_lock_key)
        def add_message(self, mq_message, commit_log_file):
            """
            :type mq_message: MQMessage
            :type commit_log_file: str
            """
            if str_is_blank(mq_message.message_text):
                mq_message.message_text = ""

            # 拼接插入 SQL
            add_message_sql = ADD_MESSAGE_TO_EXT_SEGMENT_TABLE_SQL % (
                mq_message.message_id,
                mq_message.message_topic,
                mq_message.message_text,
                mq_message.message_status,
                mq_message.create_time,
                mq_message.update_time,
                mq_message.expire_time,
                mq_message.failed_times,
                mq_message.producer,
                mq_message.consumer,
                mq_message.message_uuid
            )

            connection, cursor = self._get_connection_with_transaction()
            cursor.execute(add_message_sql)
            connection.commit()

            # Archiver 归档时 不需要 Commit Log File
            if str_not_blank(commit_log_file):
                self._delete_commit_log_file(commit_log_file)

        @synchronized(mq_operation_lock_key)
        def update_message(self, mq_message, commit_log_file):
            """
            :type mq_message: MQMessage
            :type commit_log_file: str
            """
            update_message_sql = UPDATE_EXT_SEGMENT_MESSAGE_SQL % (
                mq_message.message_status,
                mq_message.update_time,
                mq_message.consumer,
                mq_message.expire_time,
                mq_message.failed_times,
                mq_message.message_uuid
            )
            self._execute_sql(update_message_sql)
            self._delete_commit_log_file(commit_log_file)

        @synchronized(mq_operation_lock_key)
        def delete_message_by_mq_message(self, mq_message, commit_log_file):
            """
            :type mq_message: MQMessage
            :type commit_log_file: str
            """
            delete_message_sql = DELETE_EXT_SEGMENT_MESSAGE_SQL % (
                mq_message.message_topic,
                mq_message.message_uuid
            )

            connection, cursor = self._get_connection_with_transaction()
            cursor.execute(delete_message_sql)

            if self.archiver_segment_ is not None:
                self.archiver_segment_.add_message(mq_message, None)
            self._delete_commit_log_file(commit_log_file)
            connection.commit()

        def delete_message_by_uuid(self, message_topic, message_uuid):
            delete_message_sql = DELETE_EXT_SEGMENT_MESSAGE_SQL % (
                message_topic,
                message_uuid
            )
            self._execute_sql(delete_message_sql)

        def scan_message(self, message_topic, every_page_quantity, page_number, message_status=None):
            every_page_quantity = get_int_value(every_page_quantity)
            page_number = get_int_value(page_number)
            scan_message_sql = SCAN_EXT_SEGMENT_MESSAGE_SQL % ("%s",
                                                               page_number * every_page_quantity,
                                                               every_page_quantity)

            if str_not_blank(message_topic):
                scan_message_sql = scan_message_sql % (WHERE_MESSAGE_TOPIC_STR % message_topic)
            elif message_status is not None:
                scan_message_sql = scan_message_sql % (WHERE_MESSAGE_STATUS_STR % get_int_value(message_status))
            else:
                scan_message_sql = scan_message_sql % ""

            result = list()
            execute_result = self._execute_sql(scan_message_sql).fetchall()
            if execute_result is None or len(execute_result) == 0:
                return result
            for element in execute_result:
                result.append(
                    MQMessage(
                        message_id=element[0],
                        message_topic=self._get_str_column(element[1]),
                        message_text=self._get_str_column(element[2]),
                        message_status=element[3],
                        create_time=element[4],
                        update_time=element[5],
                        consumer=self._get_str_column(element[6]),
                        expire_time=element[7],
                        failed_times=element[8],
                        producer=self._get_str_column(element[9]),
                        message_uuid=self._get_str_column(element[10])
                    )
                )
            return result

        def get_recover_message(self):
            current_timestamp = get_current_timestamp()
            execute_result = self._execute_sql(SCAN_EXT_SEGMENT_EXPIRE_MESSAGE_SQL % (
                current_timestamp, MESSAGE_STATUS_LOCKED, MESSAGE_STATUS_FAILED)).fetchall()
            if list_is_empty(execute_result):
                return execute_result

            mq_message_list = list()
            for element in execute_result:
                mq_message_list.append(MQMessage(
                    message_topic=self._get_str_column(element[0]),
                    message_uuid=self._get_str_column(element[1])
                ))

            return mq_message_list

        @synchronized(mq_operation_lock_key)
        def do_recover_message_status(self, recover_message_uuid_list):
            # 虽然在 Common Segment 中加入了判断条件
            # 但是 Hold 与 Commit 都会让消息的状态最终一致
            message_uuid_list_str = ""
            for message_uuid in recover_message_uuid_list:
                message_uuid_list_str += "'%s', " % message_uuid
            recover_message_sql = SEGMENT_RECOVER_MESSAGE_SQL % (
                message_uuid_list_str[:-2], get_current_timestamp()
            )
            self._execute_sql(recover_message_sql)

        def fetch_message_by_uuid(self, message_uuid):
            """
            :rtype: MQMessage
            """
            execute_result = self._execute_sql(FETCH_EXT_MESSAGE_BY_MESSAGE_UUID_SQL % message_uuid).fetchone()
            if execute_result is None or len(execute_result) == 0:
                return None

            return MQMessage(
                message_id=execute_result[0],
                message_topic=self._get_str_column(execute_result[1]),
                message_text=self._get_str_column(execute_result[2]),
                message_status=execute_result[3],
                create_time=execute_result[4],
                update_time=execute_result[5],
                expire_time=execute_result[6],
                consumer=self._get_str_column(execute_result[7]),
                failed_times=execute_result[8],
                producer=self._get_str_column(execute_result[9]),
                message_uuid=message_uuid
            )

        def get_db_path(self):
            return self._db_path

    return SimpleSQLiteBrokerExtSegment()

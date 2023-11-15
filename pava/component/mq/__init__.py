# coding=utf-8

# 消息状态 初始化
MESSAGE_STATUS_INIT = 0
# 消息状态 锁定中 (或 消费中)
MESSAGE_STATUS_LOCKED = 1
# 消息状态 消费完成 (正常结束)
MESSAGE_STATUS_DONE = 2
# 消息状态 已删除
MESSAGE_STATUS_DELETE = 3
# 消息状态 消费失败 (待重试)
MESSAGE_STATUS_FAILED = 4
# 消息状态 消费挂起 (无法重试)
MESSAGE_STATUS_PENDING = 5
# 消息状态 暂停消费
MESSAGE_STATUS_SUSPEND = 6

# 供外部使用的 Segment 接口 Ext Segment Key
EXT_SQLITE_MQ_SEGMENT = "ext_sqlite_mq_segment"
# 归档器 Segment Key
ARCHIVER_SQLITE_MQ_SEGMENT = "archiver_sqlite_mq_segment"

# 事务开启语句
BEGIN_TRANSACTION = "BEGIN;"

# 存储消息的 主要 Segment 表名 一般的 Topic 都存放这里
# 根据文档 https://www.sqlite.org/autoinc.html
# SQLite 会自动使用 ROWID 规则 无需 Autoincrement
CREATE_SIMPLE_SQLITE_MQ_COMMON_SEGMENT_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS simple_sqlite_mq (
        message_id     INTEGER PRIMARY KEY,
        message_text   TEXT,
        message_status INTEGER,
        create_time    INTEGER,
        update_time    INTEGER,
        consumer       TEXT,
        expire_time    INTEGER,
        failed_times    INTEGER,
        producer       TEXT,
        uuid           TEXT
    );
"""

# 存储消息的 Ext Segment 作用为对外接口 以及 Archiver Segment 归档存储
CREATE_SIMPLE_SQLITE_MQ_EXT_SEGMENT_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS simple_sqlite_mq (
        message_id     INTEGER,
        message_topic  TEXT,
        message_text   TEXT,
        message_status INTEGER,
        create_time    INTEGER,
        update_time    INTEGER,
        consumer       TEXT,
        expire_time    INTEGER,
        failed_times    INTEGER,
        producer       TEXT,
        uuid           TEXT
    );
"""

# 在 UUID 上建立索引 业务唯一ID
CREATE_INDEX_UUID = """
    CREATE INDEX IF NOT EXISTS idx_uuid ON simple_sqlite_mq (uuid);
"""

# 在 Topic 上建立索引 供 Ext Segment 在管理消息时使用
CREATE_INDEX_MESSAGE_TOPIC = """
    CREATE INDEX IF NOT EXISTS idx_message_topic ON simple_sqlite_mq (message_topic);
"""

# 在 Expire Time 上建立索引 供 Ext Segment 在 Recover Message 时使用
CREATE_INDEX_EXPIRE_TIME = """
    CREATE INDEX IF NOT EXISTS idx_expire_time ON simple_sqlite_mq (expire_time);
"""

# 在 Update Time 上建立索引 供 Ext Segment 在 管理消息 时使用
CREATE_INDEX_UPDATE_TIME = """
    CREATE INDEX IF NOT EXISTS idx_update_time ON simple_sqlite_mq (update_time);
"""

# 向 Common Segment 添加消息的 SQL
ADD_MESSAGE_TO_COMMON_SEGMENT_TABLE_SQL = """
    INSERT INTO simple_sqlite_mq (
        message_text,
        message_status,
        create_time,
        update_time,
        expire_time,
        failed_times,
        producer,
        consumer,
        uuid
    ) VALUES (
        '%s',
        %s,
        %s,
        %s,
        %s,
        0,
        '%s',
        '%s',
        '%s'
    )
"""

# 向 Ext Segment 以及 Archiver Segment 添加消息的 SQL
ADD_MESSAGE_TO_EXT_SEGMENT_TABLE_SQL = """
    INSERT INTO simple_sqlite_mq (
        message_id,
        message_topic,
        message_text,
        message_status,
        create_time,
        update_time,
        expire_time,
        failed_times,
        producer,
        consumer,
        uuid
    ) VALUES (
        '%s',
        '%s',
        '%s',
        %s,
        %s,
        %s,
        %s,
        %s,
        '%s',
        '%s',
        '%s'
    )
"""

# 用于 Common Segment 获取未消费的消息
GET_COMMON_SEGMENT_MESSAGE_SQL = """
    SELECT
        message_id,
        message_text,
        create_time,
        failed_times,
        producer,
        uuid
    FROM
        simple_sqlite_mq
    WHERE
        message_status = 0
    LIMIT 1
"""

# 用于 Common Segment 更新消息
UPDATE_COMMON_SEGMENT_MESSAGE_SQL = """
    UPDATE
        simple_sqlite_mq
    SET
        message_status = %s,
        update_time = %s,
        consumer= '%s',
        expire_time= %s,
        failed_times= %s
    WHERE
        uuid = '%s'
"""

# 用于 Ext Segment 更新消息
UPDATE_EXT_SEGMENT_MESSAGE_SQL = """
    UPDATE
        simple_sqlite_mq
    SET
        message_status = %s,
        update_time = %s,
        consumer= '%s',
        expire_time= %s,
        failed_times= %s
    WHERE
        uuid = '%s'
"""


# 删除 Common Segment 的 Message
DELETE_COMMON_SEGMENT_MESSAGE_SQL = """
    DELETE FROM
        simple_sqlite_mq
    WHERE
        uuid = '%s'
"""

# 删除 Ext Segment 的 Message
DELETE_EXT_SEGMENT_MESSAGE_SQL = """
    DELETE FROM
        simple_sqlite_mq
    WHERE
        message_topic = '%s'
    AND
        uuid = '%s'
"""

# Ext Segment 在检索消息的时候使用
SCAN_EXT_SEGMENT_MESSAGE_SQL = """
    SELECT
        message_id,
        message_topic,
        message_text,
        message_status,
        create_time,
        update_time,
        consumer,
        expire_time,
        failed_times,
        producer,
        uuid
    FROM
        simple_sqlite_mq %s
    ORDER BY
        update_time DESC
    LIMIT %s, %s
"""

# Ext Segment 在检索消息的时候 指定 Topic
WHERE_MESSAGE_TOPIC_STR = "WHERE message_topic = '%s'"

# Ext Segment 在检索消息的时候 指定 状态
WHERE_MESSAGE_STATUS_STR = "WHERE message_status = %s"

# 获取 Ext Segment 中 需要进行 Recover 的消息
SCAN_EXT_SEGMENT_EXPIRE_MESSAGE_SQL = """
    SELECT
        message_topic,
        uuid
    FROM
        simple_sqlite_mq
    WHERE
        expire_time < %s
    AND
        expire_time > 1
    AND
        (
            message_status = %s
        OR
            message_status = %s
        )
    LIMIT 50
"""

# 对 Common Segment 中的 Message 进行恢复
RECOVER_COMMON_SEGMENT_MESSAGE_SQL = """
    SELECT
        message_topic,
        uuid
    FROM
        simple_sqlite_mq
    WHERE
        expire_time < %s
    AND
        expire_time > 1
    AND
        (
            message_status = %s
        OR
            message_status = %s
        )
    LIMIT 50
"""

# Common Segment 与 Ext Segment 执行 Recover Message
SEGMENT_RECOVER_MESSAGE_SQL = """
    UPDATE
        simple_sqlite_mq
    SET
        message_status = 0,
        expire_time = 0
    WHERE
        (
            message_status = 1
        OR
            message_status = 4
        )
    AND
        uuid in (%s)
    AND
        expire_time > 0
    AND
        expire_time < %s
"""

# 用于 Ext Segment 更新消息
FETCH_EXT_MESSAGE_BY_MESSAGE_UUID_SQL = """
    SELECT
        message_id,
        message_topic,
        message_text,
        message_status,
        create_time,
        update_time,
        expire_time,
        consumer,
        failed_times,
        producer
    FROM
        simple_sqlite_mq
    WHERE
        uuid = '%s'
    LIMIT 1
"""

# 用于 Common Segment 预锁定消息
FETCH_COMMON_MESSAGE_BY_MESSAGE_UUID_SQL = """
    SELECT
        message_id,
        message_text,
        message_status,
        create_time,
        update_time,
        expire_time,
        consumer,
        failed_times,
        producer
    FROM
        simple_sqlite_mq
    WHERE
        uuid = '%s'
    LIMIT 1
"""
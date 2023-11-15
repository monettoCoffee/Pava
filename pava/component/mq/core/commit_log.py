# coding=utf-8
from pava.component.mq.core.mq_message import MQMessage

from pava.decorator.decorator_impl.type_check_decorator import type_check


class CommitLog(object):
    @type_check(None, int, str, MQMessage)
    def __init__(self, commit_log_id=None, commit_log_file=None, mq_message=None):
        self.commit_log_id = commit_log_id  # type: int
        self.mq_message = mq_message  # type: MQMessage
        self.commit_log_file = commit_log_file  # type: str

    def __lt__(self, other_commit_log):
        return self.commit_log_id < other_commit_log.commit_log_id

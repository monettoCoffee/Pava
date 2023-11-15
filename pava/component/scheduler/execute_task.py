# coding=utf-8
from threading import Lock

from pava.component.p_log import PLog

from pava.utils.time_utils import *

from pava.decorator.decorator_impl.type_check_decorator import type_check

"""
基础的 执行任务类
只具备 防止重复运行的能力
"""


class ExecuteTask(object):
    @type_check(None, str, None, int)
    def __init__(self, execute_task_name, execute_task_function, schedule_task_execute_cycle):
        # 任务名
        self.execute_task_name = execute_task_name
        # 原始的计划运行周期
        self.cycle_run_time_seconds = get_int_value(schedule_task_execute_cycle)
        # 原始执行的函数
        self.execute_task_function = execute_task_function
        # 标记是否正在运行中 设计为不可重复运行
        self._running_signal = False
        # 重复运行锁
        self._run_lock = Lock()

    def get_next_run_time(self):
        """
        获取周期执行时间中的
        :return:
        """
        return get_current_timestamp() + self.cycle_run_time_seconds

    def execute(self):
        if self.execute_task_function is None:
            return

        # 防止并发运行
        if self._running_signal is False:
            with self._run_lock:
                if self._running_signal is False:
                    # 持有锁 标记为运行中
                    self._running_signal = True
                else:
                    PLog.gets().info(
                        "[ExecuteTask] [M-T] The task '%s' is running, can not run again..." % self.execute_task_name
                    )
                    return
        else:
            PLog.gets().info(
                "[ExecuteTask] The task '%s' is running, can not run again..." % self.execute_task_name
            )
            return

        try:
            self.execute_task_function()
        except Exception as e:
            PLog.gets().exception(e)
        self._running_signal = False

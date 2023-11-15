# coding=utf-8
import bisect

from pava.component.p_log import PLog
from pava.component.scheduler.execute_task import ExecuteTask
from pava.component.thread_pool.core.thread_pool_executor_wrapper import ThreadPoolExecutorWrapper
from pava.utils.time_utils import *

"""
基于 执行任务类 包装一层的调度任务类
"""


class ScheduleTask(object):
    def __init__(self, execute_task):
        """
        初始化调度执行任务
        :type execute_task: ExecuteTask
        """

        # 初始化 预期执行时间
        self.anticipate_execute_timestamp = execute_task.get_next_run_time()
        # 原始执行任务
        self.execute_task = execute_task
        self.debug_execute_time_str = timestamp_to_str(self.anticipate_execute_timestamp)

    def execute(self, scheduler_thread_pool, enable_debug=False):
        """
        使用 调度器的线程池 执行业务逻辑
        :type scheduler_thread_pool: ThreadPoolExecutorWrapper
        :return:
        """
        if enable_debug:
            PLog.gets().info(
                "[ScheduleTask DEBUG] Execute task is None: %s" % self.execute_task is None
            )

        if self.execute_task is None:
            if enable_debug:
                PLog.gets().info(
                    "[ScheduleTask DEBUG] Execute task is none, ignored..."
                )
            return

        if scheduler_thread_pool is None:
            raise Exception(
                "[ScheduleTask] Cannot run task '%s' by none thread pool!" % self.execute_task.execute_task_name)

        # 线程池中异步执行
        if enable_debug:
            PLog.gets().info(
                "[ScheduleTask DEBUG] Ready use scheduler thread pool to run!"
            )

        scheduler_thread_pool.submit(self.execute_task.execute)
        if enable_debug:
            PLog.gets().info(
                "[ScheduleTask DEBUG] Execute task submit success! Execute function: '%s', Schedule name: '%s'" % (
                self.get_execute_function_name(), self.get_schedule_name())
            )
        return

    def get_next_run_time(self):
        """
        获取下次运行的时间
        """
        if self.execute_task is None:
            return 0
        return get_int_value(self.execute_task.get_next_run_time())

    def get_execute_function_name(self):
        if self.execute_task is None:
            return ""
        if self.execute_task.execute_task_function is None:
            return ""
        return self.execute_task.execute_task_function.__name__

    def get_schedule_name(self):
        if self.execute_task is None:
            return ""
        return self.execute_task.execute_task_name

    def __lt__(self, other_execute_task):
        """
        方便使用 bisect 的 Insert 方法构建有序的 ScheduleTaskList 数组中的对比排序方法
        :type other_execute_task: ScheduleTask
        """
        return self.anticipate_execute_timestamp < other_execute_task.anticipate_execute_timestamp


if __name__ == '__main__':
    # s = ScheduleTask()
    s = None

    sorted_foos = sorted([ScheduleTask(7, s), ScheduleTask(1, s), ScheduleTask(3, s), ScheduleTask(9, s)])

    c = bisect.insort(sorted_foos, ScheduleTask(2, s))
    pass

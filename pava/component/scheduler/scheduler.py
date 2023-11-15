# coding=utf-8
import bisect
from pava.component.p_log import PLog
from pava.component.thread_pool.core.thread_pool_executor_wrapper import ThreadPoolExecutorWrapper
from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.component.scheduler.schedule_task import ScheduleTask
from pava.component.scheduler.execute_task import ExecuteTask
from pava.utils.time_utils import *

"""
周期执行任务调度器
支持 动态 添加 与 删除 任务
"""

# TODO 需要进行小幅度重构
# TODO 需要进行小幅度重构
# TODO 需要进行小幅度重构
# TODO 需要进行小幅度重构
# TODO 需要进行小幅度重构

SCHEDULER_LOCK_KEY_PREFIX = "Scheduler_"
SCHEDULER_LOCK_KEY_COUNT = 0
_DEFAULT_EXECUTE_SLEEP_TIME = 1.0
_DEFAULT_QUICK_SLEEP_TIME = 0


@synchronized(SCHEDULER_LOCK_KEY_PREFIX)
def get_scheduler(thread_pool_size=128, default_thread_pool_=None, enable_debug=False):
    global SCHEDULER_LOCK_KEY_COUNT
    SCHEDULER_LOCK_KEY_COUNT += 1
    scheduler_operation_lock_key = SCHEDULER_LOCK_KEY_PREFIX + str(SCHEDULER_LOCK_KEY_COUNT)

    class Scheduler(object):
        def __init__(self, thread_pool_size):
            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Initial scheduler ...")

            thread_pool_size = get_int_value(thread_pool_size)
            if thread_pool_size < 1 or thread_pool_size > 512:
                raise Exception("ThreadPool size must > 1 and < 512!")

            # 首次只 Sleep Quick Time 秒 方便执行短速任务 之后默认循环 DEFAULT_EXECUTE_SLEEP_TIME
            self._execute_sleep_time = _DEFAULT_QUICK_SLEEP_TIME
            self._original_execute_task_dict = dict()
            self._schedule_chain = list()

            # 自身的调度任务处理池
            self._thread_pool = default_thread_pool_
            if self._thread_pool is None:
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] Pool is None, init new pool ...")
                self._thread_pool = ThreadPoolExecutorWrapper(max_workers=thread_pool_size)

            # 一切就绪 开始执行
            self._thread_pool.submit(self._loop_execute)

            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Initial scheduler done.")

            self._thread_pool.submit(self.__loop_debug_log)

        @synchronized(scheduler_operation_lock_key)
        def add_schedule_task(self, schedule_task_name, schedule_task_function, schedule_task_execute_cycle,
                              rewrite_task=False):
            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Add task '%s' ..." % schedule_task_name)

            execute_task = self._original_execute_task_dict.get(schedule_task_name, None)  # type: ExecuteTask

            # 新添加任务的场景
            if execute_task is None:
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] New task '%s' ..." % schedule_task_name)

                execute_task = ExecuteTask(schedule_task_name, schedule_task_function, schedule_task_execute_cycle)
                self._original_execute_task_dict[schedule_task_name] = execute_task
                self._add_task_to_execute_chain(execute_task)
            else:
                # 默认不允许重复添加任务
                if rewrite_task is False:
                    raise Exception(
                        "[Scheduler] Task '%s' already exists! Cannot add task repeatedly!" % schedule_task_name)

                # 重复添加的场景 直接覆盖
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] Rewrite task '%s' ..." % schedule_task_name)

                execute_task.execute_task_function = schedule_task_function
                execute_task.cycle_run_time_seconds = schedule_task_execute_cycle

        @synchronized(scheduler_operation_lock_key)
        def _add_task_to_execute_chain(self, execute_task):
            """
            :type execute_task: ExecuteTask
            """
            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Add task '%s' to chain ..." % execute_task.execute_task_name)

            # 将 ScheduleTask 加入执行计划 这里 bisect 会按照执行时间排序
            bisect.insort(self._schedule_chain, ScheduleTask(execute_task))

        @synchronized(scheduler_operation_lock_key)
        def remove_schedule_task(self, schedule_task_name, stop_task=True):
            """
            :type schedule_task_name: str
            :type stop_task: bool
            """
            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Remove task '%s' ..." % schedule_task_name)

            execute_task = self._original_execute_task_dict.get(schedule_task_name, None)  # type: ExecuteTask
            if execute_task is None:
                raise Exception("[Scheduler] No task named '%s'" % schedule_task_name)
            else:
                PLog.gets().debug("[Scheduler DEBUG] Remove task '%s' ..., and remove execute '%s', '%s'" % (
                    schedule_task_name, execute_task.execute_task_name, execute_task.execute_task_function.__name__))
                if stop_task:
                    execute_task.execute_task_function = None
                self._original_execute_task_dict.pop(schedule_task_name)

        @synchronized(scheduler_operation_lock_key)
        def _execute(self):
            # 没有任务 无需调度
            if len(self._schedule_chain) == 0:
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] No task in scheduler ...")

                # 没有任务时 Sleep Time 恢复 Default Sleep Time 防止 某一时刻 为 0 无限占用 CPU
                self._set_next_sleep_time(_DEFAULT_EXECUTE_SLEEP_TIME + get_current_timestamp())
                return

            # schedule_chain 的顺序已经按照时间顺序排序好了 直接取 0 就是最近的那个
            schedule_task = self._schedule_chain[0]  # type: ScheduleTask

            # 预期执行时间达到了 则进行执行
            if schedule_task.anticipate_execute_timestamp <= get_current_timestamp():
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] Ready to run '%s' ..." % schedule_task.get_schedule_name())

                # 将自身的线程池传递给 ScheduleTask 来异步执行任务
                PLog.gets().debug("[Scheduler] Run task '%s'" % schedule_task.execute_task.execute_task_name)
                schedule_task.execute(self._thread_pool, enable_debug=enable_debug)

                # 将已经提交执行的任务排除
                self._schedule_chain.pop(0)

                if enable_debug:
                    PLog.gets().info(
                        "[Scheduler DEBUG] Task '%s' submitted, pop ..." % schedule_task.get_schedule_name()
                    )

                # 如果是单次延时任务执行的情况 这里不需要放到下次的执行计划中
                if schedule_task.get_execute_function_name().startswith("delay_execute_function_wrapper"):
                    PLog.gets().debug(
                        "[Scheduler] Task '%s' is once task, return..." % schedule_task.get_execute_function_name()
                    )
                    # 重新设置 Sleep Time 设置 Quick Time 的原因是 某些 延时 Task 可能会开启新的 秒级延时的 Task 所以需要在比较短的时间再次唤醒
                    self._set_next_sleep_time(_DEFAULT_QUICK_SLEEP_TIME + get_current_timestamp())
                    # 将单次调度任务移除
                    self.remove_schedule_task(schedule_task.get_schedule_name(), stop_task=False)
                    return

                # 如果已经移除任务 则不需要加入到 执行计划
                if schedule_task.get_next_run_time() == 0:
                    # 重新设置 Sleep Time 设置 Quick Time 的原因是 某些 延时 Task 可能会开启新的 秒级延时的 Task 所以需要在比较短的时间再次唤醒
                    self._set_next_sleep_time(_DEFAULT_QUICK_SLEEP_TIME + get_current_timestamp())
                    PLog.gets().debug(
                        "[Scheduler] Task '%s' has been removed, return..." % schedule_task.execute_task.execute_task_name
                    )
                    return

                # 执行完毕一次后 再次将任务加入调度执行链
                self._add_task_to_execute_chain(schedule_task.execute_task)
                if enable_debug:
                    PLog.gets().info(
                        "[Scheduler DEBUG] Task '%s' add to chain again..." % schedule_task.get_schedule_name()
                    )

                # Quick Time 的原因同上
                self._set_next_sleep_time(_DEFAULT_QUICK_SLEEP_TIME + get_current_timestamp())
            else:
                if enable_debug:
                    PLog.gets().info(
                        "[Scheduler DEBUG] Task '%s' can not run in time..." % schedule_task.get_schedule_name()
                    )

                # schedule_task.anticipate_execute_timestamp 是原本预计 Sleep 为止的时间(没执行任务), 这里对比以重新设置 Sleep Time
                self._set_next_sleep_time(schedule_task.anticipate_execute_timestamp)

        def _loop_execute(self):
            """
            循环执行 一直执行任务 主流程
            """
            while True:
                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] Loop execute...")

                time_sleep(self._execute_sleep_time)
                self._execute()

        @synchronized(scheduler_operation_lock_key)
        def _set_next_sleep_time(self, run_timestamp):
            """
            设置距离下次调度任务的睡眠时间
            :param run_timestamp: 任务下次调度的时间 这里会自动进行对比 以选取最优睡眠时间
            """
            waiting_time_seconds = run_timestamp - get_current_timestamp()
            if enable_debug:
                PLog.gets().info("[Scheduler DEBUG] Except waiting time is '%s'" % waiting_time_seconds)

            # 如果没有达到执行时间 则 继续 Sleep
            if waiting_time_seconds > _DEFAULT_EXECUTE_SLEEP_TIME:
                # 间隔时间过长的情况下 保持 Default 秒唤醒一次
                self._execute_sleep_time = _DEFAULT_EXECUTE_SLEEP_TIME

                if enable_debug:
                    PLog.gets().info("[Scheduler DEBUG] A: Real waiting time is '%s'" % self._execute_sleep_time)

            else:
                # 还有 n 秒就要执行了 n 秒之后唤醒执行
                if waiting_time_seconds >= 0:
                    self._execute_sleep_time = waiting_time_seconds

                    if enable_debug:
                        PLog.gets().info("[Scheduler DEBUG] B: Real waiting time is '%s'" % self._execute_sleep_time)
                else:
                    # 如果已经超时了 不要等待 立即执行
                    self._execute_sleep_time = 0

                    if enable_debug:
                        PLog.gets().info("[Scheduler DEBUG] C: Not sleep, execute immediately ...")

        def __loop_debug_log(self):
            while True:
                PLog().gets().debug("[Scheduler DEBUG] There has some task waiting for execute...")
                log_str = "\n\n[Scheduler DEBUG]\n"
                log_element_list = list()
                for task in self._schedule_chain:  # type: ScheduleTask
                    log_element_list.append("Run: '%s', Task: '%s', function: '%s'" % (
                        timestamp_to_str(task.anticipate_execute_timestamp), task.get_schedule_name(),
                        task.get_execute_function_name()))
                log_element_list.sort()
                PLog().gets().debug(log_str + str_list_compose(log_element_list, sep_character="\n") + "\n\n")
                time_sleep(5 * 60)

    return Scheduler(thread_pool_size)


if __name__ == '__main__':
    def print_it1():
        print(get_current_time_str())


    def print_it2():
        print(get_current_time_str())


    s = get_scheduler()
    s.add_schedule_task("P_TASK", print_it2, 8)
    s.add_schedule_task("P_TASK", print_it1, 3)
    time_sleep(20)
    s.remove_schedule_task("P_TASK")
    main_thread_hang()

# coding=utf-8
import multiprocessing
import threading

from pava.component.p_log import PLog
from pava.utils.time_utils import *
from pava.component.scheduler.scheduler import get_scheduler

from pava.component.thread_pool.core.thread_pool_executor_wrapper import ThreadPoolExecutorWrapper

# 全局异步任务处理线程池

__DEFAULT_ASYNC_THREAD_POOL_EXECUTOR = ThreadPoolExecutorWrapper(max_workers=256)
__DEFAULT_SCHEDULER = get_scheduler(thread_pool_size=256, default_thread_pool_=__DEFAULT_ASYNC_THREAD_POOL_EXECUTOR,
                                    enable_debug=False)


def async_execute(execute_function, *args, **kwargs):
    """
    异步运行某个函数
    """
    if execute_function is None:
        raise Exception("[async_execute] Execute function is None!")
    args = tuple() if args is None else args
    kwargs = dict() if kwargs is None else kwargs
    return __DEFAULT_ASYNC_THREAD_POOL_EXECUTOR.submit(execute_function, *args, **kwargs)


@type_check(str, None, [int, float, long, str])
def cycle_execute(task_name, execute_function, cycle_run_seconds):
    """
    周期性执行某个函数
    :param task_name: 任务名称
    :param execute_function: 执行的函数
    :param cycle_run_seconds: 几秒执行一次
    """
    if str_is_blank(task_name):
        raise Exception("[cycle_execute] Task name is blank!")
    if execute_function is None:
        raise Exception("[cycle_execute] Execute function is None!")
    cycle_run_seconds = get_int_value(cycle_run_seconds)
    __DEFAULT_SCHEDULER.add_schedule_task(task_name, execute_function, cycle_run_seconds)


@type_check(str, None, str)
def cycle_execute_by_fixed_time(task_name, execute_function, run_time_str):
    """
    周期性执行某个函数 在每天的固定时间
    :param task_name: 任务名称
    :param execute_function: 执行的函数
    :param run_time_str: 每天的什么时间执行
    """

    # 参数校验
    if str_is_blank(task_name):
        raise Exception("[cycle_execute_by_fixed_time] Task name is blank!")

    if len(run_time_str) != len("19:53") and len(run_time_str) != len("19:53:01"):
        raise Exception("[cycle_execute_by_fixed_time] Invalid time str '%s' !" % run_time_str)

    if execute_function is None:
        raise Exception("[cycle_execute_by_fixed_time] Execute function is None!")

    # 生成唯一任务名
    register_task_name = "%s_%s_%s_%s" % (
        task_name, run_time_str, get_current_timestamp(), str(threading.currentThread()))

    # 计算第一次预计在什么时间运行
    time_interval = calculate_time_interval(run_time_str)
    if time_interval < 0:
        PLog.gets().debug(
            "[cycle_execute_by_fixed_time] Time interval is '%s', time out, next run '%s' after in %s" % (
                time_interval, task_name, timestamp_to_str(get_current_timestamp() + 24 * 3600 + time_interval)
            )
        )
        time_interval = 24 * 3600 + time_interval
    else:
        PLog.gets().debug(
            "[cycle_execute_by_fixed_time] Time interval is '%s', will run '%s' after in %s" % (
                time_interval, task_name, timestamp_to_str(get_current_timestamp() + time_interval)
            )
        )

    # 封装成可以循环执行的函数
    def fixed_cycle_run_wrapper():
        try:
            execute_function()
        except Exception as e:
            PLog.gets().exception(e)

        # 计算再下次 什么时候运行
        next_time_interval = calculate_time_interval(run_time_str)
        if next_time_interval < 0:
            next_time_interval = 24 * 3600 + next_time_interval

        # 设置下次运行时间周期
        delay_execute(register_task_name, fixed_cycle_run_wrapper, next_time_interval)
        PLog.gets().debug(
            "[cycle_execute_by_fixed_time] Task '%s' run done, next will run in %s" % (
                register_task_name, timestamp_to_str(get_current_timestamp() + next_time_interval)
            )
        )

    # 通过延时任务来开始
    delay_execute(register_task_name, fixed_cycle_run_wrapper, time_interval)


# TODO 搜这个 Commit 的 system_utils 的 __name__ 里面就有 线程池提交不调度的情况 .......
__SOME = ""


@type_check(str, None, [int, float, long, str])
def delay_execute(task_name, execute_function, run_after_seconds):
    """
    延迟执行某个函数
    :param task_name: 任务名称
    :param execute_function: 执行的函数
    :param run_after_seconds: 多少秒后执行
    """

    #  将任务抽象为 单次执行的调度任务
    def delay_execute_function_wrapper():
        try:
            execute_function()
        except Exception as e:
            PLog.gets().exception(e)

    # 延时执行主体逻辑
    if str_is_blank(task_name):
        raise Exception("[delay_execute] Task name is blank!")
    if execute_function is None:
        raise Exception("[delay_execute] Execute function is None!")
    run_after_seconds = get_int_value(run_after_seconds)
    __DEFAULT_SCHEDULER.add_schedule_task(
        "%s_%s_%s" % (task_name, get_current_timestamp(), str(threading.currentThread())),
        delay_execute_function_wrapper, run_after_seconds)


def execute_by_process(execute_function, limit_time_seconds=0, join=False):
    """
    启用多进程对任务进行执行
    :param execute_function: 需要进行多进程执行的方法
    :param limit_time_seconds: 进程是否存在存活时间
    :param join: 是否要同步执行
    :type join: bool
    :return: 任务提交是否成功 以及 是否超时的 Bool
    """

    # 需要能够提示异常信息
    def execute_function_wrapper():
        try:
            return execute_function()
        except Exception as e:
            PLog.gets().exception(e)

    # 使用其他进程执行这个方法
    process_object = multiprocessing.Process(target=execute_function_wrapper)

    # 开启这个进程
    process_object.start()

    # 如果 没有对执行时间进行限制 同时 也不需要同步执行 那么直接 Return
    if limit_time_seconds == 0 and not join:
        return True

    # 对执行时间进行限制
    begin_timestamp = get_current_timestamp(trans_to_seconds=False)

    # 对限制执行时间的逻辑进行封装
    def prepare_to_kill_wrapper():
        if limit_time_seconds > 0:
            process_object.join(timeout=limit_time_seconds)
            if get_current_timestamp(trans_to_seconds=False) - begin_timestamp < limit_time_seconds:
                return True
            PLog.gets().warning("[execute_by_process] Execute not in limit time ... function: '%s', limit: '%s'" % (
                execute_function, limit_time_seconds))

            # 主动终止进程运行
            try:
                process_object.terminate()
            except Exception as e:
                PLog.gets().warning("[execute_by_process] Cannot stop function: '%s', limit: '%s', exception: '%s'" % (
                    execute_function, limit_time_seconds, str(e)))
            return False

        else:
            process_object.join()
            return True

    # 是否同步执行 如果异步执行 则扔到线程池里
    if join:
        return prepare_to_kill_wrapper()
    else:
        async_execute(prepare_to_kill_wrapper)
        return True

# if __name__ == '__main__':
#     a = get_current_timestamp()
#
#
#     def cycle_task_1():
#         print("Execute Cycle TaskA, after %s" % str(get_current_timestamp() - a))
#
#
#     def delay_task_1():
#         print("Execute Delay TaskB, after %s" % str(get_current_timestamp() - a))
#         cycle_execute("test_task", cycle_task_1, 1)
#
#
#     delay_execute("delay_task", delay_task_1, 3)
#
#     main_thread_hang()

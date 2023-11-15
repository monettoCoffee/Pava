# coding=utf-8
from concurrent.futures import ThreadPoolExecutor

from pava.component.p_log import PLog
from pava.utils.object_utils import main_thread_hang
import threading

"""
对 Python 线程池的一次简单的封装
使其能够显示的抛出异常
"""


class ThreadPoolExecutorWrapper(object):

    def __init__(self, max_workers=None):
        self._pool_worker_size = max_workers
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)

    def submit(self, run_function, *args, **kwargs):
        """
        提交任务到线程池中
        会将任务包装一层 如果有异常 那么会打印到控制台 以及输出日志
        :param run_function: 需要提交到线程池中的异步任务
        :param args: 执行这个异步任务所需要的参数 Args
        :param kwargs: 执行这个异步任务所需要的参数 Kwargs
        :return: 原始提交到线程池的 Task, 可以直接调用 Task.result() 获取执行结果
        """

        PLog.gets().debug(
            "[ThreadPoolExecutorWrapper] Submitted task '%s' successfully ..." % run_function.__name__
        )

        # TODO 23.06.27 可能有问题? 暂时没有复现
        # 这里通过 max_workers 这个参数来限制同时运行的异步任务

        def _handle_function_with_exception(*args, **kwargs):
            try:
                return run_function(*args, **kwargs)
            except Exception as e:
                PLog.gets().exception(e)
                raise e

        submitted_task = self._thread_pool.submit(_handle_function_with_exception, *args, **kwargs)
        # threading.Thread(target=submitted_task.result).start()

        # submitted_task = None
        # threading.Thread(target=_handle_function_with_exception, args=(run_function,)).start()

        # 但是目前发现 在编写定时任务的时候 有提交到线程池 但是不执行的任务 像是挂起了 也没有线程进行调度
        # 进入 futures 模块 Debug 成本略高 暂时先用 Thread.start() 负责调度起来任务
        # 这样的话 可以保证任务一定是被调度起来的 但是其实也失去了线程池复用线程的优势 只有限制异步任务同时运行数量的好处了
        # Thread.start() 只保证是等待线程执行结束的 不做额外逻辑 所以应该只有创建线程的成本
        # threading.Thread(target=submitted_task.result).start()
        # Task.result() 是可以被重复运行的 这里依然返回 Task 方便业务代码获取执行结果
        return submitted_task


if __name__ == '__main__':
    def print_it(a, b=5):
        print(a)
        print(b)
        c = 1 / 0


    w = ThreadPoolExecutorWrapper()
    r = w.submit(print_it, 1, 22)
    r.result()
    main_thread_hang()

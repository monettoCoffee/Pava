# coding=utf-8


# 导入线程模块
import threading

# 创建event事件
import time

from pava.utils.async_utils import async_execute

eEvent = threading.Event()


class TestThread(object):
    def __init__(self):
        self.threading_event = threading.Event()

    def handle(self):
        while True:
            self.threading_event.wait()
            # if not self.threading_event.is_set():
            #     continue
            print("Run...")
            time_sleep(1)


if __name__ == "__main__":
    t = TestThread()
    async_execute(t.handle)
    # t.threading_event.set()
    # time_sleep(3)
    # t.threading_event.clear()
    time_sleep(5)
    t.threading_event.set()
    time_sleep(5)
    t.threading_event.clear()
    time_sleep(100)

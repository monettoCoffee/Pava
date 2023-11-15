# coding=utf-8
import os
import time

from pava.decorator.decorator_impl.type_check_decorator import type_check
from pava.utils.object_utils import get_int_value, time_sleep
from pava.utils.system_utils import current_user_is_not_root

"""
修改系统时间后执行某些任务
一定时间后恢复
需要使用 root 用户执行 否则缺少修改系统时间权限
使用示例请参考 main
建议使用 sudo python -c "from pava.system.fake_time import fake_time_todo; fake_time_todo('<SOME_SYSTEM_COMMAND>')"
"""

# 默认伪造时间秒数
DEFAULT_FAKE_TIME_PERSISTENT_SECONDS = 5

# 起始时间 或 当前时间
BEGIN_TIME_STAMP = get_int_value(time.time())
time.strftime("%m%d%H%M%Y", time.localtime(BEGIN_TIME_STAMP))

# 目标时间戳
DEFAULT_TARGET_TIME_STAMP = time.mktime(time.strptime(
    "2007-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))


@type_check(str, int)
def fake_time_todo(command, sleep_time=DEFAULT_FAKE_TIME_PERSISTENT_SECONDS):
    os.popen("date 010100002007")
    os.popen(command)
    # 休眠设定秒数后恢复系统时间
    time_sleep(sleep_time)
    add_time = get_int_value(int(time.time()) - DEFAULT_TARGET_TIME_STAMP)
    new_date = (time.strftime("%m%d%H%M%Y", time.localtime(BEGIN_TIME_STAMP)))
    os.popen("date %s" % new_date)


if __name__ == '__main__':
    if current_user_is_not_root():
        print("[pava][fake_time.py] Please ensure use fake time as Root.")
    fake_time_todo("open /System/Applications/Calculator.app")

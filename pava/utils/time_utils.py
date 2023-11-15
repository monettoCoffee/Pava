# coding=utf-8
import datetime
import re

from pava.utils.object_utils import *

# 2020-01-01 19:53:01
__DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# 2020-01-01 19:53
__DEFAULT_TIME_WITHOUT_SECONDS_FORMAT = "%Y-%m-%d %H:%M"

# 2020-01-01
__DEFAULT_DATE_FORMAT = "%Y-%m-%d"

# 19:53:01
__DEFAULT_HOUR_MINUTE_SECONDS_FORMAT = "%H:%M:%S"

# 19:53
__DEFAULT_HOUR_MINUTE_FORMAT = "%H:%M"


def get_current_timestamp(trans_to_seconds=True, save_round=2):
    """
    获取当前系统时间戳
    :param trans_to_seconds: 是否要转换成秒级别的
    :type trans_to_seconds: bool
    :param save_round: 保留几位小数
    :type save_round: int
    :return: 当前时间戳
    """

    save_round = get_int_value(save_round)
    if trans_to_seconds:
        return int(time.time())
    return round(time.time(), save_round)


def get_current_time_str():
    return timestamp_to_str(get_current_timestamp(), __DEFAULT_TIME_FORMAT)


def get_current_hours():
    return datetime.datetime.now().hour


def get_current_minutes():
    return datetime.datetime.now().minute


def get_current_seconds():
    return datetime.datetime.now().second


def get_current_date_str():
    return get_before_date_str(before_days=0)


@type_check([int, NoneType])
def get_before_date_str(before_days=0):
    return timestamp_to_str(get_current_timestamp() - before_days * 24 * 3600, __DEFAULT_DATE_FORMAT)


@type_check(int, str)
def timestamp_to_str(timestamp, format_str=__DEFAULT_TIME_FORMAT):
    return time.strftime(format_str, time.localtime(timestamp))


__CALCULATE_TIME_INTERVAL_PATTERN_LIST = [
    # 2020-01-01 19:53:01
    Pair(key=r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value=__DEFAULT_TIME_FORMAT),

    # 2020-01-01 19:53
    Pair(key=r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", value=__DEFAULT_TIME_WITHOUT_SECONDS_FORMAT),

    # 2020-01-01
    Pair(key=r"\d{4}-\d{2}-\d{2}", value=__DEFAULT_DATE_FORMAT),

    # 19:53:01
    Pair(key=r"\d{2}:\d{2}:\d{2}", value=__DEFAULT_HOUR_MINUTE_SECONDS_FORMAT),

    # 19:53
    Pair(key=r"\d{2}:\d{2}", value=__DEFAULT_HOUR_MINUTE_FORMAT),
]


@type_check(str)
def calculate_time_interval(time_str):
    """
    计算当前时间距离 Time Str 相差多久
    支持时间格式参考上面
    返回秒数 如果已经过了时间 那么返回负数
    :type time_str: str
    """
    if str_is_blank(time_str):
        raise Exception("[calculate_time_interval] Cannot calculate the time interval for blank time str!")

    # 对时间类型进行依次匹配
    time_str_format = ""
    for pair_element in __CALCULATE_TIME_INTERVAL_PATTERN_LIST:  # type: Pair
        if re.match(pair_element.key, time_str):
            time_str_format = pair_element.value
            break

    # 没有找到匹配的时间格式
    if str_is_blank(time_str_format):
        raise Exception("[calculate_time_interval] Cannot calculate the time interval for '%s' !" % time_str)

    # 计算距离当前时间相差多久
    current_time = datetime.datetime.now()
    diff_time = datetime.datetime.combine(
        current_time.date(), datetime.datetime.strptime(time_str, time_str_format).time()
    ) - current_time

    # 返回秒 如果已经过了时间 则是负数
    return get_int_value(diff_time.total_seconds())


def today_is_weekday():
    """
    返回今天是不是 工作日
    """
    return not today_is_weekend()


def today_is_weekend():
    """
    返回今天是不是 周末
    """
    # 注意 Weekday 是从 0 开始 而非 1
    weekday = datetime.datetime.today().weekday()
    return weekday == 5 or weekday == 6


if __name__ == '__main__':
    print(get_current_seconds())

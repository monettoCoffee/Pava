# coding=utf-8
import os
import re
import logging.handlers
from threading import Lock

"""
日志模块
理论这个地方应该是独立的
不与任何地方依赖的
"""
def _get_logger(logger_name, log_level):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    return logger


def __str_list_compose(str_list, sep_character='', end_index=None):
    result = ""
    if end_index is None:
        end_index = len(str_list) - 1
    else:
        if 0 > end_index or end_index == len(str_list):
            raise Exception("Invalid end index %s when compose" % end_index)
    for index, str_element in enumerate(str_list):
        if index > end_index:
            return result
        if str_element is None or len(str_element) == 0:
            continue
        if result == "":
            result = str_element
            continue
        result = result + sep_character + str_element
    return result


def _get_absolute_path_parent_path(path):
    """
    获取绝对路径的的父路径
    :param path:
    :return:
    """
    # 检测 '/' 和 非法路径
    if len(path) < 2:
        raise Exception("Can't get parent path by path '%s'" % path)
    # 获取路径层级
    path_list = path.split(os.sep)
    path_list_length = len(path_list)
    # 拼接路径
    return os.sep + __str_list_compose(path_list, os.sep, path_list_length - 2)


class PLog(object):
    # 打印日志级别
    _STREAM_HANDLER_LEVEL = logging.INFO
    # 文件日志级别
    _FILE_HANDLER_LEVEL = None
    # 上一次打印的日志是否为系统日志 是的话会在业务日志前增加换行符
    _LAST_LOG_IS_SYSTEM_LOG = None

    # 保证日志顺序绝对一致且分散所使用的锁
    _FORCE_ORDER_LOG_LOCK = Lock()

    _LOGGER = _get_logger("PAVA_LOGGER", logging.DEBUG)

    _STREAM_HANDLER = logging.StreamHandler()
    # 默认打印日志级别
    _STREAM_HANDLER.setLevel(_STREAM_HANDLER_LEVEL)
    _STREAM_HANDLER.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(levelname)s] [%(module)s.%(funcName)s] [%(filename)s:%(lineno)d] - %("
        "message)s"
    ))
    _LOGGER.addHandler(_STREAM_HANDLER)

    # 如果开启文件日志 则 _FILE_HANDLER 是其对应的 Handler
    _FILE_HANDLER = None

    # 业务日志分割器
    _BUSINESS_LOGGER = _get_logger("PAVA_LOGGER_BUSINESS", logging.INFO)
    _BUSINESS_STREAM_HANDLER = logging.StreamHandler()
    _BUSINESS_STREAM_HANDLER.setLevel(logging.INFO)
    _BUSINESS_STREAM_HANDLER.setFormatter(logging.Formatter("%(message)s"))
    _BUSINESS_LOGGER.addHandler(_BUSINESS_STREAM_HANDLER)

    # 在 Web App 的情况下禁用 RootLogger 防止二次输出
    _LOGGER.propagate = False
    _BUSINESS_LOGGER.propagate = False

    @classmethod
    def add_file_handler(cls, file_path, backup_count=2):
        parent_file_path = _get_absolute_path_parent_path(file_path)
        if not os.path.exists(parent_file_path):
            os.makedirs(parent_file_path)

        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=file_path,
            when="MIDNIGHT",
            interval=1,
            backupCount=backup_count,
        )

        # 设置日志级别
        file_logger_handler_level = logging.INFO
        cls._FILE_HANDLER_LEVEL = file_logger_handler_level
        file_handler.setLevel(file_logger_handler_level)

        # 日志文件格式
        file_handler.suffix = "%Y-%m-%d.log"
        file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        file_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] [%(process)d] [%(levelname)s] [%(module)s.%(funcName)s] [%(filename)s:%(lineno)d] - %("
                "message)s "
            )
        )

        business_file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=file_path,
            when="MIDNIGHT",
            interval=1,
            backupCount=backup_count,
        )
        business_file_handler.setLevel(logging.INFO)
        business_file_handler.suffix = "%Y-%m-%d.log"
        business_file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        business_file_handler.setFormatter(logging.Formatter("%(message)s"))
        cls._BUSINESS_LOGGER.addHandler(business_file_handler)

        cls._LOGGER.addHandler(file_handler)
        cls._FILE_HANDLER = file_handler

    @classmethod
    def set_print_logger_level(cls, print_logger_level):
        cls._STREAM_HANDLER_LEVEL = print_logger_level
        cls._STREAM_HANDLER.setLevel(print_logger_level)
        cls._BUSINESS_STREAM_HANDLER.setLevel(print_logger_level)

    @classmethod
    def set_file_logger_level(cls, file_logger_level):
        if cls._FILE_HANDLER is None:
            raise Exception("[PLog] File handler is None, must do add_file_handler() before set_file_logger_level()")
        cls._FILE_HANDLER.setLevel(file_logger_level)
        cls._FILE_HANDLER_LEVEL = file_logger_level

    @classmethod
    def log_level_is_debug(cls):
        return logging.DEBUG == cls._STREAM_HANDLER_LEVEL or logging.DEBUG == int(cls._FILE_HANDLER_LEVEL)

    @classmethod
    def log_level_is_info(cls):
        return logging.INFO >= cls._STREAM_HANDLER_LEVEL or logging.INFO >= int(cls._FILE_HANDLER_LEVEL)

    @classmethod
    def gets(cls):
        """
        打印 系统级别 (或中间件级别) 日志
        :return:
        """
        # 将 业务日志 与 中间件日志 分开
        if cls._LAST_LOG_IS_SYSTEM_LOG is not None and not cls._LAST_LOG_IS_SYSTEM_LOG and cls.log_level_is_info():
            cls._BUSINESS_LOGGER.info("")
        cls._LAST_LOG_IS_SYSTEM_LOG = True
        with PLog._FORCE_ORDER_LOG_LOCK:
            return cls._LOGGER

    @classmethod
    def get(cls):
        """
        打印 业务系统类型日志
        :return:
        """
        # 将 业务日志 与 中间件日志 分开
        if cls._LAST_LOG_IS_SYSTEM_LOG is not None and cls._LAST_LOG_IS_SYSTEM_LOG and cls.log_level_is_info():
            cls._BUSINESS_LOGGER.info("")
        cls._LAST_LOG_IS_SYSTEM_LOG = False
        with PLog._FORCE_ORDER_LOG_LOCK:
            return cls._LOGGER

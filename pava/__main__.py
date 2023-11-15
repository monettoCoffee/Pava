# coding=utf-8
import sys

from pava.tool.simple_file_server import http_file_server_launch
from pava.utils.system_utils import force_exit


def _call_help():
    """
    展示帮助说明
    """
    print("This is a library with something looks like Java. The inspiration of name from Guava.")
    print("Git source: git@gitee.com:monetto/pava.git\n")
    print("Commands:")
    print("  http-file-server")
    print("    Start simple http file server in port 10001")
    force_exit()


def _start_http_file_server():
    """
    开启一个 HTTP File Server 使用端口 10001
    """
    http_file_server_launch()


# 命令对应的执行函数
COMMAND_HANDLER_DICT = {
    "http-file-server": _start_http_file_server,
}

if __name__ == '__main__':
    # 获取命令行参数
    args = sys.argv

    if len(args) == 1:
        _call_help()
    else:
        command_handler = COMMAND_HANDLER_DICT.get(args[1], None)
        if command_handler is None:
            _call_help()
        else:
            command_handler()
            force_exit()

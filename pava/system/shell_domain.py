# coding=utf-8
import subprocess
from threading import Lock

"""
获取一个终端的 session 用于执行 shell 命令
使用示例请参考 main

不要引用 object_utils 会造成循环依赖!
不要引用 object_utils 会造成循环依赖!
不要引用 object_utils 会造成循环依赖!
"""


class ShellDomain(object):

    def __init__(self, shell_interpreter="/bin/sh"):
        self._execute_lock = Lock()
        self.process = subprocess.Popen(shell_interpreter, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)

    def execute(self, cmd):
        with self._execute_lock:
            # stdout 无法判断是否执行结束 故后面加 echo EOF\n
            # 而防止部分应用本身输出 EOF 故这里改为 EOF_M
            # 执行这条命令则说明上一条命令结束了
            cmd = cmd + " ; echo EOF_M\n"
            self.process.stdin.write(cmd.encode())
            result = ""
            # 读取执行结果 直到最后读取到 EOF 进程执行结束
            while not result.endswith("EOF_M"):
                result += self.process.stdout.read(1).decode()
            # 由于后面有 EOF\n 用来标记进程是否执行结束 结果中需要去掉 EOF
            if len(result) > 2:
                result = result[:-5]
            if result.startswith("\n"):
                result = result.replace("\n", "", 1)
            if result.endswith("\n"):
                result = result[:-1]
            return result.encode("utf8")


if __name__ == '__main__':
    shell_domain = ShellDomain()
    print(shell_domain.execute("export a=5"))
    print(shell_domain.execute("echo Hey, a is ${a}"))
    # res = (shellDomain.execute("ffmpeg"))
    pass

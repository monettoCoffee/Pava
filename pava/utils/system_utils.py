# coding=utf-8

import shutil
import hashlib
from threading import Lock
import platform

from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.utils.async_utils import *


def current_user_is_root():
    """
    判断当前用户是否为 root
    """
    return os.geteuid() == 0


def current_user_is_not_root():
    """
    判断当前用户是否 不是 root
    """
    return os.geteuid() != 0


@type_check(str)
def path_is_file(file_path):
    """
    检查路径是否 是 文件
    :param file_path: 文件的路径
    :return: 路径是文件则为 True
    """
    if str_is_blank(file_path):
        raise Exception("Path is blank when check path is file")
    if not os.path.isabs(file_path):
        raise Exception("Path is not absolute path when check path is file")
    if not os.path.islink(file_path):
        if not os.path.exists(file_path):
            raise Exception("Path '%s' must exist but not when check path is file" % file_path)
    if os.path.ismount(file_path):
        return False
    return os.path.isfile(file_path) and not os.path.isdir(file_path)


@type_check(str)
def path_is_dir(file_path):
    """
    检查路径是否 是 文件夹
    :param file_path: 文件的路径
    :return: 路径是文件夹则为 True
    """
    if str_is_blank(file_path):
        raise Exception("Path is blank when check path is dir" % file_path)
    if not os.path.isabs(file_path):
        raise Exception("Path is not absolute path when check path is dir" % file_path)
    # 如果是连接文件 则 exists 为 False
    if not os.path.islink(file_path):
        if not os.path.exists(file_path):
            raise Exception("Path '%s' must exist but not when check path is dir" % file_path)
    return not os.path.isfile(file_path) and os.path.isdir(file_path)


OPERATION_JS_FILE_LOCK = Lock()


@type_check(str, [str, NoneType])
def execute_javascript(script, tmp_dir_path="/tmp"):
    """
    执行 Java Script 语句
    防止编码转义等问题 不使用 node -e "" 去执行
    :param script: 需要执行的 Java Script 语句
    :param tmp_dir_path: 临时文件路径
    :return: 执行结果
    """
    with OPERATION_JS_FILE_LOCK:
        if str_is_blank(tmp_dir_path):
            raise Exception("Must have a path to save tmp file for execute script!")
        if not os.path.exists(tmp_dir_path):
            tmp_dir_path = os.getcwd()
        if not tmp_dir_path.endswith(os.sep):
            tmp_dir_path = tmp_dir_path + os.sep
        current_time = get_int_value(time.time())
        tmp_file_path = tmp_dir_path + str(current_time) + ".js"
        with open(tmp_file_path, "a") as js_file:
            js_file.write(script)
        result = execute_command("node %s" % tmp_file_path)
        os.remove(tmp_file_path)
        return result


@type_check(str, str, str)
def write_after_remove(file_path, content, write_model="a"):
    file_path_array = file_path.split(os.sep)
    if len(file_path_array) > 1:
        parent_path = str_list_compose(file_path_array, os.sep, len(file_path_array) - 2)
        if file_path.startswith(os.sep):
            parent_path = os.sep + parent_path
        if not os.path.exists(parent_path):
            os.makedirs(parent_path)
    if os.path.exists(file_path):
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        elif os.path.isfile(file_path):
            os.remove(file_path)
    with open(file_path, write_model) as file:
        file.write(content)


def command_exist(command):
    result = execute_command(command)
    return "command not found" not in result


def command_not_exist(command):
    return not command_exist(command)


def get_file_md5(file_path):
    if not path_is_file(file_path):
        raise Exception("[get_file_md5] Path '%s' is not file" % file_path)
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


__SYSTEM_IS_LINUX = None
__SYSTEM_IS_MACOS = None
__SYSTEM_IS_WINDOWS = None


def system_is_linux():
    global __SYSTEM_IS_LINUX
    if __SYSTEM_IS_LINUX is None:
        __SYSTEM_IS_LINUX = "LINUX" in platform.system().upper()
    return __SYSTEM_IS_LINUX


def system_is_macos():
    global __SYSTEM_IS_MACOS
    if __SYSTEM_IS_MACOS is None:
        __SYSTEM_IS_MACOS = "DARWIN" in platform.system().upper()
    return __SYSTEM_IS_MACOS


def system_is_windows():
    global __SYSTEM_IS_WINDOWS
    if __SYSTEM_IS_WINDOWS is None:
        __SYSTEM_IS_WINDOWS = "WINDOWS" in platform.system().upper()
    return __SYSTEM_IS_WINDOWS


def auto_platform_run(function_name="", function_object_macos=None, function_object_linux=None,
                      function_object_windows=None, *args, **kwargs):
    if system_is_macos():
        if function_object_macos is None:
            raise Exception(
                "[auto_platform_run] Cannot find function to run in Darwin! Function name: '%s'" % str(function_name))
        else:
            return function_object_macos(*args, **kwargs)
    elif system_is_linux():
        if function_object_linux is None:
            raise Exception(
                "[auto_platform_run] Cannot find function to run in Linux! Function name: '%s'" % str(function_name))
        else:
            return function_object_linux(*args, **kwargs)
    elif system_is_windows():
        if function_object_windows is None:
            raise Exception(
                "[auto_platform_run] Cannot find function to run in Windows! Function name: '%s'" % str(function_name))
        else:
            return function_object_windows(*args, **kwargs)


@synchronized("__macos_push_notification")
@type_check(str)
def __macos_sound_notification(message_text):
    execute_command("say %s" % message_text)


def __save_sound_file_by_google_api(message_text, sound_dir, sound_file_name):
    from gtts import gTTS
    if str_is_blank(message_text):
        time_sleep(0.5)
        return

    if os.path.exists(sound_dir):
        shutil.rmtree(sound_dir)
    os.makedirs(sound_dir)
    gTTS(message_text).save(sound_dir + sound_file_name)


@synchronized("system_sound_notification")
@type_check(str, [bool, NoneType])
def system_sound_notification(message_text, by_google_api=False):
    """
    系统级别的播放提醒声音
    :param message_text: 要进行提醒的文本
    :type message_text: str
    :param by_google_api: 是否要调用谷歌网络 API
    :type by_google_api: bool
    """

    if by_google_api:
        sound_dir = "/tmp/p_panel/google_sound/"
        sound_file_name = "%s.mp3" % get_current_timestamp()
        message_text_list = message_text.split(",")
        for message_element in message_text_list:
            def _process():
                __save_sound_file_by_google_api(message_element, sound_dir, sound_file_name)

            result = execute_by_process(_process, limit_time_seconds=0.7, join=True)
            if result is True:
                command = "afplay " + sound_dir + sound_file_name
                execute_command(command)
                return
            else:
                auto_platform_run(
                    function_name="system_sound_notification",
                    function_object_macos=__macos_sound_notification,
                    message_text=message_text
                )
        return

    auto_platform_run(
        function_name="system_sound_notification",
        function_object_macos=__macos_sound_notification,
        message_text=message_text
    )


__ORIGINAL_MACOS_PUSH_NOTIFICATION_COMMAND = "osascript -e 'display notification \"%s\" with title \"%s\" %s %s'"


@synchronized("__macos_push_notification")
def __macos_push_notification(message_title, message_subtitle="", message_content="", message_sound="Submarine"):
    if str_is_blank(message_content):
        message_content = ""

    sound_args = ""
    if str_not_blank(message_sound):
        sound_args = "sound name \"%s\"" % message_sound

    subtitle_args = ""
    if str_not_blank(message_subtitle):
        subtitle_args = "subtitle \"%s\"" % message_subtitle

    execute_command(
        __ORIGINAL_MACOS_PUSH_NOTIFICATION_COMMAND % (message_content, message_title, subtitle_args, sound_args))


@type_check(str, [str, NoneType], [str, NoneType], [str, NoneType])
def system_push_notification(message_title, message_subtitle="", message_content="", message_sound="Submarine"):
    if str_is_blank(message_title):
        raise Exception("[system_push_notification] Message title can not be blank!")

    auto_platform_run(
        function_name="system_push_notification",
        function_object_macos=__macos_push_notification,
        message_title=message_title,
        message_subtitle=message_subtitle,
        message_content=message_content,
        message_sound=message_sound
    )


@type_check(str, [str, NoneType], [str, NoneType], [str, NoneType])
def push_and_sound_notification(message_title, message_subtitle="", message_content="", message_sound="Submarine"):
    if str_is_blank(message_title):
        raise Exception("[push_and_sound_notification] Message title can not be blank!")

    if str_is_blank(message_content):
        message_content = ""

    for content in message_content.split(", "):
        system_push_notification(
            message_title=message_title,
            message_subtitle=message_subtitle,
            message_content=content,
            message_sound=message_sound
        )

        system_sound_notification(content)
        time_sleep(0.5)


__ORIGINAL_MACOS_SEND_EMAIL_COMMAND = "echo \"%s\" | mail -s \"%s\" %s"


def __macos_send_email(email_subject, email_receiver, email_content=None):
    """
    macos 下使用 mail 命令发送邮件 成功率大概 70%
    """
    # 注意: 邮件不能保证 100% 收到 所以加日志方便进行追查
    send_email_command = __ORIGINAL_MACOS_SEND_EMAIL_COMMAND % (email_content, email_subject, email_receiver)
    result = execute_command(send_email_command)
    PLog().gets().info(
        "[send_email] Execute send email, subject: '%s', receiver: '%s', result: '%s'" % (
            email_subject, email_receiver, result
        )
    )
    PLog().gets().debug("[send_email] Execute command: '%s'" % send_email_command)


@type_check(str, str, [str, NoneType], [str, None], bool)
def send_email(email_subject, email_receiver, email_content=None, send_time_str="", in_advance=False):
    """
    发送邮件给某个电子邮箱
    注意: 目前测试成功率不是很高, 同时内容不要太复杂, 函数的实现非常简单 ...
    :param email_subject: 邮件的主题
    :param email_receiver: 邮件的收件人
    :param email_content: 邮件的内容
    :param send_time_str: 在指定的时间点进行发送
    :param in_advance: 是否提前发送 (邮件一般会晚30秒左右被终端收到 然后 Push 通知)
    """
    if str_is_blank(email_subject):
        raise Exception("[send_email] Cannot send email by blank subject!")

    if str_is_blank(email_receiver):
        raise Exception("[send_email] Cannot send email by blank receiver!")

    if str_is_blank(email_content):
        email_content = ""

    def delay_send_email():
        auto_platform_run(
            function_name="send_email",
            function_object_macos=__macos_send_email,
            email_subject=email_subject,
            email_receiver=email_receiver,
            email_content=email_content
        )

    if str_not_blank(send_time_str):
        time_interval_seconds = calculate_time_interval(send_time_str)
        # 如果想要提起发送的话 那就提前30秒
        if in_advance is True:
            time_interval_seconds = time_interval_seconds - 30

        # 过了时间了 第二天再发送
        if time_interval_seconds < 0:
            time_interval_seconds = 24 * 3600 + time_interval_seconds
        delay_execute("send_email", delay_send_email, time_interval_seconds)
    else:
        # 直接运行封装好的函数
        delay_send_email()


def draw_line_in_picture(original_picture_path, start_point, end_point, output_picture_path, line_color=(255, 0, 0)):
    """
    在图片上画一条线
    """
    from PIL import Image, ImageDraw
    image = Image.open(original_picture_path)
    draw = ImageDraw.Draw(image)
    start_point = tuple(start_point)
    end_point = tuple(end_point)
    draw.line([start_point, end_point], fill=line_color, width=3)
    image.save(output_picture_path)


@type_check(str, [tuple, list], [tuple, list], [tuple, list, NoneType], [tuple, list, NoneType], str, [tuple, list])
def draw_rectangle_in_picture(original_picture_path, point_a, point_b, point_c=None, point_d=None,
                              output_picture_path="", line_color=(255, 0, 0)):
    """
    在图片上画一个矩形 可以传四点 也可以只传两点
    调用示例:
    两点：draw_rectangle_in_picture("a.file", [0, 0], [1169, 498], "b.file")
    四点: draw_rectangle_in_picture("a.file", [0, 498], [1169, 498], [0, 279], [1169, 279], "b.file")

    通过构建出四个点 然后进行绘图
    point_left_up      point_right_up
                   []
    point_left_down    point_right_down
    """

    # 这里调用库 防止其他应用调用其他方法报错
    from PIL import Image, ImageDraw

    # 初始化画板
    image = Image.open(original_picture_path)
    draw = ImageDraw.Draw(image)

    # 标准化参数
    line_color = tuple(line_color)
    if point_c is None or point_d is None:
        point_c = tuple([point_b[0], point_a[1]])
        point_d = tuple([point_a[0], point_b[1]])

    # 类型必须统一 防止排序出问题
    point_a = tuple(point_a)
    point_b = tuple(point_b)
    point_c = tuple(point_c)
    point_d = tuple(point_d)

    # 如果没有输出路径 那么覆盖输入路径
    if str_is_blank(output_picture_path):
        output_picture_path = original_picture_path

    # 排序一遍 方便取四个点
    sort_list = [point_a, point_b, point_c, point_d]
    sort_list.sort()

    # 赋值四个点 方位看一开始的注释
    point_left_down = tuple(sort_list[0])
    point_left_up = tuple(sort_list[1])
    point_right_down = tuple(sort_list[2])
    point_right_up = tuple(sort_list[3])

    # 画出来
    draw.line([point_left_up, point_left_down], fill=line_color, width=1)
    draw.line([point_left_up, point_right_up], fill=line_color, width=1)
    draw.line([point_left_down, point_right_down], fill=line_color, width=1)
    draw.line([point_right_up, point_right_down], fill=line_color, width=1)
    image.save(output_picture_path)


def convert_images_to_pdf(image_path_list, output_path, keep_ratio=True):
    """
    将图片转换为 PDF
    :param image_path_list: 需要转 PDF 的图片
    :type image_path_list: list
    :param output_path: 输出路径
    :type output_path: str
    :param keep_ratio: 是否保持原图纵横比
    :type keep_ratio: bool
    """

    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    canvas_object = canvas.Canvas(output_path, pagesize=letter)
    for image_path in image_path_list:
        canvas_object.drawImage(image_path, 0, 0, letter[0], letter[1], preserveAspectRatio=keep_ratio)
        canvas_object.showPage()
    canvas_object.save()


def force_exit():
    """
    强行退出进程
    """
    exec ("os._exit(0)")


if __name__ == '__main__':
    system_sound_notification("g, o, v, e, r, n", True)
    os._exit(0)

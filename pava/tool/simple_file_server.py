# coding=utf-8
import os
import waitress
from flask import Flask, Response, send_file, abort
from pava.entity.file_domain import FileDomain, type_check
from pava.utils.object_utils import str_is_blank, trans_object_properties_to_utf8, list_is_empty, list_not_empty, \
    replace_to_str_end
from pava.utils.system_utils import force_exit


def get_file_server_root_path():
    """
    :return: 文件服务器的根路径
    :rtype: str
    """
    return "/Users/monetto"


# 创建 Flask App
file_server_app = Flask(__name__)

# 全局根路径
ROOT_PATH = get_file_server_root_path()

# 文件服务地址
SERVER_HOST = "0.0.0.0"

# 架设端口
SERVER_PORT = 10001

# 服务端线程数 默认 8 个
SERVER_THREAD_NUMBER = 8


@file_server_app.route('/file_server_stop')
def _file_server_stop():
    """
    终止文件服务器
    """
    force_exit()


@file_server_app.route('/<path:request_string>')
def handle_request(request_string):
    """
    请求指定路径
    :param request_string: 请求的子路径
    :type request_string: str
    """
    # 判断参数是否为 None
    if request_string is None:
        return _root_index()

    # Flask 默认在 Py2 传递的参数是 Unicode 转化为 Str
    request_string = trans_object_properties_to_utf8(request_string)
    if str_is_blank(request_string):
        return _root_index()

    # 创建一个文件的抽象对象
    file_domain = FileDomain(os.path.join(ROOT_PATH, request_string))

    # 判断文件是否存在
    if file_domain.get_exist_status() is False:
        return abort(404)

    # 如果文件是目录的话 那么返回这个目录下所有文件组成的 HTML 结构页面
    if file_domain.path_is_dir():
        return _wrapper_path_domain_to_html_response(file_domain)

    # 如果请求的是一个文件 那么直接通过 HTTP 返回文件
    if file_domain.path_is_file():
        return send_file(file_domain.file_absolute_path)

    return "Unknown Operation ..."


@file_server_app.route('/')
def _root_index():
    """
    请求访问文件服务根路径
    """
    return _wrapper_path_domain_to_html_response(FileDomain(get_file_server_root_path()))


@type_check(FileDomain)
def _wrapper_path_domain_to_html_response(file_domain):
    """
    将路径包装成 HTML 页面结构返回
    :param file_domain: 路径的文件对象
    :type file_domain: FileDomain
    :rtype: Response
    """
    if file_domain.path_is_dir() is False:
        return abort(404)

    # 要拼接到 HTML 选项里面的文本
    html_ul_list = ""

    # 如果当前路径不是根路径 那么就需要有一个返回上一级路径的选项
    if file_domain.file_absolute_path != get_file_server_root_path():
        # 首先获取父路径
        parent_path = replace_to_str_end(file_domain.get_parent_file_domain().file_absolute_path,
                                         get_file_server_root_path())
        # 处理根路径情况
        if parent_path == "":
            parent_path = os.sep

        # 将返回上一级的选项拼接到 HTML 结构中
        html_ul_list = html_ul_list + HTML_PATH_OPTION_TEMPLATE % (parent_path, "..")

    # 获取这个路径下的所有子文件
    file_domain_list = file_domain.get_children_file_domain()

    # 空目录 直接返回
    if list_is_empty(file_domain_list):
        return Response(HTML_TEMPLATE % (file_domain.file_absolute_path, html_ul_list), mimetype='text/html')

    # 如果子文件不为空 那么开始拼接 HTML 选项
    for child_file_domain in file_domain_list:  # type: FileDomain
        # 如果文件是目录类型的 那么名字后面加上路径分隔符 以区分是 文件 还是 目录
        file_name = child_file_domain.file_name
        if child_file_domain.path_is_dir():
            file_name = file_name + os.sep

        # 拼接 HTML 选项文本
        html_ul_list = html_ul_list + HTML_PATH_OPTION_TEMPLATE % (
            replace_to_str_end(child_file_domain.file_absolute_path, get_file_server_root_path()),
            file_name)

    return Response(HTML_TEMPLATE % (file_domain.file_absolute_path, html_ul_list), mimetype='text/html')


# 跳转文件的 HTML 结构
HTML_PATH_OPTION_TEMPLATE = "<li><a href=\"%s\">%s</a>"

# 返回的 HTML 拼接结构模板
HTML_TEMPLATE = "<html>" \
                "<title>File Explorer</title>" \
                "<h1>Path: %s</h1>" \
                "<hr>" \
                "<ul>" \
                "<li><a href=\"/file_server_stop\">Stop Server</a>" \
                "</ul>" \
                "<hr>" \
                "<ul>" \
                "<li><a href=\"/\">Back Root</a>" \
                "</ul>" \
                "<hr>" \
                "<ul>" \
                "%s" \
                "</ul>" \
                "<html>"


def http_file_server_launch():
    waitress.serve(
        file_server_app,
        host=SERVER_HOST,
        port=SERVER_PORT,
        threads=SERVER_THREAD_NUMBER
    )


if __name__ == '__main__':
    http_file_server_launch()

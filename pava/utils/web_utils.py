# coding=utf-8
import random
import socket

from pava.entity.http_request_response import HTTPRequestResponse
from pava.utils.object_utils import *
from pava.utils import DEFAULT_HTTP_REQUEST_HEADERS


@type_check(object, str, str, bool)
def fetch_get_param_from_flask_request(flask_request, param_name, character_encode='utf-8', do_encode=True):
    """
    从 flask request 中 拿到 Get 参数
    :param flask_request: 应为 flask.request
    :param param_name: 参数名
    :param character_encode: 字符编码
    :param do_encode: 是否进行转码
    :return: 参数值 如不存在 则为 None
    """
    param_value = flask_request.args.get(param_name, None)
    if str_not_blank(character_encode) and do_encode:
        return trans_object_properties_to_utf8(param_value)
    return param_value


@type_check(object, str, str, bool)
def fetch_form_param_from_flask_request(flask_request, param_name, character_encode='utf-8', do_encode=True):
    """
    从 flask request 中 拿到 Form 参数
    :param flask_request: 应为 flask.request
    :param param_name: 参数名
    :param character_encode: 字符编码
    :param do_encode: 是否进行转码
    :return: 参数值 如不存在 则为 None
    """
    param_value = flask_request.form.get(param_name, None)
    if str_not_blank(character_encode) and do_encode:
        return trans_object_properties_to_utf8(param_value)
    return param_value


@type_check(object, str, str, bool)
def fetch_param_from_flask_request(flask_request, param_name, character_encode='utf-8', do_encode=True):
    """
    不区分 Get Post 方法 从 flask request 中 拿到 Get 或 Form 参数
    :param flask_request: 应为 flask.request
    :param param_name: 参数名
    :param character_encode: 字符编码
    :param do_encode: 是否进行转码
    :return: 参数值 如不存在 则为 None
    """
    get_param = fetch_get_param_from_flask_request(flask_request, param_name, character_encode, do_encode)
    if get_param is not None:
        return get_param
    return fetch_form_param_from_flask_request(flask_request, param_name, character_encode, do_encode)


@type_check(str, [dict, NoneType], [dict, NoneType], [dict, NoneType], [int, float, NoneType], [int, NoneType], str,
            bool)
def http_request(url, headers=DEFAULT_HTTP_REQUEST_HEADERS, append_headers=None,
                 params=None, timeout=10, retry_times=2, request_method="GET", random_sleep=False):
    """
    发起一个 HTTP 请求 通过 Requests 库
    :param timeout:
    :param url: 请求的 URL
    :param headers: HTTP Headers
    :param append_headers: 额外的 Headers 参数 会添加进 Headers
    :param params: Request params
    :param retry_times: 重试次数
    :param request_method: Get or Post
    :param random_sleep: 是否随机休眠 0 - 5 秒
    :return: 封装的 RequestResponse 对象
    :rtype: HTTPRequestResponse
    """
    # 判断是否需要随机休眠
    if random_sleep:
        time_sleep(random.randint(0, 5))
    # 判断要请求的方法 是 GET 还是 POST
    request_method = request_method.upper()
    get_method = request_method == "GET"
    post_method = request_method == "POST"
    if not get_method and not post_method:
        raise Exception("[HttpRequest] Unsupported request method, try POST or GET, not %s" % request_method)

    # 尝试使用 Requests 库进行请求
    try:
        import requests
    except:
        raise Exception("[HttpRequest] Can not do http request, should install requests")

    headers = dict() if headers is None else headers
    retry_times = get_int_value(retry_times)
    if retry_times < 0:
        raise Exception("[HttpRequest] Retry times must >= 0")
    # 如果存在额外的请求头 则将额外的请求头加入请求头
    if append_headers is not None:
        for key, value in append_headers.items():
            headers[key] = value
    do_request_times = 0
    while not do_request_times > retry_times:
        try:
            response_object = None
            do_request_times += 1
            if get_method:
                response_object = requests.get(url, headers=headers, params=params, timeout=timeout)
            if post_method:
                response_object = requests.post(url, headers=headers, data=params, timeout=timeout)
            return HTTPRequestResponse(url, response_object.status_code, response_object.content)
        except Exception as e:
            if do_request_times <= retry_times:
                continue
            raise Exception("[HttpRequest] occurred exception", e)


_HOST_IP = None


def get_local_host_ip():
    """
    查询本机ip地址
    :return: IP
    """
    global _HOST_IP
    if _HOST_IP is not None:
        return _HOST_IP
    connection = None
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        connection.connect(("8.8.8.8", 80))
        _HOST_IP = connection.getsockname()[0]
    finally:
        if connection is not None:
            connection.close()
    return _HOST_IP

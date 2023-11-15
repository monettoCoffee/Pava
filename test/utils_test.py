# coding=utf-8
from pava.utils.system_utils import *

from pava.utils.async_utils import *
import time

if __name__ == '__main__':
    execute_command("cp /Users/bytedance/Desktop/img.png /Users/bytedance/Desktop/img2.png")
    draw_rectangle_in_picture("/Users/bytedance/Desktop/img2.png", [378, 156], [473, 172], output_picture_path="/Users/bytedance/Desktop/img2.png")

    force_exit()

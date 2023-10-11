#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : running_time.py
# @Time    : 2023/8/29 13:34
# @motto   :  rain cats and dogs

import time

import functools


def calculate_execution_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"函数 {func.__name__} 的执行时间为 {execution_time} 秒")
        return result

    return wrapper

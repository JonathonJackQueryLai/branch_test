#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : parallel.py
# @Time    : 2023/7/11 17:14
# @motto   :  rain cats and dogs
import gc
import itertools
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


def parallel_process(processing_func, post_process_func, args: list, engine, return_data=False):
    processed_list = []
    with ProcessPoolExecutor(max_workers=4) as executor:
        res_gen = [executor.submit(processing_func, *arg) if isinstance(arg, tuple) else
                   executor.submit(processing_func, arg) for arg in args]
        with ThreadPoolExecutor() as thread_executor:
            for future in as_completed(res_gen):
                processed_list.append(future.result())
                thread_executor.map(post_process_func, itertools.cycle([engine]), processed_list)
        if return_data:
            return processed_list
        else:
            del processed_list
            gc.collect()

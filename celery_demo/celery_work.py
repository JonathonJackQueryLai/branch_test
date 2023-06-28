#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : celery_work.py
# @Time    : 2023/6/28 16:04
# @motto   :  rain cats and dogs
from celery import Celery

app = Celery('tasks', broker='redis://44.228.81.225:23079/10', backend='redis://44.228.81.225:23079/12')


@app.task
def func(x, y):
    return x + y

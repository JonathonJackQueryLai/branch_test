#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : celery_product.py
# @Time    : 2023/6/28 16:27
# @motto   :  rain cats and dogs
from celery_demo.celery_work import func

s = func.delay(12, 33)
print(s.get(timeout=10))

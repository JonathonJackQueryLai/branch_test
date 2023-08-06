#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/8/5 22:22
# @Author  : Jonathon
# @File    : openai_service.py
# @Software: PyCharm
# @ Motto : 客又至，当如何
import requests

# 设置代理服务器的地址和端口号
proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer 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'
}

try:
    context = '你好'
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": context}]
    }
    response = requests.post('https://api.openai-proxy.com/v1/chat/completions', headers=headers, proxies=proxies,data= data)
    response.raise_for_status()

    # 处理响应结果
    print("Response Code:", response.status_code)
    print("Response Body:", response.text)

except requests.exceptions.RequestException as e:
    print("Error:", e)

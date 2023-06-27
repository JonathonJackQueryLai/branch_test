#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : demo.py
# @Time    : 2023/6/27 9:10
# @motto   :  rain cats and dogs
from flask import Flask  # 用于创建 Flask 实例
from flask_restful import Api, Resource, reqparse, inputs

# 用于绑定 Flask 对象, 继承父类, 实例化对象, 判断 url

web = Flask(__name__)
# 使用 Api 来绑定 web
api = Api(web)


class IndexView(Resource):
    def get(self):
        p = reqparse.RequestParser()
        p.add_argument('name', type=str, help='名字错误', required=True)
        # required 参数的作用是只有修饰的变量接收到值时, 才能执行返回语句
        p.add_argument('password', type=str, help='密码错误', required=True)
        args = p.parse_args()
        print(args)
        return {'info': '登录成功'}

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('username', type=str, help='用户名错误', required=True)
        parse.add_argument('password', type=str, help='密码错误', required=True)
        parse.add_argument('age', type=int, help='年龄错误', required=True)
        parse.add_argument('gender', type=str, help='性别错误', choices=['man', 'woman'])
        parse.add_argument('url', type=inputs.url, help='url 错误')  # inputs.url 方法自动判断是否为 url
        args = parse.parse_args()  # 获取 parse 对象所有参数的键值
        print(args)
        return {'info': '登录成功'}


# 给视图函数类增添 url, 和起别名
api.add_resource(IndexView, '/index')


if __name__ == '__main__':
    web.run(debug=True, port=23379)  # 运行 Flask 实例


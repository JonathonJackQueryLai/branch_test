#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/8/13 11:28
# @Author  : Jonathon
# @File    : play_voice.py
# @Software: PyCharm
# @ Motto : 客又至，当如何
from pydub import AudioSegment
from pydub.playback import play

def play_string_sound(string):
    # 创建一个空的音频段对象
    sound = AudioSegment.silent(duration=1000)

    # 将字符串转换为音频段
    for char in string:
        char_sound = AudioSegment.from_file(f"./sounds/{char}.mp3")
        sound += char_sound

    # 播放音频段
    play(sound)

# 要播放的字符串
my_string = "Hello, world!"

# 播放字符串的声音
play_string_sound(my_string)

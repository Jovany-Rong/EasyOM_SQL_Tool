#!/usr/local/bin python
#coding: utf-8

import os

path = r'D:\usr\SVN\全库质检跟踪\2019年第1轮全库质检'

for root, dirs, files in os.walk(path):
    del dirs
    if not (root.endswith("/") or root.endswith("\\")):
        root = root + "/"
    for file in files:
        if "PS" in file and ".doc" in file:
            print((root + file).replace("\\", "/"))


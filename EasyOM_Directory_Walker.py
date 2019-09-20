#!/usr/local/bin python
#coding: utf-8

"""
Author: Chenfei Jovany Rong
"""

import os
import time

def findSqlFiles(dirPath):
    pathList = []
    for root, dirs, files in os.walk(dirPath):
        del dirs
        if not (root.endswith("/") or root.endswith("\\")):
            root = root + "/"
        for file in files:
            if file.endswith(".sql"):
                pathList.append((root + file).replace("\\", "/"))
    
    return pathList

def main():
    print("*********************************")
    print("*****EasyOM目录遍历工具 V1.0*****")
    print("*********************************")

    print("\n作者：戎 晨飞(Chenfei Jovany Rong)\n")
    print("Copyright 2019 Summer Moon Talk\nAll rights reserved.\n")

    print("""
【使用须知】

    - 本产品属于EasyOM系列运维工具，EasyOM团队保留一切权利

    - 请根据提示输入相关信息以执行目录遍历功能
    
    - 本产品遵循GNU GPL V3.0开源协议，如您同意方可继续使用，否则请退出本程序

"""    )

    os.system("pause")

    dirPath = input("请输入需要进行遍历的目录：")

    with open("EasyOM_Directory_Walker.log", "a+", encoding="utf-8") as fa:
        fa.write("###键入目录 %s ### %s\n" % (dirPath, time.strftime('%Y-%m-%d %H:%M:%S')))

    pathList = findSqlFiles(dirPath)

    text = "###输出SQL文件：\n"

    for path in pathList:
        text = text + "%s\n" % path
    
    text = text + "### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S'))

    print(text)

    with open("EasyOM_Directory_Walker.log", "a+", encoding="utf-8") as fa:
        fa.write(text)
    
    os.system("pause")

if __name__ == "__main__":
    main()
#!/usr/local/bin python
#coding: utf-8

"""
Author: Chenfei Jovany Rong
"""

import cx_Oracle as ora
import os
import time

#tns = "train/train@orcl"
#sqls = ""

def dataMonitor(text):
    global db
    if text.strip() == "":
        sql = "select t.table_name from user_tables t"
        cr = db.cursor()
        try:
            cr.execute(sql)
            rows = cr.fetchall()
            text = ""
            for row in rows:
                text = text + row[0] + ","
            text = text.rstrip(",")
            
        except Exception as E:
            print(E)
        cr.close()
    
    tableList = text.split(",")
    sqlTemplate = "select count(1) from <table>"
    resultDict = dict()
    for table in tableList:
        dd = dict()
        sql = sqlTemplate.replace("<table>", table)
        cr = db.cursor()
        try:
            cr.execute(sql)
            row = cr.fetchone()
            resultDict[table] = str(row[0])

        except:
            resultDict[table] = "N/A"
        cr.close()
    return [resultDict, text]

def noComment(text):
    flag = True
    start = 0
    target = text
    while flag:
        comBeg = target.find("/*", start)
        if comBeg != -1:
            comEnd = target.find("*/", comBeg)
            if comEnd != -1:
                target = target[:comBeg] + target[comEnd + 2:]
            else:
                flag = False

        else:
            flag = False
    
    rowList = target.split("\n")

    target = ""

    for row in rowList:
        if not row.startswith("--"):
            target = target + row + "\n"
    
    return target

def exSqlOneFile():
    global path, db
    begTime = time.time()
    filePath = path
    with open(filePath, "r", encoding="utf-8-sig") as f:
        sqls = f.read()
    sqls = noComment(sqls)
    sqls = sqls.rstrip().rstrip(";")
    cr = db.cursor()
    sqlList = sqls.split(";")
    length = len(sqlList)
    print("\n共包含%d个SQL语句。\n" % length)
    sqlNo = 0
    wrongNo = 0
    errInfo = ""
    for sql in sqlList:
        sqlNo += 1
        print("\t执行第%d条SQL：\n%s" % (sqlNo, sql))
        try:
            cr.execute(sql)
            print("\n\t\t完成\n")
        except Exception as E:
            errInfo = E
            print("\n\t\t失败，系统报错：%s\n" % errInfo)
            wrongNo = sqlNo
            break
    cr.close()

    endTime = time.time()

    exTime = endTime - begTime

    if wrongNo == 0:
        print("\n所有SQL执行完成，耗时%.2f秒。" % exTime)
        commit = input("\n【是否需要系统进行提交？】(y/n)")
        if commit == "y" or commit == "Y":
            db.commit()
            print("\n提交成功。\n")
    else:
        print("\n\t第%d条SQL执行失败，报错信息为：%s。" % (wrongNo, errInfo))
        print("\nSQL执行已中止，耗时%.2f秒。\n" % exTime)    

def exSqlFiles():
    global path, db
    dirPath = path

    exTime = 0.00

    if not (dirPath.endswith("/") or dirPath.endswith("\\")):
        dirPath = dirPath + "/"
    
    if (not os.path.exists(dirPath)):
        print("\n输入的目录有错误！\n")
    else:
        fileList = findSqlFiles(dirPath)
        print("\n所选目录共包含如下SQL文件：\n")
        for file in fileList:
            print("\t%s" % file)
        
        exe = input("\n【将按序执行所有以上SQL文件，是否继续？】(y/n)")

        if (exe == "y") or (exe == "Y"):
            fileNo = 0
            exeFlag = True
            errInfo = ""
            wrongNo = 0

            for filePath in fileList:
                if exeFlag == False:
                    break
                begTime = time.time()
                fileNo += 1
                print("\n执行第%d个SQL文件：%s\n" % (fileNo, filePath))
                with open(filePath, "r", encoding="utf-8-sig") as f:
                    sqls = f.read()
                sqls = noComment(sqls)
                sqls = sqls.rstrip().rstrip(";")
                cr = db.cursor()
                sqlList = sqls.split(";")
                length = len(sqlList)
                print("\n\t共包含%d个SQL语句。\n" % length)
                sqlNo = 0
                for sql in sqlList:
                    sqlNo += 1
                    print("\t\t执行第%d条SQL：\n%s" % (sqlNo, sql))
                    try:
                        cr.execute(sql)
                        print("\n\t\t\t完成\n")
                    except Exception as E:
                        errInfo = E
                        print("\n\t\t\t失败，系统报错：%s\n" % errInfo)
                        wrongNo = sqlNo
                        exeFlag = False
                        break
                cr.close()
                endTime = time.time()
                exTime4One = endTime - begTime
                exTime += exTime4One
                if wrongNo == 0:
                    print("\n\tSQL文件【%s】执行完成，耗时%.2f秒。\n" % (filePath, exTime4One))
                else:
                    print("\n\t\tSQL文件【%s】第%d条SQL执行失败，报错信息为：%s。" % (filePath, wrongNo, errInfo))
                    print("\nSQL执行已中止，耗时%.2f秒。\n" % exTime4One)
            
            if wrongNo == 0:
                print("\n所有SQL执行完成，耗时%.2f秒。" % exTime)
                commit = input("\n【是否需要系统进行提交？】(y/n)")
                if commit == "y" or commit == "Y":
                    db.commit()
                    print("\n提交成功。\n")


def findSqlFiles(dirPath):
    pathList = []
    for root, dirs, files in os.walk(dirPath):
        if not (root.endswith("/") or root.endswith("\\")):
            root = root + "/"
        for file in files:
            if file.endswith(".sql"):
                #print((root + file).replace("\\", "/"))
                pathList.append((root + file).replace("\\", "/"))
    
    return pathList

def main():
    global path, db

    print("*******************************************")
    print("*****EasyOM数据库脚本批量执行工具 V1.0*****")
    print("*******************************************")

    print("\n作者：戎 晨飞(Chenfei Jovany Rong)\n")
    print("Copyright 2019 Summer Moon Talk\nAll rights reserved.\n")

    print("""
【使用须知】

    - 本产品属于EasyOM系列运维工具，EasyOM团队保留一切权利

    - 本产品适用于Oracle 11gR2以上版本数据库

    - 请根据提示输入相关信息以执行SQL脚本

    - 本产品不会记录用户输入的数据库用户名和密码，请放心使用
    
    - 本产品遵循GNU GPL V3.0开源协议，如您同意方可继续使用，否则请退出本程序

"""    )

    os.system("pause")

    tns = input("\n【请输入数据库连接】(username/password@sid)：")

    print("\nTNS: %s\n" % tns)

    try:
        db = ora.connect(tns)
        dbFlag = True
        print("连接成功！\n")
    except Exception as E:
        print(E)
        dbFlag = False
        print("连接失败！\n")

    if dbFlag:
        dataMoni = input("【是否需要执行数据监控？】(y/n)：")
        if dataMoni == "y" or dataMoni == "Y":
            tables = input("\n【请输入需要监控的数据库表名称，多个表以英文逗号(,)分隔】：")
            dataBefore = dataMonitor(tables)[0]
        path = ""
        path = input("\n【请输入SQL文件路径】(包含SQL文件的目录或具体SQL文件)：")
        if path.endswith(".sql"):
            exSqlOneFile()
        else:
            exSqlFiles()
        if dataMoni == "y" or dataMoni == "Y": 
            dataAfter = dataMonitor(tables)

            tableList = dataAfter[1].split(",")

            print("\n数据监控结果如下：\n")
            for table in tableList:
                print("\t表%s执行前数据量为%s，执行后数据量为%s。" % (table, dataBefore[table], dataAfter[0][table]))
            print("\n")

        db.close()

    os.system("pause")

if __name__ == "__main__":
    main()

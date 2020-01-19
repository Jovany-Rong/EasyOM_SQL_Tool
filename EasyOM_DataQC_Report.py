#!/usr/local/bin python
#coding: utf-8

"""
Author: Chenfei Jovany Rong
"""

import cx_Oracle as ora
import os
import time
import threading
import report

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

exList = []

tns = ""

enCode = "utf-8-sig"

class sqlThread(threading.Thread):
    def __init__(self, threadID, pathList, tns, enCode):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.isConn = False
        self.pathList = pathList
        self.tns = tns
        self.enCode = enCode

    def run(self):
        print ("\n【线程%d %s】线程开始" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S')))
        self.dbConn(self.tns)
        if self.isConn:
            for path in self.pathList:
                if (path not in exList):
                    exList.append(path)
                
                    print("\n【线程%d %s】开始执行SQL文件'%s'" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path))
                    
                    logText = "线程%d : %s\n" % (self.threadID, path)
                    
                    with open("executing.log", "a+", encoding="utf-8") as f:
                        f.write(logText)
                
                    self.executeSQL(path)

                    print("\n【线程%d %s】完成执行SQL文件'%s'" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path))

                    with open("executing.log", "w+", encoding="utf-8") as f:
                        text = f.read()
                        text = text.replace(logText, "")
                        f.write(text)

            self.cr.close()
            self.db.close()
                

            

        print ("\n【线程%d %s】线程结束" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S')))

    def dbConn(self, tns):
        print("\n【线程%d %s】建立数据库连接" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            self.db = ora.connect(tns)
            self.cr = self.db.cursor()
            self.isConn = True
            print("\n【线程%d %s】数据库连接成功" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            self.isConn = False
            logText = "\n【线程%d %s】数据库连接失败：%s" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), e)
            print(logText)
            with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
            with open("error.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))

    def executeSQL(self, path):
        begTime = time.time()
        print("\n【线程%d %s】开始分析SQL文件'%s'" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path))
        with open(path, "r", encoding=self.enCode) as f:
            sqls = f.read()
        sqls = noComment(sqls)

        sqls = sqls.replace(";'", "<fenhaoyinhao>")

        sqls = sqls.rstrip().rstrip(";")
        sqlList = sqls.split(";")

        sqlList1 = []
        for sql in sqlList:
            sql = sql.replace("<fenhaoyinhao>", ";'")
            sqlList1.append(sql)

        del sqlList
        sqlList = sqlList1[:]
        del sqlList1

        length = len(sqlList)
        print("\n【线程%d %s】SQL文件'%s'共包含%d个SQL语句" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, length))
        sqlNo = 0
        wrongNo = 0
        errInfo = ""
        
        for sql in sqlList:
            sqlNo += 1
            #print("\n【线程%d %s】执行SQL文件'%s'中第%d条SQL：\n%s" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, sqlNo, sql))
            try:
                self.cr.execute(sql)
                res = self.cr.fetchall()
                rpt = report.Report()
                rpt.appendH4("数据检查结果")
                tb = report.Table()
                tb.addHead("大类", "小类", "规则", "等级", "数据量")
                cont = ""
                c = "大类\t\t\t\t小类\t\t\t\t规则\t\t\t\t等级\t\t\t\t数据量"
                cont = cont + c + "\n"
                #print(c)
                for row in res:
                    li = list()
                    i = 0
                    tmp = ""
                    for fld in row:
                        tmp = tmp + str(fld) + "\t\t\t\t"
                        li.append(str(fld))
                        i += 1
                    #print(tmp)
                    cont = cont + tmp + "\n"
                    tb.addRow(li[0], li[1], li[2], li[3], li[4])
                cont = cont + "\n\nSQL:\n" + sql
                print(cont)
                strTemp = tb.makeTable("数据检查结果")
                rpt.appendHTML(strTemp)
                rpt.appendH4("检查SQL")
                rpt.appendParagraph(sql)
                rpt.makeRpt()
                
            except Exception as E:
                errInfo = E
                print("\n【线程%d %s】SQL执行失败：%s" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), errInfo))
                wrongNo = sqlNo
                break

        endTime = time.time()

        exTime = endTime - begTime

        if (wrongNo == 0):
            logText = "\n【线程%d %s】SQL文件'%s'所有SQL执行成功，耗时%.2f秒" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, exTime)
            print(logText)
            with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
        
        else:
            logText = "\n【线程%d %s】SQL文件'%s'第%d条SQL执行失败，报错信息为：%s" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, wrongNo, errInfo)
            print(logText)
            with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
            with open("error.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
            print("\n【线程%d %s】SQL文件'%s'执行已中止，耗时%.2f秒" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, exTime))  

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

def findSqlFiles(dirPath):
    pathList = []
    for root, dirs, files in os.walk(dirPath):
        del dirs
        if not (root.endswith("/") or root.endswith("\\")):
            root = root + "/"
        for file in files:
            if file.endswith(".sql"):
                #print((root + file).replace("\\", "/"))
                pathList.append((root + file).replace("\\", "/"))
    
    return pathList

def startThreads(threadNo, pathList, tns, enCode):
    begin = time.time()
    thread = []
    for ct in range(threadNo):
        thread.append(sqlThread(ct + 1, pathList, tns, enCode))
        thread[ct].start()
        
    for ct in range(threadNo):
        thread[ct].join()

    end = time.time()

    spend = end - begin
    print("\n总耗时%.2f秒" % spend)

def readConf():
    text = ""
    if not os.path.exists("EasyOM_DataQC_Report.conf"):
        text = ""
    else:
        with open("EasyOM_DataQC_Report.conf", "r", encoding="utf-8") as fr:
            text = fr.read()

        print("\n监测到配置文件，正在读取配置文件")
    
    text = noComment(text)

    elemList = text.split("****")

    dd = dict()

    rDd = dict()

    for elem in elemList:
        if "=" in elem:
            elem = elem.strip()
            temp = elem.split("=")
            dd[temp[0].strip()] = temp[1].strip()

    if "tns" in dd:
        print("\n读取到数据库连接配置：%s" % dd["tns"])
        rDd["tns"] = dd["tns"]
    else:
        print("\n未读取到数据库连接配置")
        tns = input("\n【请输入数据库连接】(username/password@sid)：")
        rDd["tns"] = tns

    if "sqlList" in dd:
        print("\n读取到SQL文件组配置：%s" % dd["sqlList"])
        
        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###通过配置文件获取SQL目录### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))

        tempList = dd["sqlList"].split(";")
        pathLists = []
        for temp in tempList:
            temp = temp.strip()
            pathList = []
            if len(temp) > 0:
                tmpList = temp.split(",")
                for tmp in tmpList:
                    tmp = tmp.strip()
                    if len(tmp) > 0:
                        pathList.append(tmp)
            pathLists.append(pathList)


        rDd["sqlList"] = pathLists
    else:
        print("\n未读取到SQL文件组配置")

        dirText = ""
        dirText = input("\n【请输入SQL目录】(包含SQL文件的目录，多个目录用英文逗号【,】隔开)：")

        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###键入SQL目录 %s ### %s\n" % (dirText, time.strftime('%Y-%m-%d %H:%M:%S')))

        dirs = dirText.split(",")

        pathLists = []

        for path in dirs:
            path = path.strip()
            pathList = findSqlFiles(path)
            pathLists.append(pathList)

        rDd["sqlList"] = pathLists

    if "enCode" in dd:
        print("\n读取到文件编码配置：%s" % dd["enCode"])
        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###通过配置文件获取文件编码方式 %s ### %s\n" % (dd["enCode"], time.strftime('%Y-%m-%d %H:%M:%S')))
        rDd["enCode"] = dd["enCode"]
    else:
        print("\n未读取到文件编码配置")
        enCode = input("\n【请输入文件编码方式】(默认gb2312，可选utf-8、utf-8-sig、ANSI等)：")

        if enCode == "":
            enCode = "gb2312"

        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###键入文件编码方式 %s ### %s\n" % (enCode, time.strftime('%Y-%m-%d %H:%M:%S')))

        rDd["enCode"] = enCode
    
    if "threadNo" in dd:
        print("\n读取到启动线程数配置：%s" % dd["threadNo"])
        rDd["threadNo"] = dd["threadNo"]
    else:
        print("\n未读取到启动线程数配置")
        threadNoStr = input("\n【请输入要启动的SQL线程数】(1-100)：")
        
        rDd["threadNo"] = threadNoStr

    return rDd

def main():
    print("***********************************************")
    print("*****EasyOM数据质检及报告输出工具 V1.0*****")
    print("***********************************************")

    print("\n作者：戎 晨飞(Chenfei Jovany Rong)\n")
    print("Copyright 2020 Summer Moon Talk\nAll rights reserved.\n")

    print("""
【使用须知】

    - 本产品属于EasyOM系列运维工具，EasyOM团队保留一切权利

    - 本产品适用于Oracle 11gR2以上版本数据库客户端

    - 请根据提示输入相关信息以执行SQL文件组或存入专用配置文件在本程序目录
    
    - 本产品遵循GNU GPL V3.0开源协议，如您同意方可继续使用，否则请退出本程序

"""    )

    os.system("pause")

    with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("打开EasyOM DataQC Report %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")

    cfg = readConf()

    tns = cfg["tns"]

    dbFlag = False

    try:
        db = ora.connect(tns)
        dbFlag = True
        print("测试连接成功！\n")
        db.close()
        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###连接数据库 %s 成功### %s\n" % (tns, time.strftime('%Y-%m-%d %H:%M:%S')))
            
    except Exception as E:
        print(E)
        dbFlag = False
        print("连接失败！\n")
        with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            fa.write("###连接数据库 %s 失败：%s### %s\n" % (tns, E, time.strftime('%Y-%m-%d %H:%M:%S')))

    if dbFlag == True:
        pathLists = cfg["sqlList"]
        #dirText = ""
        #dirText = input("\n【请输入SQL目录】(包含SQL文件的目录，多个目录用英文逗号【,】隔开)：")

        #with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            #fa.write("###键入SQL目录 %s ### %s\n" % (dirText, time.strftime('%Y-%m-%d %H:%M:%S')))

        #dirs = dirText.split(",")

        #pathLists = []

        #for path in dirs:
            #path = path.strip()
            #pathList = findSqlFiles(path)
            #pathLists.append(pathList)

        enCode = cfg["enCode"]

        #if enCode == "":
            #enCode = "gb2312"

        #with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
            #fa.write("###键入文件编码方式 %s ### %s\n" % (enCode, time.strftime('%Y-%m-%d %H:%M:%S')))
    
        threadNoStr = cfg["threadNo"]

        threadNo = 0

        exeFlag = False

        try:
            threadNo = int(threadNoStr)
            if threadNo >= 1 and threadNo <= 100:
                exeFlag = True
                with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                    fa.write("###键入启动线程数 %d 成功### %s\n" % (threadNo, time.strftime('%Y-%m-%d %H:%M:%S')))
            else:
                print("只能输入1-100之间的数字！")
                with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                    fa.write("###键入的线程数不符合要求### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))
        except:
            print("只能输入1-100之间的数字！")
            with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                fa.write("###键入的线程数不符合要求### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))

        if exeFlag:
            for pathList in pathLists:
                print("\n执行SQL文件组：\n")
                for path in pathList:
                    print("\t - %s\n" % path)
                startThreads(threadNo, pathList, tns, enCode)
                print("\n本组SQL文件执行完毕\n")
            
            with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
                fa.write("###所有SQL文件执行完毕### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))

    os.system("pause")

    with open("EasyOM_DataQC_Report.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("关闭EasyOM DataQC Report %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")


if __name__ == "__main__":
    main()
#!/usr/local/bin python
#coding: utf-8

"""
Author: Chenfei Jovany Rong
"""

import cx_Oracle as ora
import os
import time
import threading

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
            with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
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
            with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
        
        else:
            logText = "\n【线程%d %s】SQL文件'%s'第%d条SQL执行失败，报错信息为：%s" % (self.threadID, time.strftime('%Y-%m-%d %H:%M:%S'), path, wrongNo, errInfo)
            print(logText)
            with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
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
    if not os.path.exists("EasyOM_SQL_Tool_Pro.conf"):
        text = ""
    else:
        with open("EasyOM_SQL_Tool_Pro.conf", "r", encoding="utf-8") as fr:
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
        
        with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
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

        with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
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
        with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
            fa.write("###通过配置文件获取文件编码方式 %s ### %s\n" % (dd["enCode"], time.strftime('%Y-%m-%d %H:%M:%S')))
        rDd["enCode"] = dd["enCode"]
    else:
        print("\n未读取到文件编码配置")
        enCode = input("\n【请输入文件编码方式】(默认gb2312，可选utf-8、utf-8-sig、ANSI等)：")

        if enCode == "":
            enCode = "gb2312"

        with open("EasyOM_SQL_Tool.log", "a+", encoding="utf-8") as fa:
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

def impNew(tnsRes, tnsFml):
    

    dbRes = ora.connect(tnsRes)
    
    crRes = dbRes.cursor()
    

    sqlResAll = """
    select x.xmid, x.qllx, x.bdcdyh, x.slbh from bdc_xm x
    """

    crRes.execute(sqlResAll)
    
    flag = True

    ct = 1

    while flag:
        row = crRes.fetchone()

        if row == None:
            row = ["", "", ""]

        if row[0] == None or row[0] == "":
            flag = False
            continue
        xmid = row[0]
        qllx = row[1]
        bdcdyh = row[2]
        slbh = row[3]
        print("\n【%d】 %s %s %s\n" % (ct, xmid, qllx, bdcdyh))

        if str(qllx) == "4":
            nowReal = getNowReal(tnsFml, str(bdcdyh), str(slbh))
            if nowReal[1] != "":
                makeZxSql("BDC_FDCQ", nowReal[1])
            if nowReal[0] != "":
                makeLsgx(tnsRes, xmid, nowReal[0], '1')
        else:
            nowReal = getNowReal(tnsFml, str(bdcdyh), str(slbh))
            if nowReal[0] != "":
                makeLsgx(tnsRes, xmid, nowReal[0], '0')
        
        ct += 1

    sqlZsGx = """
    insert into bdc_xm_zs_gx
    SELECT SYS_GUID(),XMID,ZSID FROM(
SELECT DISTINCT A.XMID, B.ZSID
  FROM BDC_QLR A,
       ((SELECT ZSID, BDCQZH FROM BDC_ZS) UNION ALL
        (SELECT ZSID, BDCQZH FROM BDCDJ.BDC_ZS)) B
 WHERE A.BDCQZH = B.BDCQZH)
    """
    crRes.execute(sqlZsGx)
    dbRes.commit()

    crRes.close()
    dbRes.close()
    print("\nFinish.\n")



def getNowReal(tns, bdcdyh, slbh):
    db = ora.connect(tns)
    cr = db.cursor()
    sql = """
    SELECT QLID, XMID FROM(
SELECT A.*,ROW_NUMBER() OVER(PARTITION BY A.BDCDYH ORDER BY a.DJSJ DESC) SX FROM BDC_FDCQ A 
inner join bdc_xm x on x.xmid = a.xmid
where x.slbh != '%s'
) WHERE SX =1
and bdcdyh = '%s'
    """ % (slbh, bdcdyh)
    cr.execute(sql)
    row = cr.fetchone()
    cr.close()
    db.close()
    if row == None:
        row = ['', '']
    return [row[1], row[0]]

def makeZxSql(ql, qlid):
    sql = """
    UPDATE %s T
    SET T.QSZT = 2
    WHERE T.QLID = '%s';

    UPDATE BDC_XM X
    SET X.QSZT = 2
    WHERE EXISTS (SELECT 1 FROM %s T WHERE T.XMID = X.XMID AND T.QLID = '%s');
    """ % (ql, qlid, ql, qlid)
    with open("ZxSql.log", "a+", encoding="utf-8") as fa:
        fa.write(sql)

def makeLsgx(tns, xmid, yxmid, zxyql):
    dbL = ora.connect(tns)
    crL = dbL.cursor()

    sql = """
    insert into bdc_xm_lsgx (gxid, xmid, yxmid, zxyql)
    values (sys_guid(), '%s', '%s', %s)
    """ % (xmid, yxmid, zxyql)

    crL.execute(sql)
    dbL.commit()
    crL.close()
    dbL.close()




def main():
    print("****************************************************")
    print("*****EasyOM TT增量办结业务历史关系建立及状态处理工具*****")
    print("****************************************************")

    print("\n作者：戎 晨飞(Chenfei Jovany Rong)\n")
    print("Copyright 2019 Summer Moon Talk\nAll rights reserved.\n")

    print("""
【使用须知】

    - 本产品属于EasyOM系列运维工具，EasyOM团队保留一切权利

    - 本产品适用于Oracle 11gR2以上版本数据库
    
    - 本产品遵循GNU GPL V3.0开源协议，如您同意方可继续使用，否则请退出本程序

"""    )

    os.system("pause")

    with open("EasyOM_TT_BDCDJ3_Imp.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("打开EasyOM TT增量办结业务历史关系建立及状态处理工具 %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")

    #cfg = readConf()

    #tns = cfg["tns"]

    tnsRes = "ttzlsj/gtis@200.1.1.172/orcl"
    tnsFml = "bdcdj_3/HFbdcdj137de@192.168.65.9/orcl"

    dbFlag = False

    try:
        dbRes = ora.connect(tnsRes)
        dbFml = ora.connect(tnsFml)
        dbFlag = True
        print("测试连接成功！\n")
        dbRes.close()
        dbFml.close()
        with open("EasyOM_TT_BDCDJ3_Imp.log", "a+", encoding="utf-8") as fa:
            fa.write("###连接数据库成功### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))
            
    except Exception as E:
        print(E)
        dbFlag = False
        print("连接失败！\n")
        with open("EasyOM_TT_BDCDJ3_Imp.log", "a+", encoding="utf-8") as fa:
            fa.write("###连接数据库失败：%s### %s\n" % (E, time.strftime('%Y-%m-%d %H:%M:%S')))

    if dbFlag == True:

        impNew(tnsRes, tnsFml)
            
        with open("EasyOM_TT_BDCDJ3_Imp.log", "a+", encoding="utf-8") as fa:
            fa.write("###所有SQL文件执行完毕### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))

    os.system("pause")

    with open("EasyOM_TT_BDCDJ3_Imp.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("关闭EasyOM TT增量办结业务历史关系建立及状态处理工具 %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")


if __name__ == "__main__":
    main()
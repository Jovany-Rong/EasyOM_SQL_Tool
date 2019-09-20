#!/usr/local/bin python
#coding: utf-8

"""
Author: Chenfei Jovany Rong
"""

import cx_Oracle as ora
import os
import time

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

def walkDir(dirPath):
    dirPathTmp = dirPath.replace("\\", "/")
    parDir = dirPathTmp.split("/")[-1]
    pathList = []
    for root, dirs, files in os.walk(dirPath):
        del dirs
        if not (root.endswith("/") or root.endswith("\\")):
            root = root + "/"
        for file in files:
            #if file.endswith(".sql"):
                #print((root + file).replace("\\", "/"))
            pathList.append((root + file).replace("\\", "/"))
    
    return [pathList, parDir]



def mountDir(dirPath, tns, field, model):
    global db, cr
    cr = db.cursor()
    dirPathTmp = dirPath.replace("\\", "/")
    parDir = dirPathTmp.split("/")[-1]
    ctFile = 0
    success = 0
    fail = 0
    for root, dirs, files in os.walk(dirPath):
        del dirs
        if not (root.endswith("/") or root.endswith("\\")):
            root = root + "/"
        for f in files:
            dd = dict()
            ct = 0
            pos = -1
            file = (root + f).replace("\\", "/")
            partList = file.split("/")
            for part in partList:
                if part == parDir:
                    pos = ct
                    break
                ct += 1
            if pos != -1:
                ctFile += 1
                logText = "\n【%s】正在挂接第%d个原文文件..." % (time.strftime('%Y-%m-%d %H:%M:%S'), ctFile)
                print(logText)
                with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                    fa.write("%s\n" % (logText))
                dd["pk"] = partList[pos + 1]
                dd["path"] = file
                res = archMount1File(tns, field, model, dd)
                if res:
                    success += 1
                else:
                    fail += 1

    cr.close()
    logText = "\n【%s】执行完毕，成功%d条，失败%d条\n" % (time.strftime('%Y-%m-%d %H:%M:%S'), success, fail)
    print(logText)
    with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
        fa.write("%s\n" % (logText))
            

def readConf():
    text = ""
    if not os.path.exists("EasyOM_GtisArchMount.conf"):
        text = ""
    else:
        with open("EasyOM_GtisArchMount.conf", "r", encoding="utf-8") as fr:
            text = fr.read()

        #print("\n监测到配置文件，正在读取配置文件")
    
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
        rDd["tns"] = None

    if "struc" in dd:
        print("\n读取到组织形式配置：%s" % dd["struc"])
        rDd["struc"] = dd["struc"]
    else:
        rDd["struc"] = None

    if "owner_model_name" in dd:
        print("\n读取到模型名称配置：%s" % dd["owner_model_name"])
        rDd["owner_model_name"] = dd["owner_model_name"]
    else:
        rDd["owner_model_name"] = None

    return rDd

def archMount1File(tns, field, model, dd):
    global db, cr
    dltTpl = """
    delete from t_original t
    where t.path = '<path>'
    and exists (select 1 from t_archive_<model> a where a.id = t.owner_id and a.<field> = '<val>')
    """
    sqlTpl = """
    insert into t_original (id, file_size, name, owner_id, owner_model_name, path, update_time, kgb)
    select sys_guid(), <file_size>, '<name>', t.id, '<model>', '<path>', to_timestamp(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss'), 0.789
    from t_archive_<model> t
    where t.<field> = '<val>' and rownum = 1
    """
    val = dd["pk"]
    path = dd["path"]
    file_size = str(os.path.getsize(path))
    name = path.split("/")[-1]
    dlt = dltTpl.replace("<model>", model).replace("<path>", path).replace("<field>", field).replace("<val>", val)
    sql = sqlTpl.replace("<file_size>", file_size).replace("<name>", name).replace("<model>", model).replace("<path>", path).replace("<field>", field).replace("<val>", val)

    try:
        cr.execute(dlt)
        cr.execute(sql)
        db.commit()
        logText = "\n【%s】挂接成功" % (time.strftime('%Y-%m-%d %H:%M:%S'))
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("%s\n" % (logText))
        return True
    except Exception as e:
        logText = "\n【ERROR %s】挂接失败：%s\n对应SQL为：%s;\n%s" % (time.strftime('%Y-%m-%d %H:%M:%S'), e, dlt, sql)
        print(logText)
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("%s\n" % (logText))
        return False

def archMount(tns, field, model, ddList):
    num = len(ddList)
    dltTpl = """
    delete from t_original t
    where t.path = '<path>'
    and exists (select 1 from t_archive_<model> a where a.id = t.owner_id and a.<field> = '<val>')
    """
    sqlTpl = """
    insert into t_original (id, file_size, name, owner_id, owner_model_name, path, update_time, kgb)
    select sys_guid(), <file_size>, '<name>', t.id, '<model>', '<path>', to_timestamp(to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss'), 0.789
    from t_archive_<model> t
    where t.<field> = '<val>' and rownum = 1
    """
    logText = "\n【%s】检测到%d个原文文件待挂接" % (time.strftime('%Y-%m-%d %H:%M:%S'), num)
    print(logText)
    with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
        fa.write("%s\n" % (logText))

    print("\n【%s】建立数据库连接" % (time.strftime('%Y-%m-%d %H:%M:%S')))
    isConn = False
    try:
        db = ora.connect(tns)
        cr = db.cursor()
        isConn = True
        logText = "\n【%s】数据库连接成功" % (time.strftime('%Y-%m-%d %H:%M:%S'))
        print(logText)
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("%s\n" % (logText))
    except Exception as e:
        isConn = False
        logText = "\n【ERROR %s】数据库连接失败：%s" % (time.strftime('%Y-%m-%d %H:%M:%S'), e)
        print(logText)
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("%s\n" % (logText))
        
    if isConn:
        ct = 1
        success = 0
        fail = 0

        for dd in ddList:
            logText = "\n【%s】正在挂接 %d / %d ..." % (time.strftime('%Y-%m-%d %H:%M:%S'), ct, num)
            with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                fa.write("%s\n" % (logText))
            val = dd["pk"]
            path = dd["path"]
            file_size = str(os.path.getsize(path))
            name = path.split("/")[-1]
            dlt = dltTpl.replace("<model>", model).replace("<path>", path).replace("<field>", field).replace("<val>", val)
            sql = sqlTpl.replace("<file_size>", file_size).replace("<name>", name).replace("<model>", model).replace("<path>", path).replace("<field>", field).replace("<val>", val)

            try:
                cr.execute(dlt)
                cr.execute(sql)
                db.commit()
                logText = "\n【%s】%d / %d 挂接成功" % (time.strftime('%Y-%m-%d %H:%M:%S'), ct, num)
                with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                    fa.write("%s\n" % (logText))
                success += 1
            except Exception as e:
                logText = "\n【ERROR %s】%d / %d 挂接失败：%s\n对应SQL为：%s;\n%s" % (time.strftime('%Y-%m-%d %H:%M:%S'), ct, num, e, dlt, sql)
                print(logText)
                with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                    fa.write("%s\n" % (logText))
                fail += 1
            
            ct += 1

        logText = "\n【%s】执行完毕，成功%d条，失败%d条\n" % (time.strftime('%Y-%m-%d %H:%M:%S'), success, fail)
        print(logText)
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("%s\n" % (logText))

def main():
    print("***********************************************")
    print("*****EasyOM国图档案系统原文挂接工具 V2.0*******")
    print("***********************************************")

    print("\n作者：戎 晨飞(Chenfei Jovany Rong)\n")
    print("Copyright 2019 Summer Moon Talk\nAll rights reserved.\n")

    print("""
【使用须知】

    - 本产品属于EasyOM系列运维工具，EasyOM团队保留一切权利

    - 本产品适用于Oracle 11gR2以上版本数据库

    - 请存入专用配置文件在本程序目录以供程序读取
    
    - 本产品遵循GNU GPL V3.0开源协议，如您同意方可继续使用，否则请退出本程序

"""    )

    os.system("pause")

    with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("打开EasyOM国图档案系统原文挂接工具 %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")

    cfg = readConf()

    dbFlag = False

    if cfg["tns"] and cfg["struc"] and cfg["owner_model_name"]:
        dbFlag = True
        print("\n配置文件读取成功！")
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("###配置文件读取成功### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))
            fa.write("###tns=%s### %s\n" % (cfg["tns"], time.strftime('%Y-%m-%d %H:%M:%S')))
            fa.write("###struc=%s### %s\n" % (cfg["struc"], time.strftime('%Y-%m-%d %H:%M:%S')))
            fa.write("###owner_model_name=%s### %s\n" % (cfg["owner_model_name"], time.strftime('%Y-%m-%d %H:%M:%S')))
    else:
        dbFlag = False
        print("\n配置文件读取失败！请检查EasyOM_GtisArchMount.conf文件。")
        with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
            fa.write("###【ERROR】配置文件读取失败### %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S')))

    tns = cfg["tns"].strip()
    struc = cfg["struc"].strip()
    model = cfg["owner_model_name"].strip()

    field = struc.replace("/", "")

    if dbFlag:
        try:
            global db
            db = ora.connect(tns)
            dbFlag = True
            print("\n测试连接成功！\n")
            with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                fa.write("###连接数据库 %s 成功### %s\n" % (tns, time.strftime('%Y-%m-%d %H:%M:%S')))
            
        except Exception as E:
            print("\n")
            print(E)
            dbFlag = False
            print("连接失败！\n")
            with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
                fa.write("###【ERROR】连接数据库 %s 失败：%s### %s\n" % (tns, E, time.strftime('%Y-%m-%d %H:%M:%S')))

    if dbFlag:
        dirText = ""
        dirText = input("\n【请输入原文上层目录】：")
        dirText = dirText.strip()

        mountDir(dirText, tns, field, model)

    os.system("pause")

    with open("EasyOM_GtisArchMount.log", "a+", encoding="utf-8") as fa:
        fa.write("*******************************************************\n")
        fa.write("关闭EasyOM国图档案系统原文挂接工具 %s\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
        fa.write("*******************************************************\n")


if __name__ == "__main__":
    main()
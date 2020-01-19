#!/usr/local/bin python
# -*- coding: utf-8 -*-

import time
import os
#import verInfo
import base64
import webbrowser

class Report(object):
    __starter = """
    <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <title>EasyOM数据检查报告</title>
            <link href="../css/bootstrap.min.css" rel="stylesheet">
            <style type="text/css" media="screen">
                body  { font-family: Microsoft YaHei,Tahoma,arial,helvetica,sans-serif;padding: 20px;}
            </style>
        </head>
        <body>
        <header>
            <p style = "text-align:right; color: #CCCCCC">Powered by Chenfei Jovany Rong, Technology & Quality Management Section</p>
        </header>
    """

    __finisher = """
        <footer>
            <p style = "text-align:right; color: #CCCCCC">Powered by Chenfei Jovany Rong, Technology & Quality Management Section</p>
            <p style = "text-align:right; color: #CCCCCC">Contact information: rongchenfei@gtmap.cn</p>
        </footer>
        </body>
        </html>"""

    body = """
        """

    def makeRpt(self):
        #ver = verInfo.version
        rptTime = time.localtime(time.time())
        now = time.strftime('%Y-%m-%d_%H-%M-%S',rptTime)
        rptFile = r"report/EasyOM_DataQC_Report_" + now + r".html"
        #encFile = r"rep/EasyOM_Report_" + now + r".eom"

        text = """
        <div id = "statement">
            <h2 style = "text-align: center">EasyOM数据检查报告</h2>
            <p style = "font-style:italic; text-align: right">数据检查时间：%s</p>
        </div>
        """ % (time.strftime('%Y-%m-%d %H:%M:%S', rptTime))

        self.body = text + self.body

        report = self.__starter + self.body + self.__finisher

        if not os.path.isdir("report"):
            os.makedirs("report")

        with open(rptFile, "w+", encoding='utf-8') as f:
            f.write(report)

        #encReport = base64.b64encode(report.encode("utf-8")).decode("utf-8")

        #if not os.path.isdir("rep"):
            #os.makedirs("rep")

        #with open(encFile, "w+", encoding='utf-8') as f:
            #f.write(encReport)

        try:
            webbrowser.open("file://" + os.path.realpath(rptFile))
        except:
            pass

    def appendParagraph(self, str):
        str = str.replace("\n", "<br />")
        text = """
        <p>%s</p>
        """ % (str)

        self.body = self.body + text

    def appendHTML(self, str):
        self.body = self.body + str

    def appendH1(self, str):
        text = """
        <h1>%s</h1>
        """ % (str)

        self.body = self.body + text

    def appendH2(self, str):
        text = """
        <h2>%s</h2>
        """ % (str)

        self.body = self.body + text

    def appendH3(self, str):
        text = """
        <h3>%s</h3>
        """ % (str)

        self.body = self.body + text

    def appendH4(self, str):
        text = """
        <h4>%s</h4>
        """ % (str)

        self.body = self.body + text

    def appendH5(self, str):
        text = """
        <h5>%s</h5>
        """ % (str)

        self.body = self.body + text

    def appendH6(self, str):
        text = """
        <h6>%s</h6>
        """ % (str)

        self.body = self.body + text

    def init(self):
        self.body = """
        """

class Table(object):
    __starter = """
    <table border = "1" style = "text-align: center">
    """

    __finisher = """
    </table>
    """

    cap = """
    <caption></caption>
    """

    body = ""

    def makeTable(self, str):
        cap = """
        <caption>%s</caption>
        <br />
        """ % (str)

        self.cap = cap

        return self.__starter + self.cap + self.body + self.__finisher

    def addHead(self, *str):
        text = """
        <tr>
        """
        for i in str:
            text = text + """<th style = "background-color: #33CCFF">%s</th>\n""" % (i)
        
        text = text + """
        </tr>
        """

        self.body = self.body + text

    def addRow(self, *str):
        text = """
        <tr>
        """
        for i in str:
            text = text + "<td>%s</td>\n" % (i)
        
        text = text + """
        </tr>
        """

        self.body = self.body + text
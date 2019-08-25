d:
cd D:\dev\python3\EasyOM_Database_Management

del *.spec
rd /s /q build 

pyinstaller -i "icon.ico" -c -F "EasyOM_SQL_Tool.py" --key 1987623579235681

del *.spec
rd /s /q build

pause
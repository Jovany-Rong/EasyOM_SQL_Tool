d:
cd D:\dev\python3\EasyOM_Database_Management

del *.spec
rd /s /q build 

pyinstaller -i "icon.ico" -c -F "demo.py"

del *.spec
rd /s /q build

pause
@echo off
cd /d %~dp0
set ROBOT_SCRIPT_DIR=%~dp0
python "Goiânia taxas impostos.py"

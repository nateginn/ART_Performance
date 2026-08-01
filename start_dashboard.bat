@echo off
cd /d d:\Dev\ART_Performance
if not exist data mkdir data

:loop
"d:\Dev\ART_Performance\venv\Scripts\python.exe" -m streamlit run "d:\Dev\ART_Performance\dashboard.py" >>"d:\Dev\ART_Performance\data\dashboard_streamlit.log" 2>&1
set CODE=%errorlevel%
echo [%date% %time%] streamlit exited with code %CODE% - restarting in 5s>>"d:\Dev\ART_Performance\data\dashboard_supervisor.log"
timeout /t 5 /nobreak >nul
goto loop

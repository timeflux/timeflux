@echo off
set ROOT=C:\Users\%USERNAME%\Miniconda3
call %ROOT%\Scripts\activate.bat %ROOT%
call conda activate timeflux
start python -m timeflux.helpers.handler launch timeflux -d test_1.yaml
pause
call python -m timeflux.helpers.handler terminate
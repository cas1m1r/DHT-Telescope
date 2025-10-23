@ECHO OFF
:loop
del geo_cache.json
cls
%*
python resolver.py top
timeout /t 13 > NUL
goto loop

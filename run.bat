@ECHO OFF
:loop
del geo_cache.json
cls
%*
python resolver.py summary --geo-cache geo_cache.json --cutoff-hours 2 --refresh-sec 10
timeout /t 13 > NUL
goto loop

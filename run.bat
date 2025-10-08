@ECHO OFF
:loop
del geo_cache.json
cls
%*
python resolver.py summary --plot live --plot-output bt_map_opacity_live.html --geo-cache geo_cache.json --half-life-min 15 --cutoff-hours 6 --refresh-sec 10
timeout /t 13 > NUL
goto loop

@echo off
setlocal EnableDelayedExpansion
for /L %%i in (1,1,10) do (
    set /a offset=%%i*500
    bin\loader.exe -csv .\data -limit 500 -offset !offset!
    echo Lot %%i charge
)
@echo off
echo Correction des imports LevelDB...

for /r %%f in (*.go) do (
    powershell -Command "(Get-Content '%%f') -replace 'github.com/google/leveldb-go/leveldb/db', 'github.com/syndtr/goleveldb/leveldb/opt' -replace 'github.com/google/leveldb-go/leveldb', 'github.com/syndtr/goleveldb/leveldb' -replace '&db.Options', '&opt.Options' -replace 'db.SnappyCompression', 'opt.SnappyCompression' -replace 'WriteBufferSize:', 'WriteBuffer:' | Set-Content '%%f'"
)

echo.
echo Mise a jour go.mod...
go mod tidy

echo.
echo Terminé! Vous pouvez maintenant compiler:
echo   go build -o bin\setup.exe .\cmd\setup
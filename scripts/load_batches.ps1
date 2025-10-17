for ($i=1; $i -le 10; $i++) {
    $offset = $i * 500
    .\bin\loader.exe -csv .\data -limit 500 -offset $offset
    Write-Host "Lot $i chargé"
}
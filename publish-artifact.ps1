<#
  publish-artifact.ps1
  Publica un instalador en el nginx de artefactos (contenedor orb-artifacts) y actualiza latest.txt.
  No guarda secretos: la clave de Portainer se lee de la variable de entorno PORTAINER_PW.

  Uso:
    $env:PORTAINER_PW = 'la-clave-admin'
    .\publish-artifact.ps1 -Msi build\dist\ORB-BLOOMBERG-1.0.msi -Version 1.0

  Variables opcionales:
    PORTAINER_URL   (default http://172.16.0.8:9000)
    PORTAINER_USER  (default admin)

  Queda servido en:  http://172.16.0.8:8060/<archivo.msi>  y  http://172.16.0.8:8060/latest.txt
#>
param(
  [Parameter(Mandatory=$true)][string]$Msi,
  [Parameter(Mandatory=$true)][string]$Version
)
$ErrorActionPreference = 'Stop'

$base = if ($env:PORTAINER_URL) { $env:PORTAINER_URL } else { 'http://172.16.0.8:9000' }
$user = if ($env:PORTAINER_USER) { $env:PORTAINER_USER } else { 'admin' }
$pw   = $env:PORTAINER_PW
if (-not $pw) { throw 'Falta $env:PORTAINER_PW (clave del admin de Portainer).' }
if (-not (Test-Path $Msi)) { throw "No existe el archivo: $Msi" }

# Auth
$auth = Invoke-RestMethod -Uri "$base/api/auth" -Method Post `
          -Body (@{Username=$user;Password=$pw}|ConvertTo-Json) -ContentType 'application/json' -TimeoutSec 15
$h  = @{ Authorization = "Bearer $($auth.jwt)" }
$dk = "$base/api/endpoints/3/docker"

# Staging: el .msi + latest.txt
$stage = Join-Path $env:TEMP ('orbpub_' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $stage | Out-Null
$msiName = Split-Path $Msi -Leaf
Copy-Item $Msi (Join-Path $stage $msiName)
Set-Content -Path (Join-Path $stage 'latest.txt') -Value $Version -NoNewline

# Tar y subida por la API de Docker (extrae en /usr/share/nginx/html del contenedor)
$tar = Join-Path $env:TEMP ('orbpub_' + [guid]::NewGuid().ToString('N') + '.tar')
tar -cf $tar -C $stage $msiName 'latest.txt'
$path = [uri]::EscapeDataString('/usr/share/nginx/html')
Invoke-RestMethod -Uri "$dk/containers/orb-artifacts/archive?path=$path" -Method Put -Headers $h `
  -InFile $tar -ContentType 'application/x-tar' -TimeoutSec 600 | Out-Null
Write-Host "Subido: $msiName   (latest.txt = $Version)"

# Verificacion HTTP
$pubBase = 'http://' + ([uri]$base).Host + ':8060'
try {
  $r = Invoke-WebRequest -Uri "$pubBase/$msiName" -Method Head -TimeoutSec 30 -UseBasicParsing
  Write-Host "OK -> $pubBase/$msiName  (HTTP $($r.StatusCode))"
} catch {
  Write-Host "Aviso: no pude verificar via HTTP ($($_.Exception.Message))"
}
Write-Host "latest.txt -> $pubBase/latest.txt"

# A/B-замер предзагрузки: один и тот же сценарий k6/prefetch-chain.js
# с PREFETCH_ENABLED=false и true, несколько прогонов, порядок режимов
# чередуется, перед каждым прогоном прокси пересоздаётся (холодный кэш и
# пустой предиктор, прогрев внутри сценария).
#
#   .\bench\prefetch-ab.ps1 -Runs 3 -Duration 2m
#
# Перед первым запуском: docker compose up -d --wait postgres rabbitmq upstream proxy
# и migrations/bench_seed.sql (см. README).
param(
    [int]$Runs = 3,
    [int]$Books = 2000,
    [int]$Rate = 40,
    [string]$Warmup = "30s",
    [string]$Duration = "2m",
    [string]$CacheMaxTtl = "20s",
    [int]$ThinkMs = 150,
    [string]$ProxyUrl = "http://localhost:8080"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$results = Join-Path $PSScriptRoot "results"
New-Item -ItemType Directory -Force $results | Out-Null

$env:CACHE_MAX_TTL = $CacheMaxTtl
$files = @()
for ($run = 1; $run -le $Runs; $run++) {
    $modes = if ($run % 2 -eq 1) { @("false", "true") } else { @("true", "false") }
    foreach ($mode in $modes) {
        $env:PREFETCH_ENABLED = $mode
        $label = "prefetch-$mode-run$run"
        $file = Join-Path $results "$label.json"
        # docker compose and k6 write progress to stderr. Windows PowerShell
        # turns that into errors when output is redirected, so native commands
        # run with Continue and are judged by their exit code only.
        $ErrorActionPreference = "Continue"
        docker compose -f (Join-Path $root "docker-compose.yml") up -d --force-recreate --no-deps --wait proxy
        $composeExit = $LASTEXITCODE
        if ($composeExit -eq 0) {
            k6 run --quiet `
                -e PROXY_URLS=$ProxyUrl -e BOOKS=$Books -e RATE=$Rate -e WARMUP=$Warmup `
                -e DURATION=$Duration -e THINK_MS=$ThinkMs -e LABEL=$label -e RESULT_FILE=$file `
                (Join-Path $root "k6\prefetch-chain.js")
            $k6Exit = $LASTEXITCODE
        }
        $ErrorActionPreference = "Stop"
        if ($composeExit -ne 0) { throw "proxy did not become healthy" }
        # k6 exits with 99 when a threshold fails (error rate >= 1%); the result
        # file is still written, so keep it and let the table show the run.
        if ($k6Exit -ne 0) { Write-Warning "k6 exited with $k6Exit for $label" }
        if (-not (Test-Path $file)) { throw "k6 wrote no result for $label" }
        $files += $file
    }
}

function Median([double[]]$values) {
    $sorted = $values | Sort-Object
    $n = $sorted.Count
    if ($n % 2 -eq 1) { return $sorted[[int][math]::Floor($n / 2)] }
    return ($sorted[$n / 2 - 1] + $sorted[$n / 2]) / 2
}

$all = $files | ForEach-Object { Get-Content $_ -Raw | ConvertFrom-Json }
$rows = foreach ($mode in @("false", "true")) {
    $set = $all | Where-Object { $_.label -like "prefetch-$mode-*" }
    [pscustomobject]@{
        prefetch          = $mode
        runs              = $set.Count
        "all p95"         = "{0:N2}" -f (Median ($set | ForEach-Object { $_.latency_ms.all.p95 }))
        "card p95"        = "{0:N2}" -f (Median ($set | ForEach-Object { $_.latency_ms.card.p95 }))
        "authors p50"     = "{0:N2}" -f (Median ($set | ForEach-Object { $_.latency_ms.authors.p50 }))
        "authors p95"     = "{0:N2}" -f (Median ($set | ForEach-Object { $_.latency_ms.authors.p95 }))
        "authors p99"     = "{0:N2}" -f (Median ($set | ForEach-Object { $_.latency_ms.authors.p99 }))
        "hit ratio"       = "{0:P1}" -f (Median ($set | ForEach-Object { $_.server.hit_ratio }))
        "upstream rps"    = "{0:N1}" -f (Median ($set | ForEach-Object { $_.server.upstream_rps }))
        "useless prefetch"= ($set | ForEach-Object { if ($null -ne $_.server.useless_prefetch_share) { "{0:P0}" -f $_.server.useless_prefetch_share } else { "n/a" } }) -join " / "
    }
}
Write-Host "`nMedian over $Runs runs (latency in ms; files in $results):"
$rows | Format-Table -AutoSize

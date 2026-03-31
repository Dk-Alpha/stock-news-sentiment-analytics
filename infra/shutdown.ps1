<#
.SYNOPSIS
Safely backs up the PostgreSQL database with a timestamp and shuts down the Docker stack.

.DESCRIPTION
Run this from the project ROOT directory:
  .\infra\shutdown.ps1

It calls pg_dump on the running postgres container, saves a .sql dump
to .\backups\ with a timestamp, then calls docker-compose down.
#>

$RootDir = Split-Path -Parent $PSScriptRoot
$BackupDir = Join-Path $RootDir "backups"
$ComposePath = Join-Path $PSScriptRoot "docker-compose.yml"

If (!(Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
    Write-Host "Created backups directory: $BackupDir" -ForegroundColor Green
}

$Timestamp = Get-Date -Format "yyyy_MM_dd_HH_mm_ss"
$BackupFile = Join-Path $BackupDir "db_dump_$Timestamp.sql"

Write-Host "Starting database backup..." -ForegroundColor Cyan

$PgRunning = docker ps -q -f "name=^postgres$"
If ([string]::IsNullOrWhiteSpace($PgRunning)) {
    Write-Host "WARNING: PostgreSQL container 'postgres' is not running. Skipping backup." -ForegroundColor Yellow
} Else {
    docker exec -t postgres pg_dump -U pipeline_user -d news_pipeline > $BackupFile
    If ($LASTEXITCODE -eq 0) {
        Write-Host "Database backed up successfully → $BackupFile" -ForegroundColor Green
    } Else {
        Write-Host "ERROR: Database backup failed!" -ForegroundColor Red
    }
}

Write-Host "Shutting down Docker stack..." -ForegroundColor Cyan
docker-compose -f $ComposePath down

Write-Host "All done. Goodbye." -ForegroundColor Green

<#
.SYNOPSIS
Safely backups the PostgreSQL database with a timestamp and shuts down the Docker stack.

.DESCRIPTION
This script uses docker exec to run pg_dump on the running postgres container, saves it locally to ./backups, and then safely tears down docker-compose.
#>

$BackupDir = ".\backups"
If (!(Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
    Write-Host "Created backups directory: $BackupDir" -ForegroundColor Green
}

$Timestamp = Get-Date -Format "yyyy_MM_dd_HH_mm_ss"
$BackupFile = "$BackupDir\db_dump_$Timestamp.sql"

Write-Host "Starting database backup..." -ForegroundColor Cyan

# Check if postgres container is running
$PgRunning = docker ps -q -f "name=^postgres$"
If ([string]::IsNullOrWhiteSpace($PgRunning)) {
    Write-Host "WARNING: PostgreSQL container 'postgres' is not running. Cannot create a local backup." -ForegroundColor Yellow
} Else {
    # Dump the database
    docker exec -t postgres pg_dump -U pipeline_user -d news_pipeline -F c > $BackupFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Database successfully backed up to $BackupFile" -ForegroundColor Green
    } else {
        Write-Host "ERROR: Database backup failed!" -ForegroundColor Red
    }
}

Write-Host "Shutting down Docker containers..." -ForegroundColor Cyan
docker-compose down

Write-Host "Shutdown complete." -ForegroundColor Green

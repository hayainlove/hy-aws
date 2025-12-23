# ============================================
# Complete Export API Test Script
# ============================================

# Configuration
$apiBaseUrl = "https://iteb3yso7d.execute-api.ap-southeast-1.amazonaws.com/dev"
$userId = "user123"

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Starting Export API Test" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan

# Step 1: Create Export Job
Write-Host "`n[Step 1] Creating export job..." -ForegroundColor Yellow

$createBody = @{
    export_type = "users"
    user_id = $userId
    filters = @{
        status = "active"
    }
    format = "csv"
} | ConvertTo-Json

try {
    $createResponse = Invoke-RestMethod -Uri "$apiBaseUrl/exports" -Method Post -Body $createBody -ContentType "application/json"
    $jobId = $createResponse.job_id
    
    Write-Host "Export job created successfully!" -ForegroundColor Green
    Write-Host "  Job ID: $jobId" -ForegroundColor White
    Write-Host "  Status: $($createResponse.status)" -ForegroundColor White
}
catch {
    Write-Host "Failed to create export job" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit
}

# Step 2: Poll for completion
Write-Host "`n[Step 2] Waiting for job to complete..." -ForegroundColor Yellow

$maxAttempts = 30
$attempt = 0
$downloadUrl = $null
$completed = $false

while ($attempt -lt $maxAttempts -and -not $completed) {
    $attempt++
    Start-Sleep -Seconds 5
    
    try {
        $statusResponse = Invoke-RestMethod -Uri "$apiBaseUrl/exports/$jobId" -Method Get
        $status = $statusResponse.status
        
        Write-Host "  Attempt $attempt/$maxAttempts - Status: $status" -ForegroundColor Cyan
        
        if ($status -eq "completed") {
            Write-Host "Job completed successfully!" -ForegroundColor Green
            Write-Host "  Records exported: $($statusResponse.record_count)" -ForegroundColor White
            $downloadUrl = $statusResponse.download_url
            $completed = $true
        }
        elseif ($status -eq "failed") {
            Write-Host "Job failed: $($statusResponse.error_message)" -ForegroundColor Red
            exit
        }
    }
    catch {
        Write-Host "  Error checking status: $($_.Exception.Message)" -ForegroundColor Red
    }
}

if ($null -eq $downloadUrl) {
    Write-Host "Job did not complete within expected time" -ForegroundColor Red
    exit
}

# Step 3: Download the file
Write-Host "`n[Step 3] Downloading export file..." -ForegroundColor Yellow

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = ".\exports\users_export_$timestamp.csv"
$outputDir = Split-Path -Parent $outputFile

if (!(Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

try {
    Invoke-WebRequest -Uri $downloadUrl -OutFile $outputFile
    Write-Host "File downloaded successfully!" -ForegroundColor Green
    Write-Host "  Location: $outputFile" -ForegroundColor White
    
    # Show file size
    $fileInfo = Get-Item $outputFile
    Write-Host "  Size: $($fileInfo.Length) bytes" -ForegroundColor White
    
    # Preview first few lines
    Write-Host "`n[Preview] First 5 lines:" -ForegroundColor Yellow
    Get-Content $outputFile -First 5 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
}
catch {
    Write-Host "Failed to download file" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
}

# Step 4: List all jobs for user
Write-Host "`n[Step 4] Listing all export jobs for user..." -ForegroundColor Yellow

try {
    $listResponse = Invoke-RestMethod -Uri "$apiBaseUrl/exports/dummy-id?user_id=$userId" -Method Get
    Write-Host "Found $($listResponse.count) export jobs" -ForegroundColor Green
    
    if ($listResponse.count -gt 0) {
        Write-Host "`nRecent Jobs:" -ForegroundColor White
        $listResponse.jobs | Select-Object -First 5 | Format-Table -Property @{
            Label = "Job ID"
            Expression = { $_.job_id.Substring(0, 8) + "..." }
        }, export_type, status, @{
            Label = "Created"
            Expression = { ([DateTime]$_.createdAt).ToString("yyyy-MM-dd HH:mm") }
        } -AutoSize
    }
}
catch {
    Write-Host "Failed to list jobs" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
}

Write-Host "`n======================================" -ForegroundColor Cyan
Write-Host "Test Complete!" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
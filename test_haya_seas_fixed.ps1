# =============================================================================
# MyHayati Phase 2 - PowerShell Test Script for User "Haya Seas" (FIXED)
# =============================================================================

$API_URL = "https://iteb3yso7d.execute-api.ap-southeast-1.amazonaws.com/dev"

# Test Data
$testUser = @{
    email = "haya.seas@example.com"
    userName = "hayaseas"
    firstName = "Haya"
    lastName = "Seas"
    phone = "+60123456789"
    address = "123 Beach Road, Miri, Sarawak"
    age = 28
    location = "Miri, Sarawak, Malaysia"
    status = "active"
} | ConvertTo-Json

$global:userId = $null
$global:orderId = $null

function Write-Section($title) {
    Write-Host "`n$('='*80)" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host "$('='*80)" -ForegroundColor Cyan
}

function Write-Test($name) {
    Write-Host "`n🧪 TEST: $name" -ForegroundColor Yellow
    Write-Host "$('-'*80)" -ForegroundColor Yellow
}

function Write-Success($message) {
    Write-Host "✅ SUCCESS: $message" -ForegroundColor Green
}

function Write-Error-Custom($message) {
    Write-Host "❌ ERROR: $message" -ForegroundColor Red
}

function Test-CreateUser {
    Write-Test "Create User - Haya Seas"
    
    $url = "$API_URL/users"
    Write-Host "POST $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Post -Body $testUser -ContentType "application/json"
        
        if ($response.user_id) {
            $global:userId = $response.user_id
        } elseif ($response.user.user_id) {
            $global:userId = $response.user.user_id
        }
        
        Write-Success "User created with ID: $($global:userId)"
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        return $true
    }
    catch {
        Write-Error-Custom "Failed to create user: $_"
        return $false
    }
}

function Test-GetUser {
    Write-Test "Get User by ID"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/users/$($global:userId)"
    Write-Host "GET $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get
        
        # Handle nested response
        $user = if ($response.user) { $response.user } else { $response }
        
        if ($user.firstName -eq "Haya" -and $user.lastName -eq "Seas") {
            Write-Success "User retrieved correctly"
            Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
            return $true
        }
        else {
            Write-Error-Custom "User data mismatch. Got: $($user.firstName) $($user.lastName)"
            return $false
        }
    }
    catch {
        Write-Error-Custom "Failed to get user: $_"
        return $false
    }
}

function Test-UpdateUser {
    Write-Test "Update User"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/users/$($global:userId)"
    $updateData = @{
        age = 29
        location = "Kuala Lumpur, Malaysia"
        phone = "+60987654321"
    } | ConvertTo-Json
    
    Write-Host "PUT $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Put -Body $updateData -ContentType "application/json"
        Write-Success "User updated successfully"
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        return $true
    }
    catch {
        Write-Error-Custom "Failed to update user: $_"
        return $false
    }
}

function Test-SearchUsers {
    Write-Test "Search Users"
    
    Write-Host "⏳ Waiting 10 seconds for OpenSearch to sync..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
    
    $url = "$API_URL/users/search?q=Haya"
    Write-Host "GET $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get
        
        if ($response.users.Count -gt 0) {
            Write-Success "Found $($response.users.Count) user(s) matching 'Haya'"
            Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
            return $true
        }
        
        Write-Error-Custom "No users found"
        return $false
    }
    catch {
        Write-Error-Custom "Search failed: $_"
        return $false
    }
}

function Test-GenerateUploadUrl {
    Write-Test "Generate Upload URL"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/users/$($global:userId)/files/upload-url"
    $uploadData = @{
        fileType = "profile_picture"
        fileName = "haya_profile.jpg"
        contentType = "image/jpeg"
    } | ConvertTo-Json
    
    Write-Host "POST $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Post -Body $uploadData -ContentType "application/json"
        
        if ($response.uploadUrl) {
            Write-Success "Upload URL generated successfully"
            return $true
        }
        
        Write-Error-Custom "No uploadUrl in response"
        return $false
    }
    catch {
        Write-Error-Custom "Failed to generate upload URL: $_"
        return $false
    }
}

function Test-CreateOrder {
    Write-Test "Create Order"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/orders"
    $orderData = @{
        user_id = $global:userId
        items = @(
            @{
                product_id = "PROD-001"
                product_name = "Organic Honey"
                quantity = 2
                price = 25.50
            },
            @{
                product_id = "PROD-002"
                product_name = "Royal Jelly"
                quantity = 1
                price = 45.00
            }
        )
        total = 96.00
        shipping_address = "123 Beach Road, Miri, Sarawak"
    } | ConvertTo-Json -Depth 10
    
    Write-Host "POST $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Post -Body $orderData -ContentType "application/json"
        
        $global:orderId = $response.order_id
        Write-Success "Order created with ID: $($global:orderId)"
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        return $true
    }
    catch {
        Write-Error-Custom "Failed to create order: $_"
        return $false
    }
}

function Test-ListOrders {
    Write-Test "List User Orders"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/orders?user_id=$($global:userId)"
    Write-Host "GET $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get
        Write-Success "Found $($response.orders.Count) order(s) for user"
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        return $true
    }
    catch {
        Write-Error-Custom "Failed to list orders: $_"
        return $false
    }
}

function Test-DeleteUser {
    Write-Test "Delete User"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/users/$($global:userId)"
    Write-Host "DELETE $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Delete
        Write-Success "User deleted successfully"
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        return $true
    }
    catch {
        Write-Error-Custom "Failed to delete user: $_"
        return $false
    }
}

function Test-VerifyDeletion {
    Write-Test "Verify User Deletion"
    
    if (-not $global:userId) {
        Write-Error-Custom "No user ID available"
        return $false
    }
    
    $url = "$API_URL/users/$($global:userId)"
    Write-Host "GET $url" -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get
        Write-Error-Custom "User still exists"
        return $false
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 404) {
            Write-Success "User successfully deleted (404 returned)"
            return $true
        }
        Write-Error-Custom "Unexpected error"
        return $false
    }
}

function Run-AllTests {
    Write-Section "MyHayati Phase 2 - Comprehensive Test Suite (FIXED)"
    Write-Host "Testing User: Haya Seas" -ForegroundColor Cyan
    Write-Host "API URL: $API_URL" -ForegroundColor Cyan
    
    $results = @{}
    
    $results["Create User"] = Test-CreateUser
    Start-Sleep -Seconds 2
    
    if (-not $results["Create User"]) {
        Write-Error-Custom "Cannot continue"
        return
    }
    
    $results["Get User"] = Test-GetUser
    Start-Sleep -Seconds 2
    
    $results["Update User"] = Test-UpdateUser
    Start-Sleep -Seconds 2
    
    $results["Search Users"] = Test-SearchUsers
    Start-Sleep -Seconds 2
    
    $results["Generate Upload URL"] = Test-GenerateUploadUrl
    Start-Sleep -Seconds 2
    
    $results["Create Order"] = Test-CreateOrder
    Start-Sleep -Seconds 3
    
    $results["List Orders"] = Test-ListOrders
    Start-Sleep -Seconds 2
    
    $results["Delete User"] = Test-DeleteUser
    Start-Sleep -Seconds 2
    
    $results["Verify Deletion"] = Test-VerifyDeletion
    
    Write-Section "Test Results Summary"
    
    $passed = ($results.Values | Where-Object { $_ -eq $true }).Count
    $total = $results.Count
    
    foreach ($test in $results.GetEnumerator()) {
        $status = if ($test.Value) { "✅ PASS" } else { "❌ FAIL" }
        Write-Host "$status - $($test.Key)"
    }
    
    Write-Host "`n$('='*80)"
    Write-Host "Total: $passed/$total tests passed ($([Math]::Round(($passed/$total)*100, 1))%)" -ForegroundColor Cyan
    Write-Host "$('='*80)`n"
    
    if ($passed -eq $total) {
        Write-Host "🎉 All tests passed!" -ForegroundColor Green
    }
    else {
        Write-Host "⚠️  Some tests failed." -ForegroundColor Yellow
    }
}

Run-AllTests

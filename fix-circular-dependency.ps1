# Read the stack file
$stackPath = "my_hayati_phase2\my_hayati_phase2_stack.py"
$content = Get-Content $stackPath -Raw

# Remove all authorizer references from add_method calls
$content = $content -replace ',\s*authorizer=cognito_authorizer', ''
$content = $content -replace ',\s*authorization_type=apigateway\.AuthorizationType\.COGNITO', ''

# Save the modified file
$content | Set-Content $stackPath

Write-Host "✓ Removed all Cognito authorizers from API routes" -ForegroundColor Green
Write-Host "Deploy now, then we'll add auth back via AWS Console" -ForegroundColor Yellow
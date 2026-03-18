# Define Grupo-Fleury como Admin no repo via API do GitHub
# Uso: .\scripts\github-set-collaborator-admin.ps1 -Token "ghp_xxxx"
# Crie um token em: https://github.com/settings/tokens (escopo: repo)

param(
    [Parameter(Mandatory = $true)]
    [string]$Token
)

$owner = "automatizetech-org"
$repo = "fleury-insights-hub"
$username = "Grupo-Fleury"
$url = "https://api.github.com/repos/$owner/$repo/collaborators/$username"

$headers = @{
    "Authorization" = "token $Token"
    "Accept"        = "application/vnd.github.v3+json"
}
$body = @{ permission = "admin" } | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $url -Method Put -Headers $headers -Body $body -ContentType "application/json"
    Write-Host "OK: $username definido como Admin no $owner/$repo" -ForegroundColor Green
} catch {
    Write-Host "Erro: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) { Write-Host $_.ErrorDetails.Message }
}

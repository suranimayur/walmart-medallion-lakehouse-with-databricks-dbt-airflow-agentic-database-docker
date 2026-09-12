# ==============================================================================
# load-env.ps1 - load walmart-airflow/.env into the current PowerShell session
# ==============================================================================
# USAGE (local dbt development on Windows):
#   cd walmart-airflow
#   . .\load-env.ps1                      # note the leading dot (dot-source)!
#   cd ..\walmart_project
#   dbt debug                             # picks up DATABRICKS_* automatically
#
# Equivalent bash (Git Bash / WSL) one-liner:
#   export $(grep -vE '^\s*#|^\s*$' .env | xargs) && cd ../walmart_project && dbt debug
#   (PowerShell/WSL set values verbatim; Git Bash mangles leading-slash values)
# ==============================================================================

$envFile = Join-Path $PSScriptRoot '.env'

if (-not (Test-Path $envFile)) {
    Write-Error (".env not found at " + $envFile + " - copy .env.example to .env and fill it in first.")
    return
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq '' -or $line.StartsWith('#')) { return }

    $idx = $line.IndexOf('=')
    if ($idx -lt 1) { return }

    $key = $line.Substring(0, $idx).Trim()
    $val = $line.Substring($idx + 1).Trim()

    # strip surrounding quotes if present
    if ($val.Length -ge 2 -and ($val.StartsWith('"') -or $val.StartsWith("'"))) {
        $val = $val.Substring(1, $val.Length - 2)
    }

    Set-Item -Path ("Env:\" + $key) -Value $val
}

Write-Host ("Loaded " + $envFile + " into this session.") -ForegroundColor Green
Write-Host "Now run:  cd ..\walmart_project ; dbt debug" -ForegroundColor Cyan

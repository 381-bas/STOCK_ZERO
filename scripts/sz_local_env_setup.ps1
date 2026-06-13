param(
    [switch]$DryRun,
    [switch]$CreateVenv,
    [switch]$RunImportSmoke,
    [switch]$Pretty
)

$ErrorActionPreference = "Stop"

function Write-JsonResult {
    param(
        [hashtable]$Payload,
        [int]$ExitCode
    )
    $depth = 8
    if ($Pretty) {
        $Payload | ConvertTo-Json -Depth $depth
    } else {
        $Payload | ConvertTo-Json -Depth $depth -Compress
    }
    exit $ExitCode
}

function Get-RepoRoot {
    $root = (& git rev-parse --show-toplevel 2>$null).Trim()
    if (-not $root) {
        throw "not_a_git_repository"
    }
    return $root
}

function Get-Python312 {
    $candidates = @()
    $py312 = & py -3.12 -c "import sys; print(sys.executable)" 2>$null
    if ($LASTEXITCODE -eq 0 -and $py312) {
        $candidates += $py312.Trim()
    }
    $python = & python -c "import sys; print(sys.executable if sys.version_info[:2] == (3, 12) else '')" 2>$null
    if ($LASTEXITCODE -eq 0 -and $python) {
        $trimmed = $python.Trim()
        if ($trimmed) { $candidates += $trimmed }
    }
    return @($candidates | Select-Object -Unique)
}

try {
    $repoRoot = Get-RepoRoot
    $venvPath = Join-Path $repoRoot ".venv"
    $requirementsPath = Join-Path $repoRoot "requirements.txt"
    $python312 = @(Get-Python312)
    $effectiveDryRun = $true
    if ($CreateVenv -and -not $DryRun) {
        $effectiveDryRun = $false
    }

    $requiredImports = @(
        "streamlit",
        "pandas",
        "dotenv",
        "sqlalchemy",
        "psycopg2",
        "openpyxl",
        "reportlab",
        "plotly"
    )

    $actions = @()
    $errors = @()
    $venvCreated = $false
    if ($CreateVenv) {
        $actions += "create_venv_under_worktree"
        $resolvedRoot = [System.IO.Path]::GetFullPath($repoRoot)
        $resolvedVenv = [System.IO.Path]::GetFullPath($venvPath)
        if (-not $resolvedVenv.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
            $errors += "venv_path_outside_repo"
        } elseif (-not $effectiveDryRun) {
            if ($python312.Count -eq 0) {
                $errors += "python_3_12_not_found"
            } elseif (-not (Test-Path -LiteralPath $venvPath)) {
                & $python312[0] -m venv $venvPath
                if ($LASTEXITCODE -ne 0) {
                    $errors += "venv_create_failed"
                } else {
                    $venvCreated = $true
                }
            }
        }
    }

    $importSmoke = [ordered]@{}
    if ($RunImportSmoke) {
        $actions += "run_import_smoke"
        $pythonForSmoke = $null
        $venvPython = Join-Path $venvPath "Scripts\python.exe"
        if (Test-Path -LiteralPath $venvPython) {
            $pythonForSmoke = $venvPython
        } elseif ($python312.Count -gt 0) {
            $pythonForSmoke = $python312[0]
        }

        if (-not $pythonForSmoke) {
            $errors += "python_for_import_smoke_not_found"
        } else {
            foreach ($name in $requiredImports) {
                & $pythonForSmoke -c "import $name" 2>$null
                $importSmoke[$name] = ($LASTEXITCODE -eq 0)
            }
        }
    }

    $payload = [ordered]@{
        ok = ($errors.Count -eq 0)
        dry_run = $effectiveDryRun
        repo_root = $repoRoot
        powershell = [ordered]@{
            edition = $PSVersionTable.PSEdition
            version = $PSVersionTable.PSVersion.ToString()
        }
        python_3_12_found = ($python312.Count -gt 0)
        python_3_12_candidates_count = $python312.Count
        requirements_txt_exists = Test-Path -LiteralPath $requirementsPath
        venv_exists = Test-Path -LiteralPath $venvPath
        venv_path_relative = ".venv"
        venv_created = $venvCreated
        create_venv_requested = [bool]$CreateVenv
        import_smoke_requested = [bool]$RunImportSmoke
        required_imports = $requiredImports
        import_smoke = $importSmoke
        actions = $actions
        installs_performed = $false
        db_access = "none"
        docker_executed = $false
        data_copy = $false
        git_modified = $false
        errors = $errors
    }

    if ($errors.Count -gt 0) {
        Write-JsonResult $payload 1
    }
    Write-JsonResult $payload 0
} catch {
    Write-JsonResult @{ ok = $false; error = "setup_failed"; error_type = $_.Exception.GetType().Name } 2
}

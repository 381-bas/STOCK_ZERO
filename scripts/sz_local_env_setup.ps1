param(
    [switch]$DryRun,
    [switch]$CreateVenv,
    [switch]$RunImportSmoke,
    [switch]$AllowSystemPythonSmoke,
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

function Test-PythonImport {
    param(
        [string]$PythonExe,
        [string]$ImportName
    )
    $oldErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $PythonExe -c "import $ImportName" *> $null
        return ($LASTEXITCODE -eq 0)
    } finally {
        $ErrorActionPreference = $oldErrorActionPreference
    }
}

try {
    $repoRoot = Get-RepoRoot
    $repoName = Split-Path -Leaf $repoRoot
    $venvPath = Join-Path $repoRoot ".venv"
    $venvPython = Join-Path $venvPath "Scripts\python.exe"
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
    $warnings = @()
    $venvCreated = $false
    $environmentSource = "NONE"
    $environmentReproducible = $false

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
        if (Test-Path -LiteralPath $venvPython) {
            $pythonForSmoke = $venvPython
            $environmentSource = "WORKTREE_VENV"
            $environmentReproducible = $true
        } elseif ($AllowSystemPythonSmoke -and $python312.Count -gt 0) {
            $pythonForSmoke = $python312[0]
            $environmentSource = "SYSTEM_PYTHON"
            $environmentReproducible = $false
            $warnings += "system_python_not_worktree_reproducible"
        } else {
            $errors += "worktree_venv_required"
        }

        if ($pythonForSmoke) {
            foreach ($name in $requiredImports) {
                $importSmoke[$name] = Test-PythonImport -PythonExe $pythonForSmoke -ImportName $name
            }
        }
    }

    $payload = [ordered]@{
        ok = ($errors.Count -eq 0)
        dry_run = $effectiveDryRun
        repo_name = $repoName
        path_redacted = $true
        root_relative = "."
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
        allow_system_python_smoke = [bool]$AllowSystemPythonSmoke
        environment_source = $environmentSource
        environment_reproducible = $environmentReproducible
        required_imports = $requiredImports
        import_smoke = $importSmoke
        actions = $actions
        installs_performed = $false
        db_access = "none"
        docker_executed = $false
        data_copy = $false
        kernels_copied = $false
        git_modified = $false
        warnings = $warnings
        errors = $errors
    }

    if ($errors.Count -gt 0) {
        Write-JsonResult $payload 1
    }
    Write-JsonResult $payload 0
} catch {
    Write-JsonResult @{
        ok = $false
        error = "setup_failed"
        error_type = $_.Exception.GetType().Name
        errors = @("setup_failed")
    } 2
}

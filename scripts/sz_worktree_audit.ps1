param(
    [string]$ExpectedBaseline = "",
    [switch]$RequireClean,
    [switch]$Pretty
)

$ErrorActionPreference = "Stop"
$ExpectedBranch = "codex/PLATFORM_005B-load-observation-correction"

function Invoke-GitLines {
    param([string[]]$GitArgs)
    $output = & git @GitArgs 2>$null
    if ($null -eq $output) { return @() }
    return @($output)
}

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

function As-Array {
    param($Value)
    $list = New-Object "System.Collections.Generic.List[string]"
    if ($null -ne $Value) {
        foreach ($item in @($Value)) {
            if ($null -ne $item) {
                [void]$list.Add([string]$item)
            }
        }
    }
    return ,$list
}

try {
    $repoRoot = (& git rev-parse --show-toplevel 2>$null).Trim()
    if (-not $repoRoot) {
        Write-JsonResult @{ ok = $false; error = "not_a_git_repository" } 2
    }

    $branch = (& git rev-parse --abbrev-ref HEAD).Trim()
    $head = (& git rev-parse HEAD).Trim()
    $statusShort = As-Array (Invoke-GitLines -GitArgs @("status", "--short"))
    $stagedFiles = As-Array (Invoke-GitLines -GitArgs @("diff", "--cached", "--name-only"))
    $modifiedFiles = As-Array (Invoke-GitLines -GitArgs @("ls-files", "-m"))
    $untrackedFiles = As-Array (Invoke-GitLines -GitArgs @("ls-files", "--others", "--exclude-standard"))
    $worktrees = As-Array (Invoke-GitLines -GitArgs @("worktree", "list", "--porcelain"))

    $presence = [ordered]@{
        ".venv" = Test-Path -LiteralPath (Join-Path $repoRoot ".venv")
        ".env" = Test-Path -LiteralPath (Join-Path $repoRoot ".env")
        ".local_secrets" = Test-Path -LiteralPath (Join-Path $repoRoot ".local_secrets")
        "data" = Test-Path -LiteralPath (Join-Path $repoRoot "data")
        "evidence" = Test-Path -LiteralPath (Join-Path $repoRoot "evidence")
    }

    $errors = @()
    if ($branch -ne $ExpectedBranch) {
        $errors += "unexpected_branch"
    }
    if ($ExpectedBaseline -and $head -ne $ExpectedBaseline) {
        $errors += "baseline_mismatch"
    }
    if ($RequireClean -and $statusShort.Count -gt 0) {
        $errors += "worktree_not_clean"
    }

    $payload = [ordered]@{
        ok = ($errors.Count -eq 0)
        repo_root = $repoRoot
        branch = $branch
        expected_branch = $ExpectedBranch
        head = $head
        expected_baseline = $ExpectedBaseline
        require_clean = [bool]$RequireClean
        status_short = $statusShort
        staged_files = $stagedFiles
        modified_files = $modifiedFiles
        untracked_files = $untrackedFiles
        worktree_list = $worktrees
        presence = $presence
        errors = $errors
    }

    if ($errors.Count -gt 0) {
        Write-JsonResult $payload 1
    }
    Write-JsonResult $payload 0
} catch {
    Write-JsonResult @{ ok = $false; error = "audit_failed"; error_type = $_.Exception.GetType().Name } 2
}

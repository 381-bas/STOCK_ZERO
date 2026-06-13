param(
    [string]$ExpectedBranch = "",
    [string]$ExpectedBaseline = "",
    [switch]$AllowDetached,
    [switch]$RequireClean,
    [switch]$IncludeAbsolutePaths,
    [switch]$Pretty
)

$ErrorActionPreference = "Stop"

function Write-JsonResult {
    param(
        [hashtable]$Payload,
        [int]$ExitCode
    )
    $depth = 10
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

function Invoke-GitSafe {
    param(
        [string]$Name,
        [string[]]$GitArgs
    )
    $stderrPath = [System.IO.Path]::GetTempFileName()
    try {
        $oldErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $output = & git @GitArgs 2>$stderrPath
        $exitCode = $LASTEXITCODE
        $ErrorActionPreference = $oldErrorActionPreference
        $result = [ordered]@{
            name = $Name
            success = ($exitCode -eq 0)
            exit_code = $exitCode
            error_category = $(if ($exitCode -eq 0) { "none" } else { "git_command_failed" })
        }
        return [ordered]@{
            result = $result
            lines = As-Array $output
            text = [string]::Join("`n", @(As-Array $output))
        }
    } finally {
        if ($oldErrorActionPreference) {
            $ErrorActionPreference = $oldErrorActionPreference
        }
        Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Redact-WorktreeLines {
    param([System.Collections.Generic.List[string]]$Lines)
    $redacted = New-Object "System.Collections.Generic.List[string]"
    foreach ($line in $Lines) {
        if ($line.StartsWith("worktree ")) {
            [void]$redacted.Add("worktree <redacted>")
        } else {
            [void]$redacted.Add($line)
        }
    }
    return ,$redacted
}

function Infer-RepoRole {
    param(
        [string]$RepoName,
        [string]$Branch,
        [string]$BranchMode
    )
    if ($BranchMode -eq "DETACHED") { return "detached_review_worktree" }
    if ($Branch -eq "main" -and $RepoName -eq "STOCK_ZERO") { return "primary_checkout" }
    if ($Branch -like "codex/*") { return "implementation_worktree" }
    return "unknown"
}

try {
    $gitResults = New-Object "System.Collections.Generic.List[object]"
    $errors = New-Object "System.Collections.Generic.List[string]"

    $repoRootCall = Invoke-GitSafe -Name "rev_parse_toplevel" -GitArgs @("rev-parse", "--show-toplevel")
    [void]$gitResults.Add($repoRootCall.result)
    if (-not $repoRootCall.result.success -or -not $repoRootCall.text.Trim()) {
        $payload = [ordered]@{
            ok = $false
            repo_name = $null
            repo_role = "unknown"
            path_redacted = $true
            root_relative = "."
            git_results = $gitResults
            errors = @("not_a_git_repository")
        }
        Write-JsonResult $payload 2
    }

    $repoRoot = $repoRootCall.text.Trim()
    $repoName = Split-Path -Leaf $repoRoot

    $branchCall = Invoke-GitSafe -Name "rev_parse_branch" -GitArgs @("rev-parse", "--abbrev-ref", "HEAD")
    $headCall = Invoke-GitSafe -Name "rev_parse_head" -GitArgs @("rev-parse", "HEAD")
    $statusCall = Invoke-GitSafe -Name "status_short" -GitArgs @("status", "--short")
    $stagedCall = Invoke-GitSafe -Name "diff_cached_name_only" -GitArgs @("diff", "--cached", "--name-only")
    $modifiedCall = Invoke-GitSafe -Name "ls_files_modified" -GitArgs @("ls-files", "-m")
    $untrackedCall = Invoke-GitSafe -Name "ls_files_untracked" -GitArgs @("ls-files", "--others", "--exclude-standard")
    $worktreesCall = Invoke-GitSafe -Name "worktree_list_porcelain" -GitArgs @("worktree", "list", "--porcelain")

    foreach ($call in @($branchCall, $headCall, $statusCall, $stagedCall, $modifiedCall, $untrackedCall, $worktreesCall)) {
        [void]$gitResults.Add($call.result)
        if (-not $call.result.success) {
            [void]$errors.Add($call.result.name + "_failed")
        }
    }

    $branchRaw = $branchCall.text.Trim()
    $branchMode = $(if ($branchRaw -eq "HEAD") { "DETACHED" } else { "BRANCH" })
    $branch = $(if ($branchMode -eq "DETACHED") { $null } else { $branchRaw })
    $head = $headCall.text.Trim()

    if ($branchMode -eq "DETACHED" -and -not $AllowDetached) {
        [void]$errors.Add("detached_not_allowed")
    }
    if ($branchMode -eq "BRANCH" -and $ExpectedBranch -and $branch -ne $ExpectedBranch) {
        [void]$errors.Add("branch_mismatch")
    }
    if ($ExpectedBaseline -and $head -ne $ExpectedBaseline) {
        [void]$errors.Add("baseline_mismatch")
    }
    if ($RequireClean -and $statusCall.lines.Count -gt 0) {
        [void]$errors.Add("worktree_not_clean")
    }

    $presence = [ordered]@{
        ".venv" = Test-Path -LiteralPath (Join-Path $repoRoot ".venv")
        ".env" = Test-Path -LiteralPath (Join-Path $repoRoot ".env")
        ".local_secrets" = Test-Path -LiteralPath (Join-Path $repoRoot ".local_secrets")
        "data" = Test-Path -LiteralPath (Join-Path $repoRoot "data")
        "evidence" = Test-Path -LiteralPath (Join-Path $repoRoot "evidence")
        "kernels" = $false
    }

    $payload = [ordered]@{
        ok = ($errors.Count -eq 0)
        repo_name = $repoName
        repo_role = Infer-RepoRole -RepoName $repoName -Branch $branchRaw -BranchMode $branchMode
        path_redacted = (-not [bool]$IncludeAbsolutePaths)
        root_relative = "."
        branch_mode = $branchMode
        branch = $branch
        expected_branch = $(if ($ExpectedBranch) { $ExpectedBranch } else { $null })
        head = $head
        expected_baseline = $(if ($ExpectedBaseline) { $ExpectedBaseline } else { $null })
        allow_detached = [bool]$AllowDetached
        require_clean = [bool]$RequireClean
        status_short = $statusCall.lines
        staged_files = $stagedCall.lines
        modified_files = $modifiedCall.lines
        untracked_files = $untrackedCall.lines
        worktree_list = $(if ($IncludeAbsolutePaths) { $worktreesCall.lines } else { Redact-WorktreeLines -Lines $worktreesCall.lines })
        presence = $presence
        git_results = $gitResults
        errors = $errors
    }

    if ($IncludeAbsolutePaths) {
        $payload["repo_root"] = $repoRoot
    }

    if ($errors.Count -gt 0) {
        Write-JsonResult $payload 1
    }
    Write-JsonResult $payload 0
} catch {
    Write-JsonResult @{
        ok = $false
        repo_name = $null
        repo_role = "unknown"
        path_redacted = $true
        root_relative = "."
        error = "audit_failed"
        error_type = $_.Exception.GetType().Name
        errors = @("audit_failed")
    } 2
}

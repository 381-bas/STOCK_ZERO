#Requires -Version 7.2

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet(
        'bootstrap',
        'inventory',
        'set-readonly',
        'set-admin-temporary',
        'generate-role-password-temporary',
        'build-and-store-route-b-dsn',
        'remove-temporary'
    )]
    [string]$Operation,

    [string]$EvidenceDirectory,
    [string]$RunId,
    [string]$ExpectedGitSha
)

$ErrorActionPreference = 'Stop'
$vaultName = 'STOCK_ZERO'
$secretManagementVersion = '1.1.2'
$secretStoreVersion = '1.0.6'
$secretNames = @{
    Readonly = 'STOCK_ZERO_DB_CODEX_RO'
    Admin = 'STOCK_ZERO_DB_ADMIN'
    ProductivePassword = 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD'
    ProductiveDsn = 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE'
}

function Import-StockZeroSecretModules {
    Import-Module Microsoft.PowerShell.SecretManagement -RequiredVersion $secretManagementVersion -ErrorAction Stop
    Import-Module Microsoft.PowerShell.SecretStore -RequiredVersion $secretStoreVersion -ErrorAction Stop
}

function Assert-StockZeroVault {
    $vault = Get-SecretVault -Name $vaultName -ErrorAction Stop
    if ($vault.Name -ne $vaultName) {
        throw 'STOCK_ZERO vault is not registered.'
    }
}

function Set-StockZeroInteractiveSecret {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Prompt
    )
    $value = Read-Host -Prompt $Prompt -AsSecureString
    if ($null -eq $value -or $value.Length -eq 0) {
        throw 'An empty secret is not allowed.'
    }
    Set-Secret -Vault $vaultName -Name $Name -Secret $value -ErrorAction Stop
    $value = $null
}

function ConvertFrom-StockZeroSecureString {
    param([Parameter(Mandatory)][System.Security.SecureString]$Value)
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Get-StockZeroGovernedEvidence {
    if ($RunId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') {
        throw 'The evidence run id must be a canonical lowercase UUID v4.'
    }
    if ([string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
        throw 'The canonical evidence directory is required.'
    }
    if ($ExpectedGitSha -cnotmatch '^[0-9a-f]{40}$') {
        throw 'The expected Git SHA must contain exactly 40 lowercase hexadecimal characters.'
    }

    $repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
    $expectedDirectory = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot (Join-Path 'evidence/runtime/020B' $RunId))
    )
    $candidateDirectory = if ([IO.Path]::IsPathRooted($EvidenceDirectory)) {
        [IO.Path]::GetFullPath($EvidenceDirectory)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repositoryRoot $EvidenceDirectory))
    }
    $resolvedDirectory = (Resolve-Path -LiteralPath $candidateDirectory -ErrorAction Stop).Path
    if ($resolvedDirectory -ne $expectedDirectory) {
        throw 'The evidence directory is not canonical for the supplied run id.'
    }

    $contract = [ordered]@{
        readonly_baseline_precheck = @{
            File = '01_readonly_baseline.json'
            Type = 'kpione_route_b_readonly_baseline_evidence_v1'
            Verdict = 'PASS_READONLY_BASELINE'
            Step = 1
        }
        admin_provisioning = @{
            File = '02_admin_provisioning.json'
            Type = 'kpione_route_b_role_provisioning_evidence_v1'
            Verdict = 'PASS_ADMIN_PROVISIONING'
            Step = 2
        }
        productive_role_verification = @{
            File = '03_productive_role_verification.json'
            Type = 'kpione_route_b_productive_role_verification_evidence_v1'
            Verdict = 'PASS_PRODUCTIVE_ROLE_VERIFICATION'
            Step = 3
        }
        readonly_postcheck = @{
            File = '04_readonly_postcheck.json'
            Type = 'kpione_route_b_readonly_postcheck_evidence_v1'
            Verdict = 'PASS_READONLY_POSTCHECK'
            Step = 4
        }
    }
    $bundleName = '05_infrastructure_bundle.json'
    $expectedNames = @($contract.Values.File) + @($bundleName)
    $actualNames = @(Get-ChildItem -LiteralPath $resolvedDirectory -File | ForEach-Object { $_.Name })
    if (@(Compare-Object -ReferenceObject $expectedNames -DifferenceObject $actualNames).Count -ne 0) {
        throw 'The evidence directory must contain exactly the five canonical files.'
    }

    $bundlePath = Join-Path $resolvedDirectory $bundleName
    $bundle = Get-Content -LiteralPath $bundlePath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($bundle.document_type -ne 'kpione_route_b_infrastructure_evidence_bundle_v1' -or
        $bundle.status -ne 'PASSED' -or $bundle.run_id -cne $RunId) {
        throw 'The infrastructure bundle is not approved for this run.'
    }

    foreach ($componentName in $contract.Keys) {
        $item = $contract[$componentName]
        $path = Join-Path $resolvedDirectory $item.File
        $evidence = Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($evidence.document_type -ne $item.Type -or
            $evidence.verdict -ne $item.Verdict -or
            $evidence.evidence_sequence_step -ne $item.Step -or
            $evidence.run_id -cne $RunId) {
            throw "Evidence component is not approved: $componentName."
        }
        $expectedHash = $bundle.components.PSObject.Properties[$componentName].Value
        $actualHash = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($expectedHash -cne $actualHash) {
            throw "Evidence component hash mismatch: $componentName."
        }
    }
    return $bundle
}

function Assert-ExistingBundleSemanticValidation {
    param([Parameter(Mandatory)]$StoredBundle)

    $repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
    $validatorPath = (Resolve-Path (
        Join-Path $repositoryRoot 'scripts/build_kpione_route_b_infrastructure_evidence.py'
    ) -ErrorAction Stop).Path
    $planPath = (Resolve-Path (
        Join-Path $repositoryRoot 'plans/018_kpione_route_b_productive_apply_plan.json'
    ) -ErrorAction Stop).Path
    $bundlePath = Join-Path $repositoryRoot (
        Join-Path (Join-Path 'evidence/runtime/020B' $RunId) '05_infrastructure_bundle.json'
    )
    $python = (Get-Command python -ErrorAction Stop).Source
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $python
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @(
        $validatorPath,
        '--validate-existing',
        '--run-id', $RunId,
        '--plan', $planPath,
        '--expected-git-sha', $ExpectedGitSha,
        '--output', $bundlePath
    )) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw 'Existing bundle validator failed to start.'
        }
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        if ($process.ExitCode -ne 0) {
            throw 'Existing infrastructure bundle semantic validation failed.'
        }
        try {
            $result = $stdout | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            throw 'Existing bundle validator returned invalid JSON.'
        }
        if ($result.verdict -ne 'PASS_EXISTING_INFRASTRUCTURE_BUNDLE_VALIDATION' -or
            $result.run_id -cne $RunId -or
            $result.bundle_sha256 -cne $StoredBundle.bundle_sha256) {
            throw 'Existing bundle validator result did not match the governed evidence.'
        }
    }
    finally {
        $stdout = [string]::Empty
        $stderr = [string]::Empty
        $process.Dispose()
    }
}

if ($Operation -eq 'bootstrap') {
    Install-Module Microsoft.PowerShell.SecretManagement -RequiredVersion $secretManagementVersion `
        -Scope CurrentUser -Repository PSGallery -Force -AllowClobber -ErrorAction Stop
    Install-Module Microsoft.PowerShell.SecretStore -RequiredVersion $secretStoreVersion `
        -Scope CurrentUser -Repository PSGallery -Force -AllowClobber -ErrorAction Stop
    Import-StockZeroSecretModules
    $existing = Get-SecretVault -Name $vaultName -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        Register-SecretVault -Name $vaultName -ModuleName Microsoft.PowerShell.SecretStore `
            -DefaultVault -ErrorAction Stop
    }
    Set-SecretStoreConfiguration -Authentication Password -Interaction Prompt `
        -PasswordTimeout 900 -Confirm:$false -ErrorAction Stop
    return
}

Import-StockZeroSecretModules
Assert-StockZeroVault

switch ($Operation) {
    'inventory' {
        $inventory = @(
            foreach ($name in $secretNames.Values | Sort-Object) {
                $info = Get-SecretInfo -Vault $vaultName -Name $name -ErrorAction SilentlyContinue
                [pscustomobject]@{
                    vault = $vaultName
                    name = $name
                    type = if ($null -eq $info) { $null } else { [string]$info.Type }
                    present = $null -ne $info
                }
            }
        )
        [Console]::Out.WriteLine(($inventory | ConvertTo-Json -Compress))
    }
    'set-readonly' {
        Set-StockZeroInteractiveSecret -Name $secretNames.Readonly -Prompt 'Enter the read-only DSN'
    }
    'set-admin-temporary' {
        Set-StockZeroInteractiveSecret -Name $secretNames.Admin -Prompt 'Enter the temporary admin DSN'
    }
    'generate-role-password-temporary' {
        $bytes = [byte[]]::new(32)
        [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
        $plain = [Convert]::ToBase64String($bytes)
        $secure = ConvertTo-SecureString -String $plain -AsPlainText -Force
        Set-Secret -Vault $vaultName -Name $secretNames.ProductivePassword -Secret $secure -ErrorAction Stop
        [Array]::Clear($bytes, 0, $bytes.Length)
        $plain = [string]::Empty
        $secure = $null
    }
    'build-and-store-route-b-dsn' {
        $securePassword = Get-Secret -Vault $vaultName -Name $secretNames.ProductivePassword -ErrorAction Stop
        if ($securePassword -isnot [System.Security.SecureString]) {
            throw 'The productive role password must be stored as SecureString.'
        }
        $plainPassword = ConvertFrom-StockZeroSecureString -Value $securePassword
        try {
            $encodedPassword = [Uri]::EscapeDataString($plainPassword)
            $dsn = 'postgresql://stock_zero_kpione_route_b_load:' + $encodedPassword + `
                '@db.xheyrgfagpoigpgakilu.supabase.co/postgres?sslmode=require'
            $secureDsn = ConvertTo-SecureString -String $dsn -AsPlainText -Force
            Set-Secret -Vault $vaultName -Name $secretNames.ProductiveDsn -Secret $secureDsn -ErrorAction Stop
        }
        finally {
            $plainPassword = [string]::Empty
            $encodedPassword = [string]::Empty
            $dsn = [string]::Empty
            $secureDsn = $null
            $securePassword = $null
        }
    }
    'remove-temporary' {
        $storedBundle = Get-StockZeroGovernedEvidence
        Assert-ExistingBundleSemanticValidation -StoredBundle $storedBundle
        $productiveDsn = Get-SecretInfo -Vault $vaultName -Name $secretNames.ProductiveDsn -ErrorAction SilentlyContinue
        if ($null -eq $productiveDsn) {
            throw 'productive_dsn_evidence_dependency_missing'
        }
        Remove-Secret -Vault $vaultName -Name $secretNames.Admin -ErrorAction SilentlyContinue
        Remove-Secret -Vault $vaultName -Name $secretNames.ProductivePassword -ErrorAction SilentlyContinue
    }
}

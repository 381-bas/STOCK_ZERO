#Requires -Version 7.2
#Requires -Modules Microsoft.PowerShell.SecretManagement, Microsoft.PowerShell.SecretStore

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet(
        'readonly-precheck',
        'readonly-postcheck',
        'verify-route-b-role',
        'route-b-apply',
        'route-b-rollback',
        'admin-provision',
        'admin-reconcile-provisioning-evidence',
        'diagnose-readonly',
        'diagnose-route-b',
        'diagnose-admin'
    )]
    [string]$Operation,

    [string[]]$ArgumentList = @()
)

$ErrorActionPreference = 'Stop'
$vaultName = 'STOCK_ZERO'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$python = (Get-Command python -ErrorAction Stop).Source
$managedEnvironmentNames = @(
    'DB_URL_CODEX_RO',
    'DB_URL_KPIONE_ROUTE_B_PRODUCTIVE',
    'DB_URL_ADMIN',
    'KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD',
    'DB_URL_LOAD',
    'DB_URL_CODEX_LOCAL'
)
$profileMap = @{
    'readonly' = @(
        @{ SecretName = 'STOCK_ZERO_DB_CODEX_RO'; EnvironmentName = 'DB_URL_CODEX_RO' }
    )
    'route-b-productive' = @(
        @{ SecretName = 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PRODUCTIVE'; EnvironmentName = 'DB_URL_KPIONE_ROUTE_B_PRODUCTIVE' }
    )
    'admin-provisioning' = @(
        @{ SecretName = 'STOCK_ZERO_DB_ADMIN'; EnvironmentName = 'DB_URL_ADMIN' },
        @{ SecretName = 'STOCK_ZERO_DB_KPIONE_ROUTE_B_PASSWORD'; EnvironmentName = 'KPIONE_ROUTE_B_PRODUCTIVE_PASSWORD' }
    )
    'admin-reconciliation' = @(
        @{ SecretName = 'STOCK_ZERO_DB_ADMIN'; EnvironmentName = 'DB_URL_ADMIN' }
    )
}
$operationMap = @{
    'readonly-precheck' = @{
        Script = 'scripts/precheck_kpione_route_b_018_read_only.py'
        Profile = 'readonly'
        PrefixArguments = @('--check-stage', 'baseline')
    }
    'readonly-postcheck' = @{
        Script = 'scripts/precheck_kpione_route_b_018_read_only.py'
        Profile = 'readonly'
        PrefixArguments = @('--check-stage', 'post-provision')
    }
    'verify-route-b-role' = @{
        Script = 'scripts/verify_kpione_route_b_productive_role.py'
        Profile = 'route-b-productive'
        PrefixArguments = @()
    }
    'route-b-apply' = @{
        Script = 'scripts/run_kpione_route_b_ingestion_v1.py'
        Profile = 'route-b-productive'
        PrefixArguments = @('--apply-productive')
    }
    'route-b-rollback' = @{
        Script = 'scripts/run_kpione_route_b_ingestion_v1.py'
        Profile = 'route-b-productive'
        PrefixArguments = @('--rollback-productive')
    }
    'admin-provision' = @{
        Script = 'scripts/provision_kpione_route_b_role.py'
        Profile = 'admin-provisioning'
        PrefixArguments = @()
        AuthorityPrecheck = $true
    }
    'admin-reconcile-provisioning-evidence' = @{
        Script = 'scripts/provision_kpione_route_b_role.py'
        Profile = 'admin-reconciliation'
        PrefixArguments = @('--reconcile-provisioning-evidence')
        AuthorityPrecheck = $true
    }
    'diagnose-readonly' = @{
        Script = 'scripts/diagnose_stock_zero_db_credentials.py'
        Profile = 'readonly'
        PrefixArguments = @('--credential-class', 'readonly')
    }
    'diagnose-route-b' = @{
        Script = 'scripts/diagnose_stock_zero_db_credentials.py'
        Profile = 'route-b-productive'
        PrefixArguments = @('--credential-class', 'route-b-productive')
    }
    'diagnose-admin' = @{
        Script = 'scripts/diagnose_stock_zero_db_credentials.py'
        Profile = 'admin-provisioning'
        PrefixArguments = @('--credential-class', 'admin-provisioning')
    }
}

function ConvertFrom-StockZeroSecureValue {
    param([Parameter(Mandatory)]$Value)

    if ($Value -is [string]) {
        return $Value
    }
    if ($Value -isnot [System.Security.SecureString]) {
        throw 'Unsupported secret type. Store STOCK_ZERO database secrets as String or SecureString.'
    }
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function New-StockZeroStartInfo {
    param(
        [Parameter(Mandatory)][string]$Script,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$Arguments
    )

    $scriptPath = (Resolve-Path (Join-Path $repositoryRoot $Script)).Path
    $expectedPath = [IO.Path]::GetFullPath((Join-Path $repositoryRoot $Script))
    if ($scriptPath -ne $expectedPath) {
        throw 'Resolved operation entrypoint differs from the approved repository path.'
    }
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $python
    $startInfo.UseShellExecute = $false
    [void]$startInfo.ArgumentList.Add($scriptPath)
    foreach ($argument in $Arguments) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
    foreach ($name in $managedEnvironmentNames) {
        [void]$startInfo.Environment.Remove($name)
    }
    return $startInfo
}

$entrypoint = $operationMap[$Operation]
$operationArguments = @($entrypoint.PrefixArguments) + @($ArgumentList)

$evidenceFileByOperation = @{
    'readonly-precheck' = '01_readonly_baseline.json'
    'admin-provision' = '02_admin_provisioning.json'
    'admin-reconcile-provisioning-evidence' = '02_admin_provisioning.json'
    'verify-route-b-role' = '03_productive_role_verification.json'
    'readonly-postcheck' = '04_readonly_postcheck.json'
}
if ($evidenceFileByOperation.ContainsKey($Operation)) {
    $runIndex = [Array]::IndexOf($operationArguments, '--run-id')
    if ($runIndex -lt 0 -or $runIndex + 1 -ge $operationArguments.Count) {
        throw 'A canonical --run-id is required for evidence operations.'
    }
    $runId = $operationArguments[$runIndex + 1]
    if ($runId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') {
        throw 'The evidence run id must be a canonical lowercase UUID v4.'
    }
    $runDirectory = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot (Join-Path 'evidence/runtime/020B' $runId))
    )
    [void](New-Item -ItemType Directory -Path $runDirectory -Force)
    $outputSwitch = if ($Operation -in @('readonly-precheck', 'readonly-postcheck')) {
        '--report-json'
    } else {
        '--evidence-json'
    }
    $outputIndex = [Array]::IndexOf($operationArguments, $outputSwitch)
    if ($outputIndex -lt 0 -or $outputIndex + 1 -ge $operationArguments.Count) {
        throw "The canonical $outputSwitch argument is required."
    }
    $actualOutput = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot $operationArguments[$outputIndex + 1])
    )
    $expectedOutput = [IO.Path]::GetFullPath(
        (Join-Path $runDirectory $evidenceFileByOperation[$Operation])
    )
    if ($actualOutput -ne $expectedOutput) {
        throw 'The evidence output path is not canonical for this operation.'
    }
    if (Test-Path -LiteralPath $expectedOutput) {
        throw 'The evidence output already exists.'
    }
}

# Admin authority is checked in a secret-free child before the vault is opened.
if ($entrypoint.AuthorityPrecheck) {
    $precheckInfo = New-StockZeroStartInfo `
        -Script $entrypoint.Script `
        -Arguments ($operationArguments + @('--authority-precheck-only'))
    $precheck = [System.Diagnostics.Process]::Start($precheckInfo)
    $precheck.WaitForExit()
    if ($precheck.ExitCode -ne 0) {
        exit $precheck.ExitCode
    }
}

$vault = Get-SecretVault -Name $vaultName -ErrorAction Stop
if ($vault.Name -ne $vaultName) {
    throw 'STOCK_ZERO vault is not registered.'
}

$startInfo = New-StockZeroStartInfo -Script $entrypoint.Script -Arguments $operationArguments
$injectedNames = [System.Collections.Generic.List[string]]::new()
$plainValues = [System.Collections.Generic.List[string]]::new()
try {
    foreach ($mapping in $profileMap[$entrypoint.Profile]) {
        $secret = Get-Secret -Vault $vaultName -Name $mapping.SecretName -ErrorAction Stop
        $plain = ConvertFrom-StockZeroSecureValue -Value $secret
        if ([string]::IsNullOrWhiteSpace($plain)) {
            throw "Required STOCK_ZERO secret is empty: $($mapping.SecretName)"
        }
        $startInfo.Environment[$mapping.EnvironmentName] = $plain
        $injectedNames.Add($mapping.EnvironmentName)
        $plainValues.Add($plain)
        $plain = $null
        $secret = $null
    }
    $process = [System.Diagnostics.Process]::Start($startInfo)
    $process.WaitForExit()
    exit $process.ExitCode
}
finally {
    foreach ($name in $injectedNames) {
        [void]$startInfo.Environment.Remove($name)
    }
    for ($index = 0; $index -lt $plainValues.Count; $index++) {
        $plainValues[$index] = [string]::Empty
    }
    $plainValues.Clear()
}

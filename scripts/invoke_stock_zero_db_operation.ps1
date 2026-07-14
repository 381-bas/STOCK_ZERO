#Requires -Version 7.2
#Requires -Modules Microsoft.PowerShell.SecretManagement, Microsoft.PowerShell.SecretStore

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet(
        'readonly-precheck',
        'route-b-apply',
        'route-b-rollback',
        'admin-provision',
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
}
$operationMap = @{
    'readonly-precheck' = @{
        Script = 'scripts/precheck_kpione_route_b_018_read_only.py'
        Profile = 'readonly'
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
        [Parameter(Mandatory)][string[]]$Arguments
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

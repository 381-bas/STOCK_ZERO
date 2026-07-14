#Requires -Version 7.2
#Requires -Modules Microsoft.PowerShell.SecretManagement, Microsoft.PowerShell.SecretStore

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('readonly', 'route-b-productive', 'admin-provisioning')]
    [string]$Profile,

    [Parameter(Mandatory)]
    [string]$FilePath,

    [string[]]$ArgumentList = @()
)

$ErrorActionPreference = 'Stop'
$vaultName = 'STOCK_ZERO'
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

$vault = Get-SecretVault -Name $vaultName -ErrorAction Stop
if ($vault.Name -ne $vaultName) {
    throw 'STOCK_ZERO vault is not registered.'
}

$startInfo = [System.Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $FilePath
$startInfo.UseShellExecute = $false
foreach ($argument in $ArgumentList) {
    [void]$startInfo.ArgumentList.Add($argument)
}

$injectedNames = [System.Collections.Generic.List[string]]::new()
$plainValues = [System.Collections.Generic.List[string]]::new()
try {
    foreach ($mapping in $profileMap[$Profile]) {
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

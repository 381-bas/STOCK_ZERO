#Requires -Version 5.1
#Requires -Modules Microsoft.PowerShell.SecretManagement, Microsoft.PowerShell.SecretStore

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet(
        'readonly-precheck',
        'readonly-postcheck',
        'readonly-reattest-route-b-june-apply',
        'verify-route-b-role',
        'route-b-apply',
        'route-b-rollback',
        'admin-provision',
        'admin-reconcile-provisioning-evidence',
        'admin-reconcile-existing-provisioned-state',
        'admin-reconcile-route-b-readonly-observer',
        'apply-route-b-app-bridge',
        'readonly-baseline-023',
        'provision-cg-mart-refresh-023',
        'apply-route-b-bridge-023',
        'dry-run-june-refresh-023',
        'apply-june-refresh-023',
        'readonly-postcheck-023',
        'validate-app-readonly-023',
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
$phase023EnvironmentNames = @(
    'DB_URL_CG_MART_REFRESH',
    'CG_MART_REFRESH_PASSWORD',
    'DB_URL_APP',
    'STOCK_ZERO_OPERATION_PROFILE',
    'STOCK_ZERO_OPERATION'
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
    'admin-ddl' = @(
        @{ SecretName = 'STOCK_ZERO_DB_ADMIN'; EnvironmentName = 'DB_URL_ADMIN' }
    )
    'cg-mart-refresh' = @(
        @{ SecretName = 'STOCK_ZERO_DB_CG_MART_REFRESH'; EnvironmentName = 'DB_URL_CG_MART_REFRESH' }
    )
    'cg-mart-refresh-provisioning' = @(
        @{ SecretName = 'STOCK_ZERO_DB_ADMIN'; EnvironmentName = 'DB_URL_ADMIN' },
        @{ SecretName = 'STOCK_ZERO_DB_CG_MART_REFRESH_PASSWORD'; EnvironmentName = 'CG_MART_REFRESH_PASSWORD' }
    )
    'app-readonly' = @(
        @{ SecretName = 'STOCK_ZERO_DB_APP_RO'; EnvironmentName = 'DB_URL_APP' }
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
    'readonly-reattest-route-b-june-apply' = @{
        Script = 'scripts/precheck_kpione_route_b_018_read_only.py'
        Profile = 'readonly'
        PrefixArguments = @('--check-stage', 'post-apply-reattestation')
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
    'admin-reconcile-existing-provisioned-state' = @{
        Script = 'scripts/provision_kpione_route_b_role.py'
        Profile = 'admin-reconciliation'
        PrefixArguments = @('--reconcile-existing-provisioned-state')
        AuthorityPrecheck = $true
    }
    'admin-reconcile-route-b-readonly-observer' = @{
        Script = 'scripts/reconcile_route_b_readonly_observer.py'
        Profile = 'admin-reconciliation'
        PrefixArguments = @()
        AuthorityPrecheck = $true
    }
    'apply-route-b-app-bridge' = @{
        Script = 'scripts/apply_control_gestion_route_b_bridge.py'
        Profile = 'admin-ddl'
        PrefixArguments = @()
    }
    'readonly-baseline-023' = @{
        Script = 'scripts/validate_control_gestion_route_b_023_read_only.py'
        Profile = 'readonly'
        PrefixArguments = @('--mode', 'baseline')
    }
    'provision-cg-mart-refresh-023' = @{
        Script = 'scripts/provision_control_gestion_mart_refresh_role.py'
        Profile = 'cg-mart-refresh-provisioning'
        PrefixArguments = @()
    }
    'apply-route-b-bridge-023' = @{
        Script = 'scripts/apply_control_gestion_route_b_bridge.py'
        Profile = 'admin-ddl'
        PrefixArguments = @()
    }
    'dry-run-june-refresh-023' = @{
        Script = 'scripts/refresh_control_gestion_v2_incremental.py'
        Profile = 'cg-mart-refresh'
        PrefixArguments = @('--dry-run', '--safety-window-weeks', '0')
    }
    'apply-june-refresh-023' = @{
        Script = 'scripts/refresh_control_gestion_v2_incremental.py'
        Profile = 'cg-mart-refresh'
        PrefixArguments = @('--apply', '--safety-window-weeks', '0')
    }
    'readonly-postcheck-023' = @{
        Script = 'scripts/validate_control_gestion_route_b_023_read_only.py'
        Profile = 'readonly'
        PrefixArguments = @('--mode', 'postcheck')
    }
    'validate-app-readonly-023' = @{
        Script = 'scripts/validate_control_gestion_route_b_023_read_only.py'
        Profile = 'app-readonly'
        PrefixArguments = @('--mode', 'app')
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
    $allArguments = @($scriptPath) + @($Arguments)
    if ($null -ne $startInfo.PSObject.Properties['ArgumentList']) {
        foreach ($argument in $allArguments) {
            [void]$startInfo.ArgumentList.Add($argument)
        }
    }
    else {
        # Windows PowerShell 5.1 uses the CommandLineToArgvW-compatible
        # legacy Arguments string. Preserve empty values and quote/backslash runs.
        $quotedArguments = @()
        foreach ($argument in $allArguments) {
            $quotedArguments += ConvertTo-StockZeroWindowsArgument -Value $argument
        }
        $startInfo.Arguments = $quotedArguments -join ' '
    }
    foreach ($name in $managedEnvironmentNames) {
        [void]$startInfo.Environment.Remove($name)
    }
    foreach ($name in $phase023EnvironmentNames) {
        [void]$startInfo.EnvironmentVariables.Remove($name)
    }
    return $startInfo
}

function ConvertTo-StockZeroWindowsArgument {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Value)

    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
        return $Value
    }
    $builder = [System.Text.StringBuilder]::new()
    [void]$builder.Append('"')
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq [char]92) {
            $backslashes++
            continue
        }
        if ($character -eq [char]34) {
            if ($backslashes -gt 0) {
                [void]$builder.Append(('\' * (2 * $backslashes)))
            }
            [void]$builder.Append('\"')
            $backslashes = 0
            continue
        }
        if ($backslashes -gt 0) {
            [void]$builder.Append(('\' * $backslashes))
            $backslashes = 0
        }
        [void]$builder.Append($character)
    }
    if ($backslashes -gt 0) {
        [void]$builder.Append(('\' * (2 * $backslashes)))
    }
    [void]$builder.Append('"')
    return $builder.ToString()
}

$entrypoint = $operationMap[$Operation]
$operationArguments = @($entrypoint.PrefixArguments) + @($ArgumentList)

$phase023EvidenceFileByOperation = @{
    'readonly-baseline-023' = @{ File = '01_readonly_baseline.json'; Switch = '--report-json' }
    'provision-cg-mart-refresh-023' = @{ File = '02_refresh_role_provisioning.json'; Switch = '--evidence-json' }
    'apply-route-b-bridge-023' = @{ File = '03_route_b_bridge_apply.json'; Switch = '--evidence-json' }
    'apply-june-refresh-023' = @{ File = '04_june_mart_refresh_apply.json'; Switch = '--evidence-json' }
    'validate-app-readonly-023' = @{ File = '05_readonly_app_postcheck.json'; Switch = '--report-json' }
}
if ($phase023EvidenceFileByOperation.ContainsKey($Operation)) {
    $runIndex = [Array]::IndexOf($operationArguments, '--run-id')
    if ($runIndex -lt 0 -or $runIndex + 1 -ge $operationArguments.Count) {
        throw 'A canonical --run-id is required for 023 evidence operations.'
    }
    $runId = $operationArguments[$runIndex + 1]
    if ($runId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') {
        throw 'The evidence run id must be a canonical lowercase UUID v4.'
    }
    $runDirectory = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot (Join-Path 'evidence/runtime/023' $runId))
    )
    [void](New-Item -ItemType Directory -Path $runDirectory -Force)
    $evidenceContract = $phase023EvidenceFileByOperation[$Operation]
    $outputIndex = [Array]::IndexOf($operationArguments, $evidenceContract.Switch)
    if ($outputIndex -lt 0 -or $outputIndex + 1 -ge $operationArguments.Count) {
        throw "The canonical $($evidenceContract.Switch) argument is required."
    }
    $actualOutput = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot $operationArguments[$outputIndex + 1])
    )
    $expectedOutput = [IO.Path]::GetFullPath(
        (Join-Path $runDirectory $evidenceContract.File)
    )
    if ($actualOutput -ne $expectedOutput) {
        throw 'The evidence output path is not canonical for this 023 operation.'
    }
    if (Test-Path -LiteralPath $expectedOutput) {
        throw 'The evidence output already exists.'
    }
}

$evidenceFileByOperation = @{
    'readonly-precheck' = '01_readonly_baseline.json'
    'admin-provision' = '02_admin_provisioning.json'
    'admin-reconcile-provisioning-evidence' = '02_admin_provisioning.json'
    'admin-reconcile-existing-provisioned-state' = '02_admin_provisioning.json'
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

if ($Operation -eq 'readonly-reattest-route-b-june-apply') {
    $runIndex = [Array]::IndexOf($operationArguments, '--run-id')
    if ($runIndex -lt 0 -or $runIndex + 1 -ge $operationArguments.Count) {
        throw 'A canonical --run-id is required for the post-apply reattestation.'
    }
    $runId = $operationArguments[$runIndex + 1]
    if ($runId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') {
        throw 'The evidence run id must be a canonical lowercase UUID v4.'
    }
    $runDirectory = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot (Join-Path 'evidence/runtime/022' $runId))
    )
    if (-not (Test-Path -LiteralPath $runDirectory -PathType Container)) {
        throw 'The productive run directory does not exist.'
    }
    $outputIndex = [Array]::IndexOf($operationArguments, '--report-json')
    if ($outputIndex -lt 0 -or $outputIndex + 1 -ge $operationArguments.Count) {
        throw 'The canonical --report-json argument is required.'
    }
    $actualOutput = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot $operationArguments[$outputIndex + 1])
    )
    $expectedOutput = [IO.Path]::GetFullPath(
        (Join-Path $runDirectory '04_route_b_post_apply_reattestation.json')
    )
    if ($actualOutput -ne $expectedOutput) {
        throw 'The reattestation evidence output path is not canonical.'
    }
    if (Test-Path -LiteralPath $expectedOutput) {
        throw 'The evidence output already exists.'
    }
}

if ($Operation -eq 'admin-reconcile-route-b-readonly-observer') {
    $maintenanceRunIndex = [Array]::IndexOf($operationArguments, '--maintenance-run-id')
    if ($maintenanceRunIndex -lt 0 -or $maintenanceRunIndex + 1 -ge $operationArguments.Count) {
        throw 'A canonical --maintenance-run-id is required for maintenance evidence operations.'
    }
    $maintenanceRunId = $operationArguments[$maintenanceRunIndex + 1]
    if ($maintenanceRunId -cnotmatch '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') {
        throw 'The maintenance run id must be a canonical lowercase UUID v4.'
    }
    $maintenanceDirectory = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot (Join-Path 'evidence/runtime/022' $maintenanceRunId))
    )
    [void](New-Item -ItemType Directory -Path $maintenanceDirectory -Force)
    $outputIndex = [Array]::IndexOf($operationArguments, '--evidence-json')
    if ($outputIndex -lt 0 -or $outputIndex + 1 -ge $operationArguments.Count) {
        throw 'The canonical --evidence-json argument is required.'
    }
    $actualOutput = [IO.Path]::GetFullPath(
        (Join-Path $repositoryRoot $operationArguments[$outputIndex + 1])
    )
    $expectedOutput = [IO.Path]::GetFullPath(
        (Join-Path $maintenanceDirectory '01_route_b_readonly_observer_grants.json')
    )
    if ($actualOutput -ne $expectedOutput) {
        throw 'The maintenance evidence output path is not canonical for this operation.'
    }
    if (Test-Path -LiteralPath $expectedOutput) {
        throw 'The maintenance evidence output already exists.'
    }
}

# Admin authority is checked in a secret-free child before the vault is opened.
$authorityPrecheckEnabled = (
    $entrypoint.ContainsKey('AuthorityPrecheck') -and
    [bool]$entrypoint['AuthorityPrecheck']
)
if ($authorityPrecheckEnabled) {
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
$startInfo.EnvironmentVariables['STOCK_ZERO_OPERATION_PROFILE'] = $entrypoint.Profile
$startInfo.EnvironmentVariables['STOCK_ZERO_OPERATION'] = $Operation
$injectedNames = [System.Collections.Generic.List[string]]::new()
$plainValues = [System.Collections.Generic.List[string]]::new()
try {
    foreach ($mapping in $profileMap[$entrypoint.Profile]) {
        $secret = Get-Secret -Vault $vaultName -Name $mapping.SecretName -ErrorAction Stop
        $plain = ConvertFrom-StockZeroSecureValue -Value $secret
        if ([string]::IsNullOrWhiteSpace($plain)) {
            throw "Required STOCK_ZERO secret is empty: $($mapping.SecretName)"
        }
        $startInfo.EnvironmentVariables[$mapping.EnvironmentName] = $plain
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
        [void]$startInfo.EnvironmentVariables.Remove($name)
    }
    for ($index = 0; $index -lt $plainValues.Count; $index++) {
        $plainValues[$index] = [string]::Empty
    }
    $plainValues.Clear()
}

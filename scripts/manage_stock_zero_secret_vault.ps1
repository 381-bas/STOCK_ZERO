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

    [string]$ProductiveRoleVerificationEvidence,
    [string]$ReadonlyPostcheckEvidence
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

function Assert-ApprovedEvidence {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$DocumentType,
        [Parameter(Mandatory)][string]$Verdict
    )
    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "Required evidence path is missing for $DocumentType."
    }
    $resolved = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    $evidence = Get-Content -LiteralPath $resolved -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($evidence.document_type -ne $DocumentType -or $evidence.verdict -ne $Verdict) {
        throw "Evidence is not approved: $DocumentType."
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
        $productiveDsn = Get-SecretInfo -Vault $vaultName -Name $secretNames.ProductiveDsn -ErrorAction SilentlyContinue
        if ($null -eq $productiveDsn) {
            throw 'productive_dsn_evidence_dependency_missing'
        }
        Assert-ApprovedEvidence -Path $ProductiveRoleVerificationEvidence `
            -DocumentType 'kpione_route_b_productive_role_verification_evidence_v1' `
            -Verdict 'PASS_PRODUCTIVE_ROLE_VERIFICATION'
        Assert-ApprovedEvidence -Path $ReadonlyPostcheckEvidence `
            -DocumentType 'kpione_route_b_readonly_postcheck_evidence_v1' `
            -Verdict 'PASS_READONLY_POSTCHECK'
        Remove-Secret -Vault $vaultName -Name $secretNames.Admin -ErrorAction SilentlyContinue
        Remove-Secret -Vault $vaultName -Name $secretNames.ProductivePassword -ErrorAction SilentlyContinue
    }
}

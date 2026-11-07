[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $ManifestPath,

    [Parameter(Mandatory)]
    [System.Management.Automation.SemanticVersion]
    $Version
)

process {
    $ManifestContent = Import-PowerShellDataFile $ManifestPath
    $ManifestContent['ModuleVersion'] = [Version]$Version
    if ($Version.PreReleaseLabel) {
        $ManifestContent['Prerelease'] = [string]$Version.PreReleaseLabel
    }
    
    @(
        '@{'
        $ManifestContent.Keys | ForEach-Object {
            "    '$($_ -replace "'", "''")' = $(($ManifestContent[$_] | ForEach-Object { "'$($_ -replace "'", "''")'" }) -join ',' )"
        }
        '}'
    ) | Out-File $ManifestPath
}
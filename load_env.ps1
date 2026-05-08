foreach($line in Get-Content C:\Users\ashra\Downloads\factory-data-platform\.env) {
    if($line -match '^[^#]' -and $line -match '=') {
        $parts = $line.Split('=', 2)
        [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
        Write-Host "Loaded: $($parts[0].Trim())"
    }
}
Write-Host "Done!"
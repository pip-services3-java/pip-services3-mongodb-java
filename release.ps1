#!/usr/bin/env pwsh

Set-StrictMode -Version latest
$ErrorActionPreference = "Stop"

$component = Get-Content -Path "component.json" | ConvertFrom-Json
$version = ([xml](Get-Content -Path "pom.xml")).project.version

if ($component.version -ne $version) {
    throw "Versions in component.json and pom.xml do not match"
}

# TODO: switch npm to maven
# Automatically login to server
# if ($env:NPM_USER -ne $null -and $env:NPM_PASS -ne $null) {
#     npm-cli-login
# }

# npm publish
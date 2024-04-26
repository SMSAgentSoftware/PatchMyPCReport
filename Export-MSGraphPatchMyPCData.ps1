######################################################################################################
## Azure automation runbook PowerShell script to export app install data for Intune apps created by ##
## Patch My PC and dump it to Azure Blob storage where it can be used as a datasource for Power BI. ##
######################################################################################################

## Requirements ##
# Module: Az.Accounts
# Module: Az.Storage
# Recommend PowerShell 7+ for this script


#region ------------------------------------------ Variables --------------------------------------------
[validateset('Overview','Detailed','All')]
$ReportType = "All" # Specify 'Overview' for the Overview report only, 'Detailed' for the Detailed report only, or 'All' for both
$ResourceGroup = "<ResourceGroupName>" # Reource group that hosts the storage account
$StorageAccount = "<StorageAccountName>" # Storage account name
$Container = "<ContainerName>" # Container name
$script:Destination = "$env:Temp" # Temp location for exporting the reports
# Make sure the thread culture is US for consistency of dates. Applies only to the single execution.
If ([System.Globalization.CultureInfo]::CurrentUICulture.Name -ne "en-US")
{
    [System.Globalization.CultureInfo]::CurrentUICulture = [System.Globalization.CultureInfo]::new("en-US")
}
If ([System.Globalization.CultureInfo]::CurrentCulture.Name -ne "en-US")
{
    [System.Globalization.CultureInfo]::CurrentCulture = [System.Globalization.CultureInfo]::new("en-US")
}
$ProgressPreference = 'SilentlyContinue' # Speeds up web requests
#endregion ----------------------------------------------------------------------------------------------


#region ---------------------------------------- Authentication -----------------------------------------
## Connect to Azure
$null = Connect-AzAccount -Identity
## Get MS Graph access token 
$script:accessToken = (Get-AzAccessToken -ResourceUrl 'https://graph.microsoft.com/').Token
#endregion ----------------------------------------------------------------------------------------------


#region ------------------------------------------- Functions -------------------------------------------
# Function to invoke a web request with error handling
Function script:Invoke-WebRequestPro {
    Param ($URL,$Headers,$Method,$Body,$ContentType)
    try {
        If ($Method -eq "Post")
        {
            If ($PSEdition -eq "Core")
            {
                $WebRequest = Invoke-WebRequest -Uri $URL -Method $Method -Headers $Headers -Body $Body -ContentType $ContentType -MaximumRetryCount 5 -RetryIntervalSec 30
            }
            else 
            {
                $WebRequest = Invoke-WebRequest -Uri $URL -Method $Method -Headers $Headers -Body $Body -ContentType $ContentType
            }
        }
        else 
        {
            If ($PSEdition -eq "Core")
            {
                $WebRequest = Invoke-WebRequest -Uri $URL -Method $Method -Headers $Headers -MaximumRetryCount 5 -RetryIntervalSec 30
            }
            else 
            {
                $WebRequest = Invoke-WebRequest -Uri $URL -Method $Method -Headers $Headers
            }
        }     
    }
    catch {
        $WebRequest = $_.Exception.Response
    }
    Return $WebRequest
}

# Function to export a device install status report
Function Export-StatusReport {
    Param($ReportOutputName,$ReportEntityName,$ApplicationData)

    # Some variables
    $reporturl = "deviceManagement/reports/$ReportEntityName"
    $headers = @{
        "Content-Type" = "application/json"
    }
    $DataTable = [System.Data.DataTable]::new()

    # Prepare the apps in batches of 20 due to the current limitation of batching with Graph
    [int]$SkipValue = 0
    $BatchArray = [System.Collections.Generic.List[Object]]::new()
    do {
        $batch = $ApplicationData | Select -First 20 -Skip $SkipValue
        $BatchArray.Add($batch)
        $SkipValue = $SkipValue + 20
    } until ($SkipValue -ge $ApplicationData.Count)

    # Process each batch
    foreach ($batch in $BatchArray)
    {
        $requests = @()
        [int]$Id = 1

        # generate a request for each app in the batch
        foreach ($app in $batch)
        {
            $body = @{
                filter = "(ApplicationId eq '$($App.id)')"
                top = 3000
            }
            $requesthash = [ordered]@{
                id = $Id.ToString()
                method = "POST"
                url = $reporturl
                body = $Body
                headers = $headers
            }
            $requests += $requesthash
            $Id ++
        }

        # Convert the requests to JSON
        If ($Requests.Count -ge 1)
        {
            $RetryCount = 0
            $requestbase = @{
                requests = $requests
            }
            $JsonBase = $requestbase | ConvertTo-Json -Depth 3
            $URL = "https://graph.microsoft.com/beta/`$batch"
            $batchheaders = @{'Authorization'="Bearer " + $accessToken; 'Accept'="application/json"}

            # Post the batch
            do {
                $WebRequest = Invoke-WebRequest -Uri $URL -Method POST -Headers $batchheaders -Body $JsonBase -ContentType "application/json" 
                $responses = ($WebRequest.Content | ConvertFrom-Json).responses | Sort-Object -Property id  
                $responsesStatusCodes = $responses.Status
                $batchIsSuccess = ($responsesStatusCodes | Select -Unique) -eq 200
                if ($responsesStatusCodes -contains 429 -or $responsesStatusCodes -contains 503 -or $responsesStatusCodes -contains 504)
                {  
                    $LastResponse = $responses | where {$_.Status -in (429,503,504)} | Select -Last 1
                    [int]$RetryAfter = $LastResponse.headers.'Retry-After'
                    If ($null -ne $RetryAfter)
                    {
                        # If a Retry-After header is present, wait for the specified time
                        $RetryCount ++
                        $statusCodes = (($responsesStatusCodes | Select -Unique) | Where {$_ -ne 200}) -join ', '
                        [int]$SleepTime = $RetryAfter * 60 + 2
                        Write-Warning "Batch request returned status codes $statusCodes. Retry-After header is present. Waiting $SleepTime seconds before retrying"
                        Start-Sleep -Seconds $SleepTime
                    }
                    else
                    {
                        # If no Retry-After header is present, use exponential backoff logic
                        $RetryCount ++
                        $SleepTime = [math]::Round([math]::Pow(2,$RetryCount +4) * (Get-Random -Minimum 0.8 -Maximum 1.2),2)
                        $statusCodes = (($responsesStatusCodes | Select -Unique) | Where {$_ -ne 200}) -join ', '
                        Write-Warning "Batch request returned status codes $statusCodes. No Retry-After header was found, falling back to exponential backoff logic. Waiting $SleepTime seconds before retrying"
                        Start-Sleep -Seconds $SleepTime
                    } 
                }
                elseif ($batchIsSuccess -ne $true)
                {
                    Write-Warning "Batch request returned status codes $(($responsesStatusCodes | Select -unique) -join ', ')"
                    break
                }
            }
            until ($batchIsSuccess -eq $true -or $RetryCount -ge 5)
        }

        # If the batch failed, exit the run
        if ($batchIsSuccess -ne $true)
        {
            If ($RetryCount -ge 5)
            {
                Write-Error "Batch request exceeded the retry count. Exiting this run"
            }
            else 
            {
                Write-Error "Exiting this run due to unexpected http status codes in the batch response"
            }
            break
        }

        # process the responses into a datatable
        foreach ($response in $responses)
        {
            $JSONresponse = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($response.body)) | ConvertFrom-Json
            If ($DataTable.Columns.Count -eq 0)
            {
                foreach ($column in $JSONresponse.Schema)
                {
                    [void]$DataTable.Columns.Add($Column.Column)
                }
                [void]$DataTable.Columns.Add("AppType")
                [void]$DataTable.Columns.Add("ApplicableDeviceCount")
                [void]$DataTable.Columns.Add("PercentSuccess")
            }
            if ($JSONresponse.values.Count -ge 1)
            {
                foreach ($value in $JSONresponse.Values)
                {
                    $PMPAppId = $PmpApps.Where({$_.Id -eq $Value[0]}) | Select -ExpandProperty notes
                    if ($PMPAppId -match "PmpAppId:")
                    {
                        $AppType = "App"
                    }
                    if ($PMPAppId -match "PmpUpdateId:")
                    {
                        $AppType = "Update"
                    }
                    if ($PMPAppId -match "PmpCustomAppId:")
                    {
                        $AppType = "CustomApp"
                    }
                    $Value += $AppType
                    $TotalApplicable = $Value[1] + $Value[2] + $Value[3] + $Value[4]
                    $Value += $TotalApplicable
                    try
                    {
                        $PercentSuccess = [math]::Round(($Value[3] / $TotalApplicable) * 100, 2)
                    }
                    catch
                    {
                        $PercentSuccess = 0
                    }
                    $Value += $PercentSuccess
                    [void]$DataTable.Rows.Add($value)
                }  
            }
            else 
            {
                # If no values are returned, add a row with all zeros for the values
                $request = $Requests.where({$_.id -eq $response.id})
                $appId = $request.body.filter.Split(" ")[2].substring(1,36)
                $PMPAppId = $PmpApps.Where({$_.Id -eq $appId}) | Select -ExpandProperty notes
                if ($PMPAppId -match "PmpAppId:")
                {
                    $AppType = "App"
                }
                if ($PMPAppId -match "PmpUpdateId:")
                {
                    $AppType = "Update"
                }
                if ($PMPAppId -match "PmpCustomAppId:")
                {
                    $AppType = "CustomApp"
                }
                [void]$DataTable.Rows.Add($appId,0,0,0,0,0,0,0,0,0,0,$AppType,0,0)
            }
        }
    }

    if ($batchFailure -eq $true)
    {
        throw "Batch request failed. Exiting this run"
    }

    # If there are no results, just add a blank row to avoid errors in the PowerBI report
    If ($DataTable.Rows.Count -eq 0)
    {
        If ($DataTable.Columns.Count -eq 0)
        {
            $ColumnNames = @(
                "ApplicationId",
                "FailedDeviceCount",
                "PendingInstallDeviceCount",
                "InstalledDeviceCount",
                "NotInstalledDeviceCount",
                "NotApplicableDeviceCount",
                "FailedUserCount",
                "PendingInstallUserCount",
                "InstalledUserCount",
                "NotInstalledUserCount",
                "NotApplicableUserCount",
                "AppType",
                "ApplicableDeviceCount",
                "PercentSuccess")
            foreach ($ColumnName in $ColumnNames)
            {
                [void]$DataTable.Columns.Add($ColumnName)
            }
        }
        $DataTable.Columns | foreach {[array]$nullString += ""}
        [void]$DataTable.Rows.Add($nullString)
        Remove-Variable nullString
    }

    # Export the data
    $DataTable | Export-Csv -Path "$Destination\$ReportOutputName.csv" -NoTypeInformation -Force

    # Calculate summary data
    $SummaryDataTable = [System.Data.DataTable]::new()
    @(
        'TotalApplicableDevices',
        'TotalInstalled',
        'TotalFailed',
        'TotalInstallPending',
        'TotalNotInstalled',
        'TotalPercentSuccess',
        'TotalCount',
        'AppsTotalApplicableDevices',
        'AppsTotalInstalled',
        'AppsTotalFailed',
        'AppsTotalInstallPending',
        'AppsTotalNotInstalled',
        'AppsTotalPercentSuccess',
        'TotalAppCount',
        'UpdatesTotalApplicableDevices',
        'UpdatesTotalInstalled',
        'UpdatesTotalFailed',
        'UpdatesTotalInstallPending',
        'UpdatesTotalNotInstalled',
        'UpdatesTotalPercentSuccess',
        'TotalUpdateCount',
        'CustomAppsTotalApplicableDevices',
        'CustomAppsTotalInstalled',
        'CustomAppsTotalFailed',
        'CustomAppsTotalInstallPending',
        'CustomAppsTotalNotInstalled',
        'CustomAppsTotalPercentSuccess',
        'TotalCustomAppCount') | 
        foreach {[void]$SummaryDataTable.Columns.Add($_)}
    $SummaryDataRow = $SummaryDataTable.NewRow()
    if ($DataTable.Rows.Count -ge 1)
    {
        [int]$TotalApplicableDevices = $DataTable.ApplicableDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$TotalInstalled = $DataTable.InstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$TotalFailed = $DataTable.FailedDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$TotalInstallPending = $DataTable.PendingInstallDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$TotalNotInstalled = $DataTable.NotInstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        try 
        {
            [double]$TotalPercentSuccess = [math]::Round(($TotalInstalled / $TotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$TotalPercentSuccess = $null
        }
        $TotalCount = $DataTable.Rows.Count
    }
    else
    {
        $TotalApplicableDevices = 0
        $TotalInstalled = 0
        $TotalFailed = 0
        $TotalInstallPending = 0
        $TotalNotInstalled = 0
        $TotalPercentSuccess = 0
        $TotalCount = 0
    }
    $SummaryDataRow.TotalApplicableDevices = $TotalApplicableDevices
    $SummaryDataRow.TotalInstalled = $TotalInstalled
    $SummaryDataRow.TotalFailed = $TotalFailed
    $SummaryDataRow.TotalInstallPending = $TotalInstallPending
    $SummaryDataRow.TotalNotInstalled = $TotalNotInstalled
    $SummaryDataRow.TotalPercentSuccess = $TotalPercentSuccess
    $SummaryDataRow.TotalCount = $TotalCount
    
    # For apps only
    [array]$Apps = $DataTable.Select("AppType = 'App'")
    if ($Apps.Count -ge 1)
    {
        [int]$AppsTotalApplicableDevices = $Apps.ApplicableDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$AppsTotalInstalled = $Apps.InstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$AppsTotalFailed = $Apps.FailedDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$AppsTotalInstallPending = $Apps.PendingInstallDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$AppsTotalNotInstalled = $Apps.NotInstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        try 
        {
            [double]$AppsTotalPercentSuccess = [math]::Round(($AppsTotalInstalled / $AppsTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$AppsTotalPercentSuccess = $null
        }  
        $TotalAppCount = $Apps.Count
    }
    else
    {
        $AppsTotalApplicableDevices = 0
        $AppsTotalInstalled = 0
        $AppsTotalFailed = 0
        $AppsTotalInstallPending = 0
        $AppsTotalNotInstalled = 0
        $AppsTotalPercentSuccess = 0
        $TotalAppCount = 0
    }
    $SummaryDataRow.AppsTotalApplicableDevices = $AppsTotalApplicableDevices
    $SummaryDataRow.AppsTotalInstalled = $AppsTotalInstalled
    $SummaryDataRow.AppsTotalFailed = $AppsTotalFailed
    $SummaryDataRow.AppsTotalInstallPending = $AppsTotalInstallPending
    $SummaryDataRow.AppsTotalNotInstalled = $AppsTotalNotInstalled
    $SummaryDataRow.AppsTotalPercentSuccess = $AppsTotalPercentSuccess
    $SummaryDataRow.TotalAppCount = $TotalAppCount

    # For updates only
    $Updates = $DataTable.Select("AppType = 'Update'")
    if ($Updates.Count -ge 1)
    {
        [int]$UpdatesTotalApplicableDevices = $Updates.ApplicableDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$UpdatesTotalInstalled = $Updates.InstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$UpdatesTotalFailed = $Updates.FailedDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$UpdatesTotalInstallPending = $Updates.PendingInstallDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$UpdatesTotalNotInstalled = $Updates.NotInstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        try 
        {
            [double]$UpdatesTotalPercentSuccess = [math]::Round(($UpdatesTotalInstalled / $UpdatesTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$UpdatesTotalPercentSuccess = $null
        }  
        $TotalUpdateCount = $Updates.Count
    }
    else
    {
        $UpdatesTotalApplicableDevices = 0
        $UpdatesTotalInstalled = 0
        $UpdatesTotalFailed = 0
        $UpdatesTotalInstallPending = 0
        $UpdatesTotalNotInstalled = 0
        $UpdatesTotalPercentSuccess = 0
        $TotalUpdateCount = 0
    }
    $SummaryDataRow.UpdatesTotalApplicableDevices = $UpdatesTotalApplicableDevices
    $SummaryDataRow.UpdatesTotalInstalled = $UpdatesTotalInstalled
    $SummaryDataRow.UpdatesTotalFailed = $UpdatesTotalFailed
    $SummaryDataRow.UpdatesTotalInstallPending = $UpdatesTotalInstallPending
    $SummaryDataRow.UpdatesTotalNotInstalled = $UpdatesTotalNotInstalled
    $SummaryDataRow.UpdatesTotalPercentSuccess = $UpdatesTotalPercentSuccess
    $SummaryDataRow.TotalUpdateCount = $TotalUpdateCount

    # For custom apps only
    $CustomApps = $DataTable.Select("AppType = 'CustomApp'")
    if ($CustomApps.Count -ge 1)
    {
        [int]$CustomAppsTotalApplicableDevices = $CustomApps.ApplicableDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$CustomAppsTotalInstalled = $CustomApps.InstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$CustomAppsTotalFailed = $CustomApps.FailedDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$CustomAppsTotalInstallPending = $CustomApps.PendingInstallDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        [int]$CustomAppsTotalNotInstalled = $CustomApps.NotInstalledDeviceCount | Measure-Object -Sum | Select -ExpandProperty Sum
        try 
        {
            [double]$CustomAppsTotalPercentSuccess = [math]::Round(($CustomAppsTotalInstalled / $CustomAppsTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$CustomAppsTotalPercentSuccess = $null
        }  
        $TotalCustomAppCount = $CustomApps.Count
    }
    else
    {
        $CustomAppsTotalApplicableDevices = 0
        $CustomAppsTotalInstalled = 0
        $CustomAppsTotalFailed = 0
        $CustomAppsTotalInstallPending = 0
        $CustomAppsTotalNotInstalled = 0
        $CustomAppsTotalPercentSuccess = 0
        $TotalCustomAppCount = 0
    }
    $SummaryDataRow.CustomAppsTotalApplicableDevices = $CustomAppsTotalApplicableDevices
    $SummaryDataRow.CustomAppsTotalInstalled = $CustomAppsTotalInstalled
    $SummaryDataRow.CustomAppsTotalFailed = $CustomAppsTotalFailed
    $SummaryDataRow.CustomAppsTotalInstallPending = $CustomAppsTotalInstallPending
    $SummaryDataRow.CustomAppsTotalNotInstalled = $CustomAppsTotalNotInstalled
    $SummaryDataRow.CustomAppsTotalPercentSuccess = $CustomAppsTotalPercentSuccess
    $SummaryDataRow.TotalCustomAppCount = $TotalCustomAppCount

    [void]$SummaryDataTable.Rows.Add($SummaryDataRow)
    
    # Export the data
    $SummaryDataTable | Export-Csv -Path "$Destination\PmpAppsOverviewSummaryData.csv" -NoTypeInformation -Force
}

# Function to get the list of Pmp Apps from Graph
Function script:Get-PmpAppsList {
    $URL = "https://graph.microsoft.com/v1.0/deviceAppManagement/mobileApps?`$filter=startswith(notes, 'Pmp')&`$expand=assignments&`$select=id,displayName,description,publisher,createdDateTime,lastModifiedDateTime,notes"
    $headers = @{'Authorization'="Bearer " + $accessToken}
    $Result = [System.Collections.Generic.List[Object]]::new()
    $GraphRequest = Invoke-WebRequestPro -URL $URL -Headers $headers -Method GET
    If ($GraphRequest.StatusCode -ne 200)
    {
        Return $GraphRequest
    }
    $Content = $GraphRequest.Content | ConvertFrom-Json
    $Result.AddRange($Content.value)
    
    # Page through the next links if there are any
    If ($Content.'@odata.nextLink')
    {
        Do {
            $GraphRequest = Invoke-WebRequestPro -URL $Content.'@odata.nextLink' -Headers $headers -Method GET
            If ($GraphRequest.StatusCode -ne 200)
            {
                Return $GraphRequest
            }
            $Content = $GraphRequest.Content | ConvertFrom-Json
            $Result.AddRange($Content.value)
        }
        While ($null -ne $Content.'@odata.nextLink')
    }
    Return $Result
}

# Function to export the Pmp Apps and Updates to CSV
Function Export-PmpAppsList {

    $Result = Get-PmpAppsList
    If ($Result.GetType().BaseType -eq [System.Net.WebResponse])
    {
        If ($Result.StatusCode.value__ -eq 504)
        {
            # Server timeout encountered, lets try again a couple of times
            Write-Warning -Message "Http 504 (gateway timeout) encountered while getting Pmp apps list. Retrying up to 3 times."
            [int]$RetryAttempts = 0
            do {
                $RetryAttempts ++ 
                Start-Sleep -Seconds 5
                $Result = Get-PmpAppsList 
            }
            until ($RetryAttempts -gt 3 -or $Result.GetType().BaseType -ne [System.Net.WebResponse])
        }
        If ($Result.GetType().BaseType -eq [System.Net.WebResponse])
        {
            throw "Http error encountered from Graph API. Status code: $($Result.StatusCode.value__). Status description: $($Result.StatusDescription)."
            Exit 1
        }
    }

    # Remove some unwanted properties
    $Results = $Result | Select -Property * -ExcludeProperty '@odata.type','assignments@odata.context'
    
    # Add the customised results to a datatable
    $DataTable = [System.Data.DataTable]::new()
    foreach ($column in ($Results | Get-Member -MemberType NoteProperty | Select -ExpandProperty Name))
    {
        [void]$DataTable.Columns.Add($Column)
    }
    [void]$DataTable.Columns.Add("appType")
    foreach ($Result in $Results)
    {
        if ($Result.notes -match "PmpAppId:")
        {
            $AppType = "App"
        }
        if ($Result.notes -match "PmpUpdateId:")
        {
            $AppType = "Update"
        }
        if ($Result.notes -match "PmpCustomAppId:")
        {
            $AppType = "CustomApp"
        }
        [void]$DataTable.Rows.Add(
            $Result.assignments.count,  
            ($Result.createdDateTime | Get-Date -Format "s"),
            $Result.description,
            $Result.displayName,
            $Result.Id,
            ($Result.lastModifiedDateTime | Get-Date -Format "s"),
            $Result.notes,
            $Result.publisher,
            $AppType
        )
    }
    
    # Export only those with assignments
    [array]$script:PmpApps = $DataTable.Select("assignments >= 1") 
    If ($PmpApps.Count -eq 0)
    {
        $TempTable = $DataTable.DefaultView.ToTable()
        $TempTable.Clear()
        [void]$TempTable.Rows.Add($DataTable.Select("assignments >= 1"))
        [array]$PmpApps = $TempTable.Rows
    }

    $PmpApps | Export-Csv -Path $Destination\PmpApps.csv -NoTypeInformation -Force
}

# Function to get a device install status report for a list of applications
Function Get-DeviceInstallStatusReport {
    param($AppIDs)

    $script:DeviceInstallStatusResults = [System.Collections.Generic.List[Object]]::new()
    
    # Process each app
    foreach ($AppId in $AppIDs)
    {
        Write-Output "Processing $AppId"
        $i = 0 
        $RetryCount = 0
        $DataTable = [System.Data.DataTable]::new() 
        $MasterArray = [System.Collections.Generic.List[Object]]::new()
        do {
            $bodyHash = [ordered]@{
                skip = $i
                top = 50
                filter = "((InstallState eq '1') or (InstallState eq '2') or (InstallState eq '3') or (InstallState eq '5') or (InstallState eq '4') or (InstallState eq '99')) and (ApplicationId eq '$AppID')"
            }
            $bodyJson = $bodyHash | ConvertTo-Json -Depth 3

            $URL = "https://graph.microsoft.com/beta/deviceManagement/reports/getDeviceInstallStatusReport"
            $Headers = @{'Authorization'="Bearer " + $accessToken; 'Accept'="application/json"}
            $GraphRequest = Invoke-WebRequestPro -URL $URL -Headers $Headers -Method POST -Body $bodyJson -ContentType "application/json"
            If ($GraphRequest.StatusCode -eq 200)
            {
                $JSONresponse = [System.Text.Encoding]::UTF8.GetString($GraphRequest.Content) | ConvertFrom-Json
                If ($DataTable.Columns.Count -eq 0)
                {
                    foreach ($column in $JSONresponse.Schema)
                    {
                        [void]$DataTable.Columns.Add($Column.Column)
                    }
                    [void]$DataTable.Columns.Add("AppType")
                }
                foreach ($value in $JSONresponse.Values)
                {
                    $value[11] = $value[11] | Get-Date -Format "s"
                    $PMPAppId = $PmpApps.Where({$_.Id -eq $Value[1]}) | Select -ExpandProperty notes
                    if ($PMPAppId -match "PmpAppId:")
                    {
                        $AppType = "App"
                    }
                    if ($PMPAppId -match "PmpUpdateId:")
                    {
                        $AppType = "Update"
                    }
                    if ($PMPAppId -match "PmpCustomAppId:")
                    {
                        $AppType = "CustomApp"
                    }
                    $Value += $AppType
                    [void]$DataTable.Rows.Add($value)
                }
                $i = $i + 50
            }
            ElseIf ($GraphRequest.StatusCode -in 429,503,504)
            {              
                Write-Warning "Graph request returned status code $($GraphRequest.StatusCode)"
                # Header type: System.Net.Http.Headers.HttpResponseHeaders
                [int]$RetryAfter = $GraphRequest.Headers.RetryAfter.ToString()
                If ($null -ne $RetryAfter)
                {
                    $RetryCount ++
                    [int]$SleepTime = $RetryAfter * 60 + 2
                    Write-Warning "Retry-After header is present. Waiting $SleepTime seconds before retrying"
                    Start-Sleep -Seconds $SleepTime
                }
                else
                {
                    $RetryCount ++
                    $SleepTime = [math]::Round([math]::Pow(2,$RetryCount +4) * (Get-Random -Minimum 0.8 -Maximum 1.2),2) # exponential backoff with random offset
                    Write-Warning "No Retry-After header was found, falling back to exponential backoff logic. Waiting $SleepTime seconds before retrying"
                    Start-Sleep -Seconds $SleepTime
                } 
            }
            else 
            {
                Write-Warning "Graph request returned status code $($GraphRequest.StatusCode)"
                $Bail = $true
                break
            }
        }
        Until (($JSONresponse.Values.Count -eq 0 -and $GraphRequest.StatusCode -eq 200) -or $RetryCount -ge 5 -or $Bail -eq $true)

        If ($RetryCount -ge 5)
        {
            Write-Error "Graph request for $AppId exceeded the retry count. No data will be returned for this app."
            continue
        }
        If ($Bail -eq $true)
        {
            Write-Error "Graph request for $AppId returned an unexpected http status code. No data will be returned for this app."
            continue
        }

        foreach ($Row in $DataTable.Rows)
        {
            $MasterArray.Add($Row)
        } 
        # Remove duplicate entries if there are any, retaining only the latest entry
        If (($MasterArray.DeviceId | Select -Unique).Count -lt $MasterArray.Count)
        {
            Remove-MSGraphExportJobDuplicates -Collection $MasterArray
        }
        foreach ($Result in $MasterArray){$DeviceInstallStatusResults.Add($Result)}
    }

    # Export the final data set
    If ($DeviceInstallStatusResults.Count -eq 0)
    {
        If ($null -eq $DataTable)
        {
            $DataTable = [System.Data.DataTable]::new() 
            $ColumnNames = @("DeviceId","ApplicationId","UserId","DeviceName","UserPrincipalName","UserName","Platform","AppVersion","ErrorCode","InstallState","InstallStateDetail","LastModifiedDateTime","AssignmentFilterIdsExist","HexErrorCode","AppInstallState","AppInstallState_loc","AppInstallStateDetails","AppInstallStateDetails_loc","AppType")
            foreach ($ColumnName in $ColumnNames)
            {
                [void]$DataTable.Columns.Add($ColumnName)
            }
        }
        $DataTable.Columns | foreach {[array]$nullString += ""}
        [void]$DataTable.Rows.Add($nullString)
        Remove-Variable nullString
        $DataTable | Export-CSV -Path $Destination\PmpDeviceInstallStatusReport.csv -NoTypeInformation -Force
    }
    else 
    {
        $DeviceInstallStatusResults | Export-CSV -Path $Destination\PmpDeviceInstallStatusReport.csv -NoTypeInformation -Force
    }

    # Calculate summary data
    $SummaryDataTable = [System.Data.DataTable]::new()
    @(
        'TotalApplicableDevices',
        'TotalInstalled',
        'TotalFailed',
        'TotalInstallPending',
        'TotalNotInstalled',
        'TotalPercentSuccess',
        'TotalCount',
        'AppsTotalApplicableDevices',
        'AppsTotalInstalled',
        'AppsTotalFailed',
        'AppsTotalInstallPending',
        'AppsTotalNotInstalled',
        'AppsTotalPercentSuccess',
        'AppsTotalCount',
        'UpdatesTotalApplicableDevices',
        'UpdatesTotalInstalled',
        'UpdatesTotalFailed',
        'UpdatesTotalInstallPending',
        'UpdatesTotalNotInstalled',
        'UpdatesTotalPercentSuccess',
        'UpdatesTotalCount',
        'CustomAppsTotalApplicableDevices',
        'CustomAppsTotalInstalled',
        'CustomAppsTotalFailed',
        'CustomAppsTotalInstallPending',
        'CustomAppsTotalNotInstalled',
        'CustomAppsTotalPercentSuccess',
        'CustomAppsTotalCount') | 
        foreach {[void]$SummaryDataTable.Columns.Add($_)}
    $SummaryDataRow = $SummaryDataTable.NewRow()
    if ($DeviceInstallStatusResults.Rows.Count -ge 1)
    {
        [int]$TotalApplicableDevices = $DeviceInstallStatusResults.Rows.Count
        [int]$TotalInstalled = $DeviceInstallStatusResults.FindAll({$args[0].AppInstallState_loc -eq "Installed" }).Count
        [int]$TotalFailed = $DeviceInstallStatusResults.FindAll({$args[0].AppInstallState_loc -eq "Failed" }).Count
        [int]$TotalInstallPending = $DeviceInstallStatusResults.FindAll({$args[0].AppInstallState_loc -eq "Install Pending" }).Count
        [int]$TotalNotInstalled = $DeviceInstallStatusResults.FindAll({$args[0].AppInstallState_loc -eq "Not installed" }).Count
        try 
        {
            [double]$TotalPercentSuccess = [math]::Round(($TotalInstalled / $TotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$TotalPercentSuccess = $null
        }
        [int]$TotalCount = $PmpApps.Count
    }
    else
    {
        $TotalApplicableDevices = 0
        $TotalInstalled = 0
        $TotalFailed = 0
        $TotalInstallPending = 0
        $TotalNotInstalled = 0
        $TotalPercentSuccess = 0
        $TotalCount = 0
    }
    $SummaryDataRow.TotalApplicableDevices = $TotalApplicableDevices
    $SummaryDataRow.TotalInstalled = $TotalInstalled
    $SummaryDataRow.TotalFailed = $TotalFailed
    $SummaryDataRow.TotalInstallPending = $TotalInstallPending
    $SummaryDataRow.TotalNotInstalled = $TotalNotInstalled
    $SummaryDataRow.TotalPercentSuccess = $TotalPercentSuccess
    $SummaryDataRow.TotalCount = $TotalCount

    # For apps only
    [array]$Apps = $DeviceInstallStatusResults.FindAll({$args[0].AppType -eq "App"})
    if ($Apps.Count -ge 1)
    {
        [int]$AppsTotalApplicableDevices = $Apps.Count
        [int]$AppsTotalInstalled = $Apps.where({$_.AppInstallState_loc -eq "Installed"}).Count
        [int]$AppsTotalFailed = $Apps.where({$_.AppInstallState_loc -eq "Failed"}).Count
        [int]$AppsTotalInstallPending = $Apps.where({$_.AppInstallState_loc -eq "Install Pending" }).Count
        [int]$AppsTotalNotInstalled = $Apps.where({$_.AppInstallState_loc -eq "Not installed" }).Count
        try 
        {
            [double]$AppsTotalPercentSuccess = [math]::Round(($AppsTotalInstalled / $AppsTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$AppsTotalPercentSuccess = $null
        }  
        $AppsTotalCount = $PMPApps.where({$_.appType -eq "App"}).Count
    }
    else
    {
        $AppsTotalApplicableDevices = 0
        $AppsTotalInstalled = 0
        $AppsTotalFailed = 0
        $AppsTotalInstallPending = 0
        $AppsTotalNotInstalled = 0
        $AppsTotalPercentSuccess = 0
        $AppsTotalCount = 0
    }
    $SummaryDataRow.AppsTotalApplicableDevices = $AppsTotalApplicableDevices
    $SummaryDataRow.AppsTotalInstalled = $AppsTotalInstalled
    $SummaryDataRow.AppsTotalFailed = $AppsTotalFailed
    $SummaryDataRow.AppsTotalInstallPending = $AppsTotalInstallPending
    $SummaryDataRow.AppsTotalNotInstalled = $AppsTotalNotInstalled
    $SummaryDataRow.AppsTotalPercentSuccess = $AppsTotalPercentSuccess
    $SummaryDataRow.AppsTotalCount = $AppsTotalCount

    # For updates only
    $Updates = $DeviceInstallStatusResults.FindAll({$args[0].AppType -eq "Update"})
    if ($Updates.Count -ge 1)
    {
        [int]$UpdatesTotalApplicableDevices = $Updates.Count
        [int]$UpdatesTotalInstalled = $Updates.where({$_.AppInstallState_loc -eq "Installed"}).Count
        [int]$UpdatesTotalFailed = $Updates.where({$_.AppInstallState_loc -eq "Failed"}).Count
        [int]$UpdatesTotalInstallPending = $Updates.where({$_.AppInstallState_loc -eq "Install Pending" }).Count
        [int]$UpdatesTotalNotInstalled = $Updates.where({$_.AppInstallState_loc -eq "Not installed" }).Count
        try 
        {
            [double]$UpdatesTotalPercentSuccess = [math]::Round(($UpdatesTotalInstalled / $UpdatesTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$UpdatesTotalPercentSuccess = $null
        }  
        $UpdatesTotalCount = $PMPApps.where({$_.appType -eq "Update"}).Count
    }
    else
    {
        $UpdatesTotalApplicableDevices = 0
        $UpdatesTotalInstalled = 0
        $UpdatesTotalFailed = 0
        $UpdatesTotalInstallPending = 0
        $UpdatesTotalNotInstalled = 0
        $UpdatesTotalPercentSuccess = 0
        $UpdatesTotalCount = 0
    }
    $SummaryDataRow.UpdatesTotalApplicableDevices = $UpdatesTotalApplicableDevices
    $SummaryDataRow.UpdatesTotalInstalled = $UpdatesTotalInstalled
    $SummaryDataRow.UpdatesTotalFailed = $UpdatesTotalFailed
    $SummaryDataRow.UpdatesTotalInstallPending = $UpdatesTotalInstallPending
    $SummaryDataRow.UpdatesTotalNotInstalled = $UpdatesTotalNotInstalled
    $SummaryDataRow.UpdatesTotalPercentSuccess = $UpdatesTotalPercentSuccess
    $SummaryDataRow.UpdatesTotalCount = $UpdatesTotalCount

    # For custom apps only
    $CustomApps = $DeviceInstallStatusResults.FindAll({$args[0].AppType -eq "CustomApp"})
    if ($CustomApps.Count -ge 1)
    {
        [int]$CustomAppsTotalApplicableDevices = $CustomApps.Count
        [int]$CustomAppsTotalInstalled = $CustomApps.where({$_.AppInstallState_loc -eq "Installed"}).Count
        [int]$CustomAppsTotalFailed = $CustomApps.where({$_.AppInstallState_loc -eq "Failed"}).Count
        [int]$CustomAppsTotalInstallPending = $CustomApps.where({$_.AppInstallState_loc -eq "Install Pending" }).Count
        [int]$CustomAppsTotalNotInstalled = $CustomApps.where({$_.AppInstallState_loc -eq "Not installed" }).Count
        try 
        {
            [double]$CustomAppsTotalPercentSuccess = [math]::Round(($CustomAppsTotalInstalled / $CustomAppsTotalApplicableDevices) * 100, 2)
        }
        catch 
        {
            [double]$CustomAppsTotalPercentSuccess = $null
        }  
        $CustomAppsTotalCount = $PMPApps.where({$_.appType -eq "CustomApp"}).Count
    }
    else
    {
        $CustomAppsTotalApplicableDevices = 0
        $CustomAppsTotalInstalled = 0
        $CustomAppsTotalFailed = 0
        $CustomAppsTotalInstallPending = 0
        $CustomAppsTotalNotInstalled = 0
        $CustomAppsTotalPercentSuccess = 0
        $CustomAppsTotalCount = 0
    }
    $SummaryDataRow.CustomAppsTotalApplicableDevices = $CustomAppsTotalApplicableDevices
    $SummaryDataRow.CustomAppsTotalInstalled = $CustomAppsTotalInstalled
    $SummaryDataRow.CustomAppsTotalFailed = $CustomAppsTotalFailed
    $SummaryDataRow.CustomAppsTotalInstallPending = $CustomAppsTotalInstallPending
    $SummaryDataRow.CustomAppsTotalNotInstalled = $CustomAppsTotalNotInstalled
    $SummaryDataRow.CustomAppsTotalPercentSuccess = $CustomAppsTotalPercentSuccess
    $SummaryDataRow.CustomAppsTotalCount = $CustomAppsTotalCount

    [void]$SummaryDataTable.Rows.Add($SummaryDataRow)

    # Export the data
    $SummaryDataTable | Export-Csv -Path "$Destination\PmpAppsDetailsSummaryData.csv" -NoTypeInformation -Force

    # Calculate status overview data
    $StatusOverviewTable = [System.Data.DataTable]::new()
    @(
        'ApplicationId',
        'FailedDeviceCount',
        'PendingInstallDeviceCount',
        'InstalledDeviceCount',
        'NotInstalledDeviceCount',
        'AppType',
        'ApplicableDeviceCount',
        'PercentSuccess'
    ) | 
        foreach {[void]$StatusOverviewTable.Columns.Add($_)}
    

    foreach ($appId in ($PMPApps.id | Select -Unique))
    {
        $StatusOverviewRow = $StatusOverviewTable.NewRow()
        $StatusOverviewRow.ApplicationId = $appId
        $StatusOverviewRow.AppType = ($PMPApps | Where-Object {$_.id -eq $appId}).appType
        $StatusResults = $DeviceInstallStatusResults.FindAll({$args[0].ApplicationId -eq $appId})
        $StatusOverviewRow.ApplicableDeviceCount = $StatusResults.Count
        $StatusOverviewRow.FailedDeviceCount = $StatusResults.FindAll({$args[0].AppInstallState_loc -eq "Failed"}).Count
        $StatusOverviewRow.PendingInstallDeviceCount = $StatusResults.FindAll({$args[0].AppInstallState_loc -eq "Install Pending"}).Count
        $StatusOverviewRow.InstalledDeviceCount = $StatusResults.FindAll({$args[0].AppInstallState_loc -eq "Installed"}).Count
        $StatusOverviewRow.NotInstalledDeviceCount = $StatusResults.FindAll({$args[0].AppInstallState_loc -eq "Not installed"}).Count
        try
        {
            $StatusOverviewRow.PercentSuccess = [math]::Round(($StatusOverviewRow.InstalledDeviceCount / $StatusOverviewRow.ApplicableDeviceCount) * 100, 2)
        }
        catch
        {
            $StatusOverviewRow.PercentSuccess = 0
        }
        [void]$StatusOverviewTable.Rows.Add($StatusOverviewRow)
    }

    # Export the data
    $StatusOverviewTable | Export-Csv -Path "$Destination\PmpAppsDetailsInstallStatusOverviewReport.csv" -NoTypeInformation -Force

}

# Function to filter out duplicates for exported data from Graph
Function script:Remove-MSGraphExportJobDuplicates {
    Param([System.Collections.Generic.List`1[Object]]$Collection)

    $GroupedCollection = $Collection | Group-Object -Property DeviceId
    foreach ($Item in ($GroupedCollection | Where {$_.Count -gt 1}))
    {
        $Others = $Item.Group | Sort LastModifiedDateTime -Descending | Select -Skip 1
        foreach ($entry in $Others)
        {
            [void]$Collection.Remove($entry)
        }
    }

    # No need to return anything as the collection is passed by reference
}
#endregion ----------------------------------------------------------------------------------------------


#region ----------------------------------------- Data exports ------------------------------------------
###############################################
## Export list of PMP applications in Intune ##
###############################################
Write-output "Exporting PMP apps list"
Export-PmpAppsList

#####################################
## Export the Overview report data ##
#####################################
if ($ReportType -in ("Overview","All"))
{
    Write-output "Exporting status overview report and summary data"
    Export-StatusReport -ReportOutputName "PmpAppsStatusOverviewReport" -ReportEntityName "getAppStatusOverviewReport" -ApplicationData $PmpApps
}

#####################################
## Export the Detailed report data ##
#####################################
if ($ReportType -in ("Detailed","All"))
{
    Write-output "Retrieving device install status reports and summary data"
    Get-DeviceInstallStatusReport -AppIDs $PmpApps.Id
}
#endregion ----------------------------------------------------------------------------------------------


#region ------------------------------------------ Data upload ------------------------------------------
Write-output "Uploading CSV files to Azure storage account"
$StorageAccount = Get-AzStorageAccount -Name $StorageAccount -ResourceGroupName $ResourceGroup
if ($ReportType -in ("All"))
{
    $FileList = @("PmpApps.csv","PmpDeviceInstallStatusReport.csv","PmpAppsStatusOverviewReport.csv","PmpAppsOverviewSummaryData.csv","PmpAppsDetailsSummaryData.csv","PmpAppsDetailsInstallStatusOverviewReport.csv")
}
elseif ($ReportType -in ("Detailed"))
{
    $FileList = @("PmpApps.csv","PmpDeviceInstallStatusReport.csv","PmpAppsDetailsSummaryData.csv","PmpAppsDetailsInstallStatusOverviewReport.csv")
}
else
{
    $FileList = @("PmpApps.csv","PmpAppsStatusOverviewReport.csv","PmpAppsOverviewSummaryData.csv")
}
$FileList | foreach {
    try {
        $File = $_
        Write-Verbose "Uploading $File to Azure storage container $Container"
        $null = Set-AzStorageBlobContent -File "$Destination\$File" -Container $Container -Blob $File -Context $StorageAccount.Context -Force -ErrorAction Stop
    }
    catch {
        Write-Error -Exception $_ -Message "Failed to upload $file to blob storage"
    } 
}
#endregion ----------------------------------------------------------------------------------------------
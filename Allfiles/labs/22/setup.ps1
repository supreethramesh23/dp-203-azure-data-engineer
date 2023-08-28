Clear-Host
write-host "Starting script at $(Get-Date)"

Set-PSRepository -Name PSGallery -InstallationPolicy Trusted
Install-Module -Name Az.Synapse -Force


# Prompt user for a password for the SQL Database
$sqlUser = "SQLUser"
write-host ""
$sqlPassword = "Password.1!!"

# Register resource providers
Write-Host "Registering resource providers...";
$provider_list = "Microsoft.Synapse", "Microsoft.Purview", "Microsoft.Sql", "Microsoft.Storage", "Microsoft.Compute"
foreach ($provider in $provider_list){
    $result = Register-AzResourceProvider -ProviderNamespace $provider
    $status = $result.RegistrationState
    Write-Host "$provider : $status"
}



# Generate unique random suffix
[string]$suffix =  -join ((48..57) + (97..122) | Get-Random -Count 7 | % {[char]$_})
Write-Host "Your randomly-generated suffix for Azure resources is $suffix"
$resourceGroupName = "dp203-$suffix"


# Choose a random region
Write-Host "Finding an available region. This may take several minutes...";
$delay = 0, 30, 60, 90, 120 | Get-Random
Start-Sleep -Seconds $delay # random delay to stagger requests from multi-student classes
# $preferred_list = "australiaeast","centralus","southcentralus","eastus2","northeurope","southeastasia","uksouth","westeurope","westus","westus2","eastus"
$preferred_list = "eastus"
$locations = Get-AzLocation | Where-Object {
    $_.Providers -contains "Microsoft.Synapse" -and
    $_.Providers -contains "Microsoft.Sql" -and
    $_.Providers -contains "Microsoft.Storage" -and
    $_.Providers -contains "Microsoft.Compute" -and
    $_.Providers -contains "Microsoft.Purview" -and
    $_.Location -in $preferred_list
}
$max_index = $locations.Count - 1
$rand = (0..$max_index) | Get-Random
# $Region = $locations.Get($rand).Location
$Region = "eastus"

# Test for subscription Azure SQL capacity constraints in randomly selected regions
# (for some subsription types, quotas are adjusted dynamically based on capacity)
 $success = 0
 $tried_list = New-Object Collections.Generic.List[string]

 while ($success -ne 1){
    write-host "Trying $Region"
    $capability = Get-AzSqlCapability -LocationName $Region
    if($capability.Status -eq "Available")
    {
        $success = 1
        write-host "Using $Region"
    }
    else
    {
        $success = 0
        $tried_list.Add($Region)
        $locations = $locations | Where-Object {$_.Location -notin $tried_list}
        if ($locations.Count -ne 1)
        {
            $rand = (0..$($locations.Count - 1)) | Get-Random
            $Region = $locations.Get($rand).Location
        }
        else {
            Write-Host "Couldn't find an available region for deployment."
            Write-Host "Sorry! Try again later."
            Exit
        }
    }
}
Write-Host "Creating $resourceGroupName resource group in $Region ..."
New-AzResourceGroup -Name $resourceGroupName -Location $Region | Out-Null

# Create Synapse workspace
$synapseWorkspace = "synapse$suffix"
$dataLakeAccountName = "datalake$suffix"
$sqlDatabaseName = "sql$suffix"
$purviewAccountName = "purview$suffix"

write-host "Creating Azure resources in $resourceGroupName resource group..."
write-host "(This may take some time!)"
New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName `
  -TemplateFile "setup.json" `
  -Mode Complete `
  -workspaceName $synapseWorkspace `
  -dataLakeAccountName $dataLakeAccountName `
  -uniqueSuffix $suffix `
  -sqlDatabaseName $sqlDatabaseName `
  -sqlUser $sqlUser `
  -sqlPassword $sqlPassword `
  -purviewAccountName $purviewAccountName `
  -Force

# Make the current user and the Synapse service principal owners of the data lake blob store
write-host "Granting permissions on the $dataLakeAccountName storage account..."
write-host "(you can ignore any warnings!)"
$subscriptionId = (Get-AzContext).Subscription.Id
$userName = ((az ad signed-in-user show) | ConvertFrom-JSON).UserPrincipalName
$id = (Get-AzADServicePrincipal -DisplayName $synapseWorkspace).id
New-AzRoleAssignment -Objectid $id -RoleDefinitionName "Storage Blob Data Owner" -Scope "/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Storage/storageAccounts/$dataLakeAccountName" -ErrorAction SilentlyContinue;
New-AzRoleAssignment -SignInName $userName -RoleDefinitionName "Storage Blob Data Owner" -Scope "/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Storage/storageAccounts/$dataLakeAccountName" -ErrorAction SilentlyContinue;

# Upload files
write-host "Loading data to data lake..."
$storageAccount = Get-AzStorageAccount -ResourceGroupName $resourceGroupName -Name $dataLakeAccountName
$storageContext = $storageAccount.Context
Get-ChildItem "./data/*.csv" -File | Foreach-Object {
    write-host ""
    $file = $_.Name
    Write-Host $file
    $blobPath = "products/$file"
    Set-AzStorageBlobContent -File $_.FullName -Container "files" -Blob $blobPath -Context $storageContext
}


sleep 15


# Create database
write-host "Creating databases..."
$serverlessSQL = Get-Content -Path "serverless.sql" -Raw
$serverlessSQL = Get-Content -Path "serverless.sql" -Raw
$serverlessSQL = $serverlessSQL.Replace("datalakexxxxxxx", $dataLakeAccountName)
$serverlessSQL = $serverlessSQL.Replace("datalakexxxxxxx", $dataLakeAccountName)
Set-Content -Path "serverless$suffix.sql" -Value $serverlessSQL
sqlcmd -S "$synapseWorkspace-ondemand.sql.azuresynapse.net" -U $sqlUser -P $sqlPassword -d master -I -i serverless$suffix.sql
sleep 3
sqlcmd -S "$synapseWorkspace.sql.azuresynapse.net" -U $sqlUser -P $sqlPassword -d $sqlDatabaseName -I -i dedicated.sql


sleep 5

# Pause SQL Pool
write-host "Pausing the $sqlDatabaseName SQL Pool..."
Suspend-AzSynapseSqlPool -WorkspaceName $synapseWorkspace -Name $sqlDatabaseName -AsJob


write-host "Resource Group Name =  $resourceGroupName"
write-host "Synapse Workspace Name =  $synapseWorkspace"
write-host "Purview AccountName =  $purviewAccountName"
write-host "Data Lake Account Name = $dataLakeAccountName"
write-host "SQL Server User = $sqlUser"
write-host "SQL Password = $sqlPassword"

write-host "Script completed at $(Get-Date)"

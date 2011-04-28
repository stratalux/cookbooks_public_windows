# Copyright (c) 2010 RightScale Inc
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# locals.
$cookbookName = Get-NewResource cookbook_name
$resourceName = Get-NewResource resource_name
$dbName = Get-NewResource name
$nodePath = $cookbookName,$resourceName,$dbName
$serverName = Get-NewResource server_name
$backupDirPath = Get-NewResource backup_dir_path
$existingBackupFileNamePattern = (Get-NewResource existing_backup_file_name_pattern) -f $dbName
$maxoldbackups = (Get-NewResource max_old_backups_to_keep)
$backupFileNameFormat = Get-NewResource backup_file_name_format
$zipBackup = Get-NewResource zip_backup
$deleteSqlAfterZip = Get-NewResource delete_sql_after_zip
$statementTimeoutSeconds = Get-NewResource statement_timeout_seconds

#check inputs.
$Error.Clear()
if (($maxoldbackups -eq $NULL) -or ($maxoldbackups -eq "") -or (!$maxoldbackups -match "^\d+$"))
{
    Write-Error "Error: 'max_old_backups_to_keep' is a required numeric attribute for the 'backup' provider. Aborting..."
    exit 140
}

# check if database exists before backing up.
if (!(Get-ChefNode ($nodePath + "exists")))
{
    Write-Warning "Not backing up ""$dbName"" because it does not exist."
    exit 141
}

# connect to server.
$server = New-Object ("Microsoft.SqlServer.Management.Smo.Server") $serverName

# default StatementTimeout to int32 max if undefined
if (($statementTimeoutSeconds -eq $NULL) -or ($statementTimeoutSeconds -eq ""))
{
    $statementTimeoutSeconds = [System.Int32]::MaxValue
}
$server.Connectioncontext.StatementTimeout = $statementTimeoutSeconds

# force creation of backup directory or ignore if already exists.
if (!(Test-Path $backupDirPath))
{
    md $backupDirPath | Out-Null
}
$backupDir     = Get-Item $backupDirPath -ea Stop
$backupDirPath = $backupDir.FullName
Write-Output "Using backup directory ""$backupDirPath"""

$backupDir     = Get-Item $backupDirPath -ea Stop

# rename existing .bak to .old after deleting existing .old files.
foreach ($backupFile in $backupDir.GetFiles($existingBackupFileNamePattern)) { ren $backupFile.FullName ($backupFile.Name + ".old") }

# TODO: account for the new _full and _log files.  We'll need to double the count for these, and check to see if the _log
# even exists in case we're running on an instance that kept some old stuff around.

$oldcount=$backupDir.GetFiles($existingBackupFileNamePattern+".old").count
# TODO: cleanup old backup files by some algorithm (allow 3 per database, older than 1 week, etc.)
if ($oldcount -gt $maxoldbackups)
{
    $deletecount=$oldcount-$maxoldbackups
    write-output "***Deleting [$deletecount] old backup(s):"
    foreach ($oldBackupFile in $backupDir.GetFiles($existingBackupFileNamePattern+".old") | Select-Object -first $deletecount)
    {
        write-output "   ***Deleting old backup: $oldBackupFile"
        del $oldBackupFile.FullName
    }
}

if ($zipBackup -eq "true")
{
    #get count and substract one(latest zip backup)
    $oldcount=$backupDir.GetFiles($existingBackupFileNamePattern+".zip").count-1
    # TODO: cleanup old zipped backup files by some algorithm (allow 3 per database, older than 1 week, etc.)
    if ($oldcount -gt $maxoldbackups)
    {
        $deletecount=$oldcount-$maxoldbackups
        write-output "Deleting [$deletecount] old zipped backups"
        foreach ($oldBackupFile in $backupDir.GetFiles($existingBackupFileNamePattern+".zip") | Select-Object -first $deletecount)
        {
            write-output "Deleting $oldBackupFile"
            del $oldBackupFile.FullName
        }
    }
}


# iterate user databases (ignoring system databases) and backup any found.
$db = $server.Databases | where { !$_.IsSystemObject_ -and ($_.Name -eq $dbName) }
if ($db)
{
    $dbName         = $db.Name
    $timestamp      = Get-Date -format yyyyMMddHHmmss

    # Full backup
    $fullBackupFileName = $backupFileNameFormat -f $dbName, $timestamp, "full"
    $fullBackupFilePath = Join-Path $backupDirPath $fullBackupFileName

    $fullBackup                      = New-Object ("Microsoft.SqlServer.Management.Smo.Backup")
    $fullBackup.Action               = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Database  # full database backup.
    $fullBackup.BackupSetDescription = "Full backup of $dbName"
    $fullBackup.BackupSetName        = "$dbName backup"
    $fullBackup.Database             = $dbName
    $fullBackup.MediaDescription     = "Disk"
    $fullBackup.LogTruncation        = "Truncate"
    $fullBackup.Devices.AddDevice($fullBackupFilePath, "File")

    $Error.Clear()

    try
    {
        $fullBackup.SqlBackup($server)
    }
    catch [System.Exception]
    {
        Resolve-Error
        Write-Error "Failed to backup ""$dbName"""
        exit 105
    }
    # /Full backup

    # Transaction Log Backup
    $logBackupFileName = $backupFileNameFormat -f $dbName, $timestamp, "log"
    $logBackupFilePath = Join-Path $backupDirPath $logBackupFileName

    $logBackup                      = New-Object ("Microsoft.SqlServer.Management.Smo.Backup")
    $logBackup.Action               = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Log  # transaction log backup
    $logBackup.BackupSetDescription = "Transaction log backup of $dbName"
    $logBackup.BackupSetName        = "$dbName backup"
    $logBackup.Database             = $dbName
    $logBackup.MediaDescription     = "Disk"
    $logBackup.LogTruncation        = "Truncate"
    $logBackup.Devices.AddDevice($logBackupFilePath, "File")

    $Error.Clear()

    try
    {
        $logBackup.SqlBackup($server)
    }
    catch [System.Exception]
    {
        Resolve-Error
        Write-Error "Failed to backup transaction logs for ""$dbName"""
        exit 106
    }
    # /Transaction Log Backup

    if ($Error.Count -eq 0)
    {
        Write-Output "Backed up database named ""$dbName"" to [""$fullBackupFilePath"",""$logBackupFilePath""]"
        if ($zipBackup -eq "true")
        {
            Write-Output "Zipping the backup"
            $output=invoke-expression 'cmd /c 7z a -tzip "$backupFilePath.zip" $fullBackupFilePath $logBackupFilePath'
            Write-Output $output
            if ($output -match "Everything is Ok")
            {
                if ($deleteSqlAfterZip -eq "true")
                {
                    Write-Output "Deleting the bak file"
                    Remove-Item $fullBackupFilePath
                    Remove-Item $logBackupFilePath
                }
                Set-ChefNode backupfilename $backupFileName".zip"
            }
        }
        else
        {
            # TODO: This is technically two files now, could set this node attribute to an array safely since it's not
            # used anywhere.  In fact the only place from which this line of code would be executed is the db_sqlserver::backup recipe
            Set-ChefNode backupfilename $backupFileName
        }
    }
    else
    {
        # report error but keep trying to backup additional databases.
        Write-Error "Failed to backup ""$dbName"""
        Write-Warning "SQL Server fails to backup/restore to/from network drives but will accept the equivalent UNC path so long as the database user has sufficient network privileges. Ensure that the SQL_BACKUP_DIR_PATH environment variable does not refer to a shared drive."
    }
}

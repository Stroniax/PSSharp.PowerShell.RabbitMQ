@{
    ModuleVersion      = '1.0.0'
    RootModule         = 'PSSharp.PowerShell.RabbitMQ.dll'
    RequiredAssemblies = @(
        'RabbitMQ.Client.dll'
        'System.Threading.RateLimiting.dll'
        'PSSharp.PowerShell.RabbitMQ.dll'
    )
    FormatsToProcess   = @(
        'PSSharp.PowerShell.RabbitMQ.formats.ps1xml'
    )
    TypesToProcess     = @(
        'PSSharp.PowerShell.RabbitMQ.types.ps1xml'
    )
}
@{
    ModuleVersion      = '1.0.0'
    RootModule         = 'PSSharp.PowerShell.RabbitMQ.Commands.dll'
    RequiredAssemblies = @(
        'RabbitMQ.Client.dll'
        'System.Threading.RateLimiting.dll'
        'PSSharp.PowerShell.RabbitMQ.Commands.dll'
    )
    FormatsToProcess   = @(
        'PSSharp.PowerShell.RabbitMQ.formats.ps1xml'
    )
    TypesToProcess     = @(
        'PSSharp.PowerShell.RabbitMQ.types.ps1xml'
    )
}
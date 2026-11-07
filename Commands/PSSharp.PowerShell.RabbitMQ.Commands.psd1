@{
    ModuleVersion      = '1.0.0'
    Guid               = '90472e9e-e9c1-4bd1-b421-84dec387b9fa'
    Author             = @(
        'Caleb Frederickson'
    )
    PowerShellVersion  = '7.2'
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
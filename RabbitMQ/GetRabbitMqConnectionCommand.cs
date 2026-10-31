using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsCommon.Get, Noun, DefaultParameterSetName = NameParameterSet)]
public sealed class GetRabbitMqConnectionCommand : PSCmdlet
{
    private const string Noun = "RabbitMqConnection";

    private const string InstanceParameterSet = "Instance";
    private const string NameParameterSet = "Name";

    [Parameter(Mandatory = true, Position = 0, ParameterSetName = NameParameterSet)]
    [SupportsWildcards]
    public string[] Name { get; set; } = [];


    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true, ParameterSetName = InstanceParameterSet)]
    public Guid[] InstanceId { get; set; } = [];

    protected override void ProcessRecord()
    {
        var store = new RabbitConnectionStore(SessionState);

        if (Name.Length == 0 && InstanceId.Length == 0)
        {
            WriteObject(store.GetConnections(), true);
            return;
        }

        var ids = new HashSet<Guid>(InstanceId);
        var names = new HashSet<string>(Name.Where(n => !WildcardPattern.ContainsWildcardCharacters(n)), StringComparer.OrdinalIgnoreCase);
        var wildcards = new HashSet<WildcardPattern>(Name.Where(n => WildcardPattern.ContainsWildcardCharacters(n)).Select(n => WildcardPattern.Get(n, WildcardOptions.IgnoreCase)));
        var foundNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var cx in store.GetConnections())
        {
            if (ids.Remove(cx.InstanceId))
            {
                WriteObject(cx);
            }
            else if (names.Contains(cx.Name) || wildcards.Any(wc => wc.IsMatch(cx.Name)))
            {
                foundNames.Add(cx.Name);
                WriteObject(cx);
            }
        }

        foreach (var missing in ids)
        {
            var exn = new ItemNotFoundException("RabbitMQ connection with the specified InstanceId was not found.");
            var err = new ErrorRecord(exn, "InstanceIdNotFound", ErrorCategory.ObjectNotFound, missing)
            {
                ErrorDetails = new($"RabbitMQ connection with the specified InstanceId \"{missing}\" was not found.")
            };
            WriteError(err);
        }

        names.ExceptWith(foundNames);
        foreach (var missing in names)
        {
            var exn = new ItemNotFoundException("RabbitMQ connection with the specified name was not found.");
            var err = new ErrorRecord(exn, "NameNotFound", ErrorCategory.ObjectNotFound, missing)
            {
                ErrorDetails = new($"RabbitMQ connection with the specified Name \"{missing}\" was not found.")
            };
            WriteError(err);

        }
    }
}

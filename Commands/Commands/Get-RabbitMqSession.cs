using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommon.Get, Nouns.RabbitMqSession, DefaultParameterSetName = NameParameterSet)]
[OutputType(typeof(PSRabbitSession))]
public sealed class GetRabbitMqSessionCommand : PSCmdlet
{
    private const string InstanceParameterSet = "Instance";
    private const string NameParameterSet = "Name";

    [Parameter(Position = 0, ParameterSetName = NameParameterSet)]
    [PSRabbitSessionCompleter]
    [ValidateNotNullOrWhiteSpace]
    [SupportsWildcards]
    public string[] Name { get; set; } = [];

    [Parameter(
        Mandatory = true,
        Position = 0,
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = InstanceParameterSet
    )]
    [PSRabbitSessionCompleter]
    public Guid[] InstanceId { get; set; } = [];

    protected override void ProcessRecord()
    {
        var store = PSRabbitSessionStore.Current;

        if (Name.Length == 0 && InstanceId.Length == 0)
        {
            WriteObject(store.GetSessions(), true);
            return;
        }

        var ids = new HashSet<Guid>(InstanceId);
        var names = new HashSet<string>(
            Name.Where(n => !WildcardPattern.ContainsWildcardCharacters(n)),
            StringComparer.OrdinalIgnoreCase
        );
        var wildcards = new HashSet<WildcardPattern>(
            Name.Where(n => WildcardPattern.ContainsWildcardCharacters(n))
                .Select(n => WildcardPattern.Get(n, WildcardOptions.IgnoreCase))
        );
        var foundNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var cx in store.GetSessions())
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
            var er = Assert.ItemNotFound("RabbitMQ session", InstanceId);
            WriteError(er);
        }

        names.ExceptWith(foundNames);
        foreach (var missing in names)
        {
            var er = Assert.ItemNotFound("RabbitMQ session", missing, nameof(PSRabbitSession.Name));
            WriteError(er);
        }
    }
}

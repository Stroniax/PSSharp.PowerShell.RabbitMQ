using System.Collections.Concurrent;
using System.Management.Automation;
using System.Runtime.CompilerServices;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public abstract class RmqCmdlet : PSCmdlet, IDisposable
{
    /// <summary>
    /// A token cancelled when the cmdlet is stopped.
    /// </summary>
    private readonly CancellationTokenSource _stopped = new();
    private BlockingCollection<Action> _queue = [];
    private bool _isDisposed;

    /// <summary>
    /// A cancellation token that is cancelled when the cmdlet is stopped.
    /// The token is not triggered if the cmdlet stops due to exception nor completion.
    /// </summary>
    protected CancellationToken StoppingToken => _stopped.Token;

    protected async Task WriteObjectAsync(object? value)
    {
        await InvokeAsync(() => WriteObject(value));
    }

    protected async Task WriteErrorAsync(ErrorRecord error)
    {
        await InvokeAsync(() => WriteError(error));
    }

    /// <summary>
    /// Marshals a callback to the pipeline thread.
    /// </summary>
    protected Task<T> InvokeAsync<T>(Func<T> fn)
    {
        var tcs = new TaskCompletionSource<T>();
        _queue.Add(() =>
        {
            try
            {
                var result = fn();
                tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        });
        return tcs.Task;
    }

    /// <summary>
    /// Marshals a callback to the pipeline thread.
    /// </summary>
    protected Task InvokeAsync(Action fn)
    {
        var tcs = new TaskCompletionSource();
        _queue.Add(() =>
        {
            try
            {
                fn();
                tcs.TrySetResult();
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        });
        return tcs.Task;
    }

    private void BlockAndRun()
    {
        foreach (var item in _queue.GetConsumingEnumerable(_stopped.Token))
        {
            item();
        }
    }

    private void OnTaskFinished(Task result)
    {
        if (result.IsFaulted && !result.IsCanceled)
        {
            _queue.Add(() =>
            {
                var exn = result.Exception.GetBaseException();
                WriteError(new ErrorRecord(exn, "AsyncCmdletException", ErrorCategory.NotSpecified, null));
                throw exn;
            });
        }
        _queue.CompleteAdding();
    }

    protected virtual Task BeginProcessingAsync()
    {
        return Task.CompletedTask;
    }

    protected override sealed void BeginProcessing()
    {
        base.BeginProcessing();
        _queue ??= [];
        BeginProcessingAsync().ContinueWith(OnTaskFinished);
        BlockAndRun();
    }

    protected virtual Task ProcessRecordAsync()
    {
        return Task.CompletedTask;
    }
    protected override sealed void ProcessRecord()
    {
        base.ProcessRecord();
        _queue = [];
        ProcessRecordAsync().ContinueWith(OnTaskFinished);
        BlockAndRun();
    }

    protected virtual Task EndProcessingAsync()
    {
        return Task.CompletedTask;
    }

    protected sealed override void EndProcessing()
    {
        base.EndProcessing();
        _queue = [];
        EndProcessingAsync().ContinueWith(OnTaskFinished);
        BlockAndRun();
    }

    protected override void StopProcessing()
    {
        base.StopProcessing();
        _stopped.Cancel();
    }

    protected void Dispose(bool isDisposing)
    {
        if (isDisposing)
        {
            _stopped.Dispose();
        }
    }

    public void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }
        _isDisposed = true;
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~RmqCmdlet()
    {
        Dispose(false);
    }
}

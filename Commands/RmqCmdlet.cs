using System.Collections.Concurrent;
using System.Management.Automation;
using System.Runtime.CompilerServices;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public abstract class RmqCmdlet : PSCmdlet
{
    private BlockingCollection<Action> _queue = [];

    protected async Task WriteObjectAsync(object? value)
    {
        await InvokeAsync(() => WriteObject(value)).ConfigureAwait(false);
    }

    protected async Task WriteErrorAsync(ErrorRecord error)
    {
        await InvokeAsync(() => WriteError(error)).ConfigureAwait(false);
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
        foreach (var item in _queue.GetConsumingEnumerable(PipelineStopToken))
        {
            item();
        }
    }

    private void OnTaskFinished(Task result)
    {
        if (result.IsFaulted && !result.IsCanceled)
        {
            var exn = result.Exception.GetBaseException();
            var er = new ErrorRecord(exn, "AsyncCmdletException", ErrorCategory.NotSpecified, null);
            _queue.Add(() =>
            {
                ThrowTerminatingError(er);
            });
        }
        _queue.CompleteAdding();
    }

    protected virtual Task BeginProcessingAsync()
    {
        return Task.CompletedTask;
    }

    protected sealed override void BeginProcessing()
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

    protected sealed override void ProcessRecord()
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
}

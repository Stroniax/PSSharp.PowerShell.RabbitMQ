namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <summary>
/// Waits for a particular window of time to elapse without an event occurring. The notification of the
/// event is triggered by the caller invoking <see cref="Reset"/>. When the duration has passed without
/// the event occurring, tasks returned from <see cref="WaitAsync(CancellationToken)"/> will complete
/// and <see cref="IsFinished"/> will be set to <see langword="true"/>.
/// </summary>
internal sealed class WindowWatcher : IDisposable
{
    private readonly TaskCompletionSource _tcs;
    private readonly ITimer _timer;
    private readonly TimeSpan _window;

    /// <summary>
    /// Whether the window has at some time elapsed without event. Once the watcher has finished,
    /// it exhibits no further behavior.
    /// </summary>
    public bool IsFinished => _tcs.Task.IsCompletedSuccessfully;

    /// <summary>
    /// Default constructor.
    /// </summary>
    /// <param name="window">The amount of time that must pass between events for the watcher to complete.</param>
    /// <param name="time">Used to measure time.</param>
    public WindowWatcher(TimeSpan window, TimeProvider time)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(
            window.TotalMilliseconds,
            uint.MaxValue
        );

        _tcs = new();
        _window = window;

        // Start "disabled", user must call Reset once to start the timer
        _timer = time.CreateTimer(
            OnWindowElapse,
            null,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan
        );
    }

    /// <summary>
    /// Default constructor using <see cref="TimeProvider.System"/>.
    /// </summary>
    /// <param name="window">The amount of time that must pass between events for the watcher to complete.</param>
    public WindowWatcher(TimeSpan window)
        : this(window, TimeProvider.System) { }

    /// <summary>
    /// Notify the watcher that the event has occurred. Resets the internal timer such that the
    /// window must elapse from the time this method has been called most recently.
    /// This function must also be called once to start the timer.
    /// <para>
    /// Calling <see cref="Reset"/> after the watcher has finished is a no-op.
    /// </para>
    /// </summary>
    public void Reset()
    {
        _timer.Change(_window, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Returns a task that waits for the window to elapse.
    /// </summary>
    /// <param name="cancellationToken">Used to terminate the current waiting task.</param>
    /// <returns>A task that completes when the window has elapsed with no event occurrence.</returns>
    public async Task WaitAsync(CancellationToken cancellationToken)
    {
        await _tcs.Task.WaitAsync(cancellationToken);
    }

    private void OnWindowElapse(object? state)
    {
        _tcs.TrySetResult();
    }

    public void Dispose()
    {
        _timer.Dispose();

        if (!_tcs.Task.IsCompleted)
        {
            // Guarded just to avoid allocation; TaskCompletionSource is inhertently a race-prone
            // resource and the guard doesn't resolve that in any way.
            _tcs.TrySetException(new ObjectDisposedException(nameof(WindowWatcher)));
        }
    }
}

using EmqxLearning.Shared.Services.Abstracts;

namespace EmqxLearning.Shared.Services;

public class DynamicRateLimiter : IDynamicRateLimiter
{
    private readonly ManualResetEventSlim _resetEvent;
    private readonly SemaphoreSlim _semaphore;

    private int _limit = 0;
    private int _queueCount = 0;
    private int _acquired = 0;

    public (int Limit, int Acquired, int Available, int QueueCount) State
    {
        get
        {
            try
            {
                _semaphore.Wait();
                return (_limit, _acquired, _limit - _acquired, _queueCount);
            }
            finally { _semaphore.Release(); }
        }
    }

    public DynamicRateLimiter()
    {
        _semaphore = new SemaphoreSlim(1);
        _resetEvent = new ManualResetEventSlim();
    }

    public async Task Acquire(CancellationToken cancellationToken = default)
    {
        bool queued = false;
        bool canAcquired = false;
        while (!canAcquired)
        {
            await _semaphore.WaitAsync(cancellationToken: cancellationToken);
            if (!queued)
            {
                Interlocked.Increment(ref _queueCount);
                queued = true;
            }
            try
            {
                if (_acquired < _limit)
                {
                    canAcquired = true;
                    _acquired++;
                    Interlocked.Decrement(ref _queueCount);
                }
                else _resetEvent.Reset();
            }
            finally
            {
                _semaphore.Release();
                if (!canAcquired)
                    _resetEvent.Wait(cancellationToken);
            }
        }
    }

    public async Task Release(CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken: cancellationToken);
        try
        {
            if (_acquired > 0)
            {
                _acquired--;
                _resetEvent.Set();
            }
        }
        finally { _semaphore.Release(); }
    }

    public async Task SetLimit(int limit, CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken: cancellationToken);
        try
        { _limit = limit; }
        finally { _semaphore.Release(); }
    }
}
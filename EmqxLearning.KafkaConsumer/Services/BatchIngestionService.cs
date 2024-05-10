using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using EmqxLearning.KafkaConsumer.Extensions;
using EmqxLearning.KafkaConsumer.Services.Abstracts;
using EmqxLearning.Shared.Exceptions;
using EmqxLearning.Shared.Models;
using EmqxLearning.Shared.Services.Abstracts;
using Npgsql;
using Polly;
using Polly.Registry;

namespace EmqxLearning.KafkaConsumer.Services;

public class BatchIngestionService : IIngestionService, IDisposable
{
    private const int DefaultLockSeconds = 3;
    private readonly IKafkaManager _kafkaManager;
    private NpgsqlDataSource _dataSource;
    private readonly IConfiguration _configuration;
    private readonly ILogger<BatchIngestionService> _logger;
    private readonly ConcurrentQueue<WrappedIngestionMessage> _messages;
    private readonly ResiliencePipeline _connectionErrorsPipeline;
    private readonly ResiliencePipeline _transientErrorsPipeline;
    private readonly List<System.Timers.Timer> _timers;
    private CancellationToken _stoppingToken;
    private Func<Task> _reconnectConsumer;

    private static readonly SemaphoreSlim _circuitLock = new SemaphoreSlim(initialCount: 1);
    private bool _isCircuitOpen;

    public BatchIngestionService(
        IKafkaManager kafkaManager,
        ILogger<BatchIngestionService> logger,
        IConfiguration configuration,
        ResiliencePipelineProvider<string> resiliencePipelineProvider)
    {
        _kafkaManager = kafkaManager;
        _logger = logger;
        _configuration = configuration;
        _timers = new List<System.Timers.Timer>();
        _messages = new ConcurrentQueue<WrappedIngestionMessage>();
        _connectionErrorsPipeline = resiliencePipelineProvider.GetPipeline(Constants.ResiliencePipelines.ConnectionErrors);
        _transientErrorsPipeline = resiliencePipelineProvider.GetPipeline(Constants.ResiliencePipelines.TransientErrors);

        SetupDataSource();
        SetupWorkerThreads();
    }

    public void Configure(Func<Task> reconnectConsumer)
    {
        _reconnectConsumer = reconnectConsumer;
    }


    public Task HandleMessage(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        try
        {
            var ingestionMessage = JsonSerializer.Deserialize<ReadIngestionMessage>(consumeResult.Message.Value);
            _stoppingToken = cancellationToken;
            _logger.LogInformation("Metrics count {Count}", ingestionMessage.RawData.Count);
            _messages.Enqueue(new(ingestionMessage));
        } 
        catch(Exception ex)
        {
            _logger.LogError("Unable to process messages: {msg} - err: {ex}", JsonSerializer.Serialize(consumeResult.Message.Value), ex);
        }
        
        return Task.CompletedTask;
    }

    private void SetupWorkerThreads()
    {
        var workerThreadCount = _configuration.GetValue<int>("BatchSettings:WorkerThreadCount");
        var batchInterval = _configuration.GetValue<int>("BatchSettings:BatchInterval");
        var batchSize = _configuration.GetValue<int>("BatchSettings:BatchSize");
        var comparer = new IngestionMessageComparer();
        var batch = new List<WrappedIngestionMessage>();
        for (int i = 0; i < workerThreadCount; i++)
        {
            var aTimer = new System.Timers.Timer(batchInterval);
            aTimer.Elapsed += async (s, e) =>
            {
                var batch = new List<WrappedIngestionMessage>();
                try
                {
                    if (_messages.Count == 0) return;
                    while (batch.Count < batchSize && _messages.TryDequeue(out var message))
                        batch.Add(message);
                    batch.Sort(comparer);
                    await HandleBatch(batch);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            };
            aTimer.AutoReset = true;
            aTimer.Enabled = true;
            _timers.Add(aTimer);
        }
       
    }

    private async Task HandleBatch(List<WrappedIngestionMessage> batch)
    {
        //step 1. insert DB
        try
        {
            await _transientErrorsPipeline.ExecuteAsync(async (token) => await InsertToDb(batch, token));
        }
        catch
        {
            await _connectionErrorsPipeline.ExecuteAsync(async (token) =>
            {
                await OpenCircuit();
                var _ = Task.Factory.StartNew(async () =>
                {
                    var reconnectAfter = _configuration.GetValue<int>("ResilienceSettings:CircuitBreakerReconnectAfter");
                    await Task.Delay(reconnectAfter);
                    await CloseCircuit();
                });
            });
            throw;
        }

        //step 2. ACK all at outside already
    }

    private void SetupDataSource() => _dataSource = NpgsqlDataSource.Create(_configuration.GetConnectionString("DeviceDb"));

    private async Task EnsureDataSourceActive()
    {
        await _connectionErrorsPipeline.ExecuteAsync(async (token) =>
        {
            try { await using NpgsqlConnection connection = await _dataSource.OpenConnectionAsync(token); }
            catch (Exception ex)
            {
                _logger.LogWarning("Reconnecting DB failed, reason: {Message}", ex.Message);
                throw;
            }
        }, _stoppingToken);
    }

    private void StartTimers()
    {
        foreach (var timer in _timers)
            timer.Start();
    }

    private void StopTimers()
    {
        foreach (var timer in _timers)
            timer.Stop();
    }

    private async Task OpenCircuit()
    {
        var acquired = await _circuitLock.WaitAsync(TimeSpan.FromSeconds(DefaultLockSeconds));
        if (acquired)
        {
            try
            {
                if (_isCircuitOpen == true) return;
                _logger.LogWarning("Opening circuit breaker ...");
                StopTimers();
                await _kafkaManager.CloseAsync();
                await _dataSource.DisposeAsync();
                _messages.Clear();
                _isCircuitOpen = true;
                _logger.LogWarning("Circuit breaker is now open");
            }
            finally { _circuitLock.Release(); }
        }
    }

    private async Task CloseCircuit()
    {
        var acquired = await _circuitLock.WaitAsync(TimeSpan.FromSeconds(DefaultLockSeconds));
        if (acquired)
        {
            try
            {
                if (_isCircuitOpen == false) return;
                _logger.LogWarning("Try closing circuit breaker ...");
                SetupDataSource();
                await EnsureDataSourceActive();
                _connectionErrorsPipeline.Execute(() =>
                {
                    
                });
                if (_reconnectConsumer != null) await _reconnectConsumer();
                _isCircuitOpen = false;
                StartTimers();
                _logger.LogWarning("Circuit breaker is now closed");
            }
            finally
            {
                _circuitLock.Release();
            }
        }
    }

    const string SeriesTable = "device_metric_series";
    const string SeriesColumns = "_ts, device_id, metric_key, value, retention_days";
    private async Task InsertToDb(IEnumerable<WrappedIngestionMessage> messages, CancellationToken cancellationToken)
    {
        if (_configuration.GetValue<bool>("InsertDb") == false)
        {
            await Task.Delay(_configuration.GetValue<int>("ProcessingTime"));
            return;
        }
        await using NpgsqlConnection connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        using NpgsqlBinaryImporter writer = connection.BeginBinaryImport($"COPY {SeriesTable} ({SeriesColumns}) FROM STDIN (FORMAT BINARY)");

        foreach (var message in messages)
        {
            var data = message.Payload.RawData;
            var deviceId = data["deviceId"].ToString();
            data.Remove("timestamp");
            data.Remove("deviceId");
            var values = new List<object>();
            foreach (var kvp in message.Payload.RawData)
            {
                writer.StartRow();
                writer.Write(DateTime.Now, NpgsqlTypes.NpgsqlDbType.Timestamp);
                writer.Write(deviceId);
                writer.Write(kvp.Key);
                writer.Write(Random.Shared.NextDouble(), NpgsqlTypes.NpgsqlDbType.Numeric);
                writer.WriteNullable(90);
            }
        }

        await writer.CompleteAsync(cancellationToken);
    }

    public void Dispose()
    {
        _dataSource?.Dispose();
    }

}

class IngestionMessageComparer : IComparer<WrappedIngestionMessage>
{
    public int Compare(WrappedIngestionMessage x, WrappedIngestionMessage y)
    {
        var xTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(x.Payload.RawData["timestamp"].ToString()));
        var yTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(y.Payload.RawData["timestamp"].ToString()));
        if (xTimestamp > yTimestamp) return 1;
        if (xTimestamp < yTimestamp) return -1;
        return 0;
    }
}

struct WrappedIngestionMessage
{
    public ReadIngestionMessage Payload { get; }
    public WrappedIngestionMessage(ReadIngestionMessage payload)
    {
        Payload = payload;
    }
}
using DeviceId;
using EmqxLearning.Shared.Exceptions;
using EmqxLearning.Shared.Models;
using EmqxLearning.Shared.Services.Abstracts;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Formatter;
using MQTTnet.Internal;
using Polly;
using Polly.Registry;
using System.Collections.Concurrent;
using System.Text.Json;

namespace EmqxLearning.MqttListener;

public class Worker : BackgroundService
{
    private const int DefaultLockSeconds = 3;

    private Queue<int> _queueCounts = new Queue<int>();
    private Queue<int> _availableCounts = new Queue<int>();
    private System.Timers.Timer _concurrencyCollector;
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IKafkaManager _kafkManager;
    private readonly ConcurrentBag<MqttClientWrapper> _mqttClients;
    private readonly ResiliencePipeline _connectionErrorsPipeline;
    private readonly ResiliencePipeline _transientErrorsPipeline;
    private CancellationToken _stoppingToken;
    private CancellationTokenSource _circuitTokenSource;
    private static readonly SemaphoreSlim _circuitLock = new SemaphoreSlim(initialCount: 1);
        
    private readonly MqttFactory _factory;
    private bool _isCircuitOpen;
    private string _testTopic;
    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        ResiliencePipelineProvider<string> resiliencePipelineProvider,
        IKafkaManager kafkManager)
    {
        _logger = logger;
        _configuration = configuration;
        _kafkManager = kafkManager;
        _circuitTokenSource = new CancellationTokenSource();
        _mqttClients = new ConcurrentBag<MqttClientWrapper>();
        _connectionErrorsPipeline = resiliencePipelineProvider.GetPipeline(Constants.ResiliencePipelines.ConnectionErrors);
        _transientErrorsPipeline = resiliencePipelineProvider.GetPipeline(Constants.ResiliencePipelines.TransientErrors);
        _factory = new MqttFactory();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        //testing with default 5 worker per processors.
        int workerAmount = Environment.ProcessorCount > 1 ? (Environment.ProcessorCount - 1) * 5 : 1 * 5;
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount
        };

        await Parallel.ForEachAsync(Enumerable.Range(1, workerAmount), options, async ( index, ct) =>
        {
            await InitializeMqttClient(index);
        });

        while (!stoppingToken.IsCancellationRequested)
            await Task.Delay(1000, stoppingToken);
    }

    private void SetupCancellationTokens()
    {
        _stoppingToken.Register(() => _circuitTokenSource.Cancel());
        _circuitTokenSource.Token.Register(() =>
        {
            foreach (var wrapper in _mqttClients)
                wrapper.TokenSource.TryCancel();
        });
    }




    private async Task RestartMqttClients()
    {
        foreach (var wrapper in _mqttClients)
        {
            var mqttClient = wrapper.Client;
            await _connectionErrorsPipeline.ExecuteAsync(async (token) =>
            {
                try { await mqttClient.StartAsync(mqttClient.Options); }
                catch (Exception ex)
                {
                    _logger.LogWarning("Reconnecting MQTT client {ClientId} failed, reason: {Message}",mqttClient.Options.ClientOptions.ClientId,ex.Message);
                    throw;
                }
            });
        }
    }

    private async Task StopMqttClients()
    {
        foreach (var wrapper in _mqttClients)
        {
            wrapper.TokenSource.TryCancel();
            await wrapper.Client.StopAsync();
        }
    }

    private async Task InitializeMqttClient(int threadIdx)
    {
        var backgroundProcessing = _configuration.GetValue<bool>("BackgroundProcessing");
        var mqttClient = _factory.CreateManagedMqttClient();
        var mqttClientConfiguration = _configuration.GetSection("MqttClientOptions");
        var optionsBuilder = new MqttClientOptionsBuilder()
            .WithTcpServer(mqttClientConfiguration["TcpServer"])
            .WithCleanSession(value: mqttClientConfiguration.GetValue<bool>("CleanSession"))
            .WithSessionExpiryInterval(mqttClientConfiguration.GetValue<uint>("SessionExpiryInterval"))
            .WithProtocolVersion(MqttProtocolVersion.V500);
        string deviceId = new DeviceIdBuilder()
            .AddMachineName()
            .AddOsVersion()
            .ToString();

        var clientId = _configuration["MqttClientOptions:ClientId"] != null
            ? $"{_configuration["MqttClientOptions:ClientId"]}_{threadIdx}"
            : $"mqtt-listener_{deviceId}_{threadIdx}";
        optionsBuilder = optionsBuilder.WithClientId(clientId);

        var options = optionsBuilder.Build();
        var managedOptions = new ManagedMqttClientOptionsBuilder()
            .WithAutoReconnectDelay(TimeSpan.FromSeconds(_configuration.GetValue<int>("MqttClientOptions:ReconnectDelaySecs")))
            .WithClientOptions(options)
            .Build();
        var wrapper = new MqttClientWrapper(mqttClient);
        _mqttClients.Add(wrapper);

        mqttClient.ConnectedAsync += (e) => OnConnected(e, mqttClient);
        mqttClient.DisconnectedAsync += (e) => OnDisconnected(e, wrapper);
        mqttClient.ApplicationMessageReceivedAsync += ((e) => OnMessageReceivedBackground(e, wrapper));

        await _connectionErrorsPipeline.ExecuteAsync(async (token) =>
            await mqttClient.StartAsync(managedOptions),
            cancellationToken: _stoppingToken);
    }

    private async Task OnConnected(MqttClientConnectedEventArgs e, IManagedMqttClient mqttClient)
    {
        _logger.LogInformation("### CONNECTED WITH SERVER - ClientId: {0} ###", mqttClient.Options.ClientOptions.ClientId);
        var topic = _configuration["MqttClientOptions:Topic"];
        var qos = _configuration.GetValue<MQTTnet.Protocol.MqttQualityOfServiceLevel>("MqttClientOptions:QoS");

        await _connectionErrorsPipeline.ExecuteAsync(async (token) =>
        {
            try
            {
                await mqttClient.SubscribeAsync(topic: topic, qualityOfServiceLevel: qos);
                _logger.LogInformation("### SUBSCRIBED topic {0} - qos {1} ###", topic, qos);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Subscribing MQTT topic failed, reason: {Message}", ex.Message);
                throw;
            }
        }, cancellationToken: _stoppingToken);
    }

    private async Task OnMessageReceivedNormal(MqttApplicationMessageReceivedEventArgs e)
    {
        await Task.Delay(_configuration.GetValue<int>("ProcessingTime"));
        _logger.LogInformation("Received message at {Time}", DateTime.Now);
    }

    private async Task OnMessageReceivedBackground(MqttApplicationMessageReceivedEventArgs e, MqttClientWrapper wrapper)
    {
        try
        {
            _ = await Task.Factory.StartNew(async () =>
            {
                try { await HandleMessage(e, wrapper); }
                catch (DownstreamDisconnectedException ex)
                {
                    _logger.LogError(ex, ex.Message);
                    var _ = Task.Factory.StartNew(HandleOpenCircuit);
                }
                catch (Exception ex) { _logger.LogError(ex, ex.Message); }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, ex.Message);
        }
    }

    private async Task HandleOpenCircuit()
    {
        await OpenCircuit();
        var reconnectAfter = _configuration.GetValue<int>("ResilienceSettings:CircuitBreakerReconnectAfter");
        System.Timers.Timer closeTimer = new System.Timers.Timer(reconnectAfter);
        closeTimer.Elapsed += async (s, e) => await CloseCircuit();
        closeTimer.AutoReset = false;
        closeTimer.Start();
    }

    private async Task HandleMessage(MqttApplicationMessageReceivedEventArgs e, MqttClientWrapper wrapper)
    {
        var payload = JsonSerializer.Deserialize<Dictionary<string, object>>(e.ApplicationMessage.PayloadSegment);
        var ingestionMessage = new IngestionMessage(payload);
        await SendIngestionMessage(ingestionMessage);

        await _transientErrorsPipeline.ExecuteAsync(
            async (token) =>
            {
                if (_isCircuitOpen) throw new CircuitOpenException();
                await e.AcknowledgeAsync(token);
            },
            cancellationToken: wrapper.TokenSource.Token);
    }

    private Task OnDisconnected(MqttClientDisconnectedEventArgs e, MqttClientWrapper wrapper)
    {
        wrapper.TokenSource.TryCancel();
        _logger.LogError(e.Exception, "### DISCONNECTED FROM SERVER ### {Event}",
            e.Exception == null ? JsonSerializer.Serialize(e) : string.Empty);
        return Task.CompletedTask;
    }


    /// <summary>
    /// Broad message to kafka topic
    /// </summary>
    /// <param name="ingestionMessage"></param>
    /// <returns></returns>
    /// <exception cref="DownstreamDisconnectedException"></exception>
    private async Task SendIngestionMessage(IngestionMessage ingestionMessage)
    {
        try
        {
            var producer = await _kafkManager.StartProducerAsync(_configuration["Kafka:Topic"]);
            var bytes = JsonSerializer.SerializeToUtf8Bytes(ingestionMessage);
            await _transientErrorsPipeline.Execute(async () =>
            {
                await _kafkManager.ProduceMessageAsync(_configuration["Kafka:Topic"], JsonSerializer.Serialize(ingestionMessage));
            });
        }
        catch (Exception ex)
        {
            throw new DownstreamDisconnectedException(ex.Message, ex);
        }
    }

    private void StopConcurrencyCollector()
    {
        _concurrencyCollector.Stop();
        _queueCounts.Clear();
        _availableCounts.Clear();
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
                StopConcurrencyCollector();
                _circuitTokenSource.Cancel();
                await StopMqttClients();
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
               
            }
            finally
            {
                _circuitLock.Release();
            }
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        foreach (var wrapper in _mqttClients)
            wrapper.Client.Dispose();
        _mqttClients.Clear();
        // [TODO] dispose others
    }
}

internal class MqttClientWrapper
{
    private CancellationTokenSource _tokenSource;
    private readonly IManagedMqttClient _client;

    public MqttClientWrapper(IManagedMqttClient client)
    {
        _client = client;
        ResetTokenSource();
    }

    public IManagedMqttClient Client => _client;
    public CancellationTokenSource TokenSource => _tokenSource;

    public void ResetTokenSource()
    {
        _tokenSource?.Dispose();
        _tokenSource = new CancellationTokenSource();
        _tokenSource.Token.Register(() => ResetTokenSource());
    }
}
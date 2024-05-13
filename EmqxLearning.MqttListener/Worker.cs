using Confluent.Kafka;
using DeviceId;
using EmqxLearning.Shared.Exceptions;
using EmqxLearning.Shared.Models;
using EmqxLearning.Shared.Services.Abstracts;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Formatter;
using MQTTnet.Internal;
using System.Collections.Concurrent;
using System.Text.Json;

namespace EmqxLearning.MqttListener;

public class Worker : BackgroundService, IHostedService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IKafkaManager _kafkManager;
    private readonly ConcurrentBag<MqttClientWrapper> _mqttClients;
    private CancellationToken _stoppingToken;
    private CancellationTokenSource _circuitTokenSource;
    private readonly ConcurrentQueue<string> _messages;
    private readonly MqttFactory _factory;
    private readonly int _batchSize;
    private readonly Mutex _mutex;
    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        IKafkaManager kafkManager)
    {
        _logger = logger;
        _configuration = configuration;
        _kafkManager = kafkManager;
        _circuitTokenSource = new CancellationTokenSource();
        _mqttClients = new ConcurrentBag<MqttClientWrapper>();
        _factory = new MqttFactory();
        _messages = new ConcurrentQueue<string>();
        _mutex = new Mutex();
        _batchSize = _configuration.GetValue<int>("BatchSettings:BatchSize");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        _logger.LogInformation("Start ExecuteAsync MqttListener worker");
        var workersPerProcessor = _configuration.GetValue<int>("WorkerPerProcessor");
        //testing with default 5 worker per processors.
        int workerAmount = Environment.ProcessorCount > 1 ? (Environment.ProcessorCount - 1) * workersPerProcessor : 1 * workersPerProcessor;
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount
        };

        await Parallel.ForEachAsync(Enumerable.Range(1, workerAmount), options, async (index, ct) =>
        {
            _logger.LogInformation("Initialize MqttClient {index}", index);
            await InitializeMqttClient(index);
        });
        _logger.LogInformation("completed initiate MqttClients");
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
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
        mqttClient.ConnectingFailedAsync += (e) => OnFailToConnect(e);
        mqttClient.ApplicationMessageReceivedAsync += ((e) => OnMessageReceivedBackground(e, wrapper));

        await mqttClient.StartAsync(managedOptions);
    }

    private async Task OnConnected(MqttClientConnectedEventArgs e, IManagedMqttClient mqttClient)
    {
        _logger.LogInformation("### CONNECTED WITH SERVER - ClientId: {0} ###", mqttClient.Options.ClientOptions.ClientId);
        var topic = _configuration["MqttClientOptions:Topic"];
        var qos = _configuration.GetValue<MQTTnet.Protocol.MqttQualityOfServiceLevel>("MqttClientOptions:QoS");
        await mqttClient.SubscribeAsync(topic: topic, qualityOfServiceLevel: qos);
    }

    private async Task OnMessageReceivedNormal(MqttApplicationMessageReceivedEventArgs e)
    {
        await Task.Delay(_configuration.GetValue<int>("ProcessingTime"));
        _logger.LogInformation("Received message at {Time}", DateTime.Now);
    }

    private async Task OnFailToConnect(ConnectingFailedEventArgs e)
    {
        _logger.LogError("OnFailToConnect {e}", e);
    }

    private async Task OnMessageReceivedBackground(MqttApplicationMessageReceivedEventArgs e, MqttClientWrapper wrapper)
    {
        try
        {
            await HandleMessage(e, wrapper);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, ex.Message);
        }
    }


    private async Task HandleMessage(MqttApplicationMessageReceivedEventArgs e, MqttClientWrapper wrapper)
    {
        var payload = JsonSerializer.Deserialize<Dictionary<string, object>>(e.ApplicationMessage.PayloadSegment);
        var ingestionMessage = new IngestionMessage(payload);
        await SendIngestionMessage(ingestionMessage);
        await e.AcknowledgeAsync(default);
    }

    private Task OnDisconnected(MqttClientDisconnectedEventArgs e, MqttClientWrapper wrapper)
    {
        wrapper.TokenSource.TryCancel();
        _logger.LogError(e.Exception, "### DISCONNECTED FROM SERVER ### {Event}", e.Exception == null ? JsonSerializer.Serialize(e) : string.Empty);
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
            var msg = JsonSerializer.Serialize(ingestionMessage);
            if (_messages.Count < _batchSize)
            {
                _messages.Enqueue(msg);
                return;
            }
            //dequeue all
            var messages = _messages.ToArray();
            _messages.Clear();
            var topic = _configuration.GetValue<string>("Kafka:Topic");
            await _kafkManager.ProduceMessageAsync(topic, messages);

        }
        catch (Exception ex)
        {
            throw new DownstreamDisconnectedException(ex.Message, ex);
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
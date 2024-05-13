using Confluent.Kafka;
using EmqxLearning.KafkaConsumer.Services.Abstracts;
using EmqxLearning.Shared.Services.Abstracts;
using Polly;
using Polly.Registry;
using System.Threading;
using static Confluent.Kafka.ConfigPropertyNames;

namespace EmqxLearning.KafkaConsumer;

public class Worker : BackgroundService, IHostedService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IKafkaManager _kafkaManager;
    private readonly IIngestionService _ingestionService;
    private CancellationToken _stoppingToken;
    private readonly ResiliencePipeline _connectionErrorsPipeline;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        IKafkaManager kafkaManager,
        IIngestionService ingestionService,
        ResiliencePipelineProvider<string> resiliencePipelineProvider)
    {
        _ingestionService = ingestionService;
        _kafkaManager = kafkaManager;
        _logger = logger;
        _configuration = configuration;

        _connectionErrorsPipeline = resiliencePipelineProvider.GetPipeline(Constants.ResiliencePipelines.ConnectionErrors);
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Start Worker ExecuteAsync");
        await SetupConsumers(cancellationToken);
        _logger.LogInformation("Completed Worker ExecuteAsync");

    }

    private async Task SetupConsumers(CancellationToken cancellationToken)
    {
        (var consumer, var err) = await _kafkaManager.StartConsumerAsync(_configuration.GetValue<string>("Kafka:Topic"), cancellationToken);
        if (!string.IsNullOrEmpty(err))
        {
            _logger.LogError(err);
            return;
        }

        try
        {

            consumer.Subscribe(_configuration.GetValue<string>("Kafka:Topic"));

            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogInformation($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                        continue;
                    }

                    _logger.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                    await _ingestionService.HandleMessage(consumeResult, _stoppingToken);
                    try
                    {
                        // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                        // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                        // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                        // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                        consumer.StoreOffset(consumeResult);
                    }
                    catch (KafkaException e)
                    {
                        _logger.LogInformation($"Store Offset error: {e.Error.Reason}");
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogInformation($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer.");
            consumer.Close();
            await _kafkaManager.ReleaseConsumersAsync(new string[] { _configuration.GetValue<string>("Kafka:Topic")});
        }
    }
}

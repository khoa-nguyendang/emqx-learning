using Confluent.Kafka;
using EmqxLearning.Shared.Services.Abstracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using static Confluent.Kafka.ConfigPropertyNames;

namespace EmqxLearning.Shared.Services
{
    public class KafkaManager : IKafkaManager
    {
        private readonly ILogger<KafkaManager> _logger;
        private IConfiguration _configuration;
        private ConcurrentDictionary<string, IProducer<string, string>> _producers = new ConcurrentDictionary<string, IProducer<string, string>>();
        private ConcurrentDictionary<string, IConsumer<string, string>> _consumers = new ConcurrentDictionary<string, IConsumer<string, string>>();

        public KafkaManager(IConfiguration configuration, ILogger<KafkaManager> logger) {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task ACK(string topicName, long offset, CancellationToken token = default)
        {
            _consumers[topicName].Commit();
        }

        public async Task CloseAsync()
        {
            foreach (var i in _producers.Keys)
            {
                _producers[i].AbortTransaction();
                _producers[i].Dispose();
            }
            foreach (var i in _consumers.Keys)
            {
                _consumers[i].Close();
                _consumers[i].Dispose();
            }
        }

        public async Task<(bool success, string err)> ProduceMessageAsync(string topicName, string[] messages, CancellationToken token = default)
        {
            try
            {
                if (!_producers.ContainsKey(topicName))
                    _ = await StartProducerAsync(topicName);

                //Message structure of Kafka <string, string> use to delivery to a partition, 
                //in this sample I don't defined it, so it would be random by set = null, or id of messages
                //following Kafka document, Keys can be used to determine the partition, but it's just a default strategy of the producer.
                //Ultimately, it is the producer who chooses which partition to use
                Task[] tasks = messages.Select((v, i) => _producers[topicName].ProduceAsync(topicName, new Message<string, string>() { Key = i.ToString(), Value = v }, token)).ToArray();
                await Task.WhenAll(tasks);
                _producers[topicName].Flush(token);
                _logger.LogInformation("Published {messageCount} messages to topic: {topicName}", messages.Length, topicName);
                return (true, string.Empty);
            }
            catch (Exception ex)
            {
                _logger.LogError("ProduceMessageAsync failure: {ex}", ex);
                return (false, ex.Message);
            }
        }

        public async Task<(bool success, string err)> ProduceMessageAsync(string topicName, string message, CancellationToken token = default)
        {
            try
            {
                _logger.LogInformation("ProduceMessageAsync received message from topic: {topicName} - msg: {message}", topicName, message);
                if (!_producers.ContainsKey(topicName))
                    _ = await StartProducerAsync(topicName);
                await _producers[topicName].ProduceAsync(topicName, new Message<string, string>() { Key = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(), Value = message }, token);
                _producers[topicName].Flush();
                _logger.LogInformation("Published message to topic: {topicName}", topicName);
                return (true, string.Empty);
            }
            catch (Exception ex)
            {
                _logger.LogError("ProduceMessageAsync failure: {ex}", ex);
                return (false, ex.Message);
            }
        }

        public async Task ReleaseConsumersAsync(string[] names)
        {
            //TBD
        }

        public async Task ReleaseProducersAsync(string[] names)
        {
            //TBD
        }

        public async Task<(IConsumer<string, string>, string err)> StartConsumerAsync(string topicName, CancellationToken token = default)
        {
            try
            {
                return _consumers.ContainsKey(topicName)
                    ? (_consumers[topicName], string.Empty)
                    : (await InitConsumerAsync(topicName, token), string.Empty);
            }
            catch (Exception ex) { return (null, ex.ToString()); }
        }

        public async Task<(IProducer<string, string>, string err)> StartProducerAsync(string topicName, CancellationToken token = default)
        {
            try
            {
                return _producers.ContainsKey(topicName)
                    ? (_producers[topicName], string.Empty)
                    : (await InitProducer(topicName, token), string.Empty);
            }
            catch (Exception ex) { return (null, ex.ToString()); }
        }

        private async Task<IConsumer<string, string>> InitConsumerAsync(string topicName, CancellationToken cancellationToken = default)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                GroupId = "csharp-consumer",
                EnableAutoOffsetStore = false,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            var consumer = new ConsumerBuilder<string, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => _logger.LogInformation($"Error: {e.Reason}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    _logger.LogInformation(
                         "Partitions incrementally assigned: [" +
                         string.Join(',', partitions.Select(p => p.Partition.Value)) +
                         "], all: [" +
                         string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                         "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    _logger.LogInformation(
                         "Partitions incrementally revoked: [" +
                         string.Join(',', partitions.Select(p => p.Partition.Value)) +
                         "], remaining: [" +
                         string.Join(',', remaining.Select(p => p.Partition.Value)) +
                         "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    _logger.LogInformation($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build();

            _consumers.TryAdd(topicName, consumer);
            return consumer;

        }

        private async Task<IProducer<string, string>> InitProducer(string topicName, CancellationToken cancellationToken = default)
        {
            var config = new ProducerConfig {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                SaslUsername = _configuration["Kafka:Username"],
                SaslPassword = _configuration["Kafka:Password"],
            };
            var producer = new ProducerBuilder<string, string>(config).Build();
            _producers.TryAdd(topicName, producer);
            _logger.LogInformation("InitProducer for topic: {topicName}", topicName);
            return producer;
        }


    }
}
